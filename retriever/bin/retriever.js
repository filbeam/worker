import { isValidEthereumAddress } from '../lib/address.js'
import { OWNER_TO_RETRIEVAL_URL_MAPPING } from '../lib/constants.js'
import { parseRequest } from '../lib/request.js'
import {
  retrieveFile as defaultRetrieveFile,
  measureStreamedEgress,
} from '../lib/retrieval.js'
import { getOwnerAndValidateClient, logRetrievalResult } from '../lib/store.js'
import { httpAssert } from '../lib/http-assert.js'

export default {
  /**
   * @param {Request} request
   * @param {Env} env
   * @param {ExecutionContext} ctx
   * @param {object} options
   * @param {typeof defaultRetrieveFile} [options.retrieveFile]
   * @returns
   */
  async fetch(request, env, ctx, { retrieveFile = defaultRetrieveFile } = {}) {
    try {
      return await this._fetch(request, env, ctx, retrieveFile)
    } catch (error) {
      return this._handleError(error)
    }
  },

  /**
   * @param {Request} request
   * @param {Env} env
   * @param {ExecutionContext} ctx
   * @param {typeof defaultRetrieveFile} retrieveFile
   * @returns
   */
  async _fetch(request, env, ctx, retrieveFile) {
    const requestTimestamp = new Date().toISOString()
    const workerStartedAt = performance.now()
    const requestCountryCode = request.headers.get('CF-IPCountry')
    httpAssert(request.method === 'GET', 405, 'Method Not Allowed')

    const { clientWalletAddress, rootCid } = parseRequest(request, env)

    httpAssert(clientWalletAddress && rootCid, 400, 'Missing required fields')
    httpAssert(
      isValidEthereumAddress(clientWalletAddress),
      400,
      `Invalid address: ${clientWalletAddress}. Address must be a valid ethereum address.`,
    )

    const retrievalResultEntry = {
      clientAddress: clientWalletAddress,
      ownerAddress: /** @type {string | null} */ (null),
      cacheMiss: /** @type {boolean | null} */ (null),
      responseStatus: /** @type {number | null} */ (null),
      egressBytes: /** @type {number | null} */ (null),
      requestCountryCode,
      timestamp: requestTimestamp,
      performanceStats: {
        fetchTtfb: /** @type {number | null} */ (null),
        fetchTtlb: /** @type {number | null} */ (null),
        workerTtfb: /** @type {number | null} */ (null),
      },
    }

    try {
      // Timestamp to measure file retrieval performance (from cache and from SP)
      const fetchStartedAt = performance.now()

      const ownerAddress = await getOwnerAndValidateClient(
        env,
        clientWalletAddress,
        rootCid,
      )

      httpAssert(
        ownerAddress &&
          Object.prototype.hasOwnProperty.call(
            OWNER_TO_RETRIEVAL_URL_MAPPING,
            ownerAddress,
          ),
        404,
        `Unsupported Storage Provider (PDP ProofSet Owner): ${ownerAddress}`,
      )

      retrievalResultEntry.ownerAddress = ownerAddress

      const spURL = OWNER_TO_RETRIEVAL_URL_MAPPING[ownerAddress].url
      const { response, cacheMiss } = await retrieveFile(
        spURL,
        rootCid,
        env.CACHE_TTL,
      )

      retrievalResultEntry.cacheMiss = cacheMiss
      retrievalResultEntry.responseStatus = response.status

      if (!response.body) {
        retrievalResultEntry.egressBytes = 0

        ctx.waitUntil(
          logRetrievalResult(env, {
            ...retrievalResultEntry,
            responseStatus: response.status,
          }),
        )
        return response
      }

      const [returnedStream, egressMeasurementStream] = response.body.tee()
      const reader = egressMeasurementStream.getReader()
      const firstByteAt = performance.now()

      ctx.waitUntil(
        (async () => {
          const egressBytes = await measureStreamedEgress(reader)
          const lastByteFetchedAt = performance.now()

          retrievalResultEntry.egressBytes = egressBytes
          retrievalResultEntry.performanceStats = {
            fetchTtfb: firstByteAt - fetchStartedAt,
            fetchTtlb: lastByteFetchedAt - fetchStartedAt,
            workerTtfb: firstByteAt - workerStartedAt,
          }

          await logRetrievalResult(env, {
            ...retrievalResultEntry,
            responseStatus: response.status,
          })
        })(),
      )

      return new Response(returnedStream, {
        status: response.status,
        statusText: response.statusText,
        headers: response.headers,
      })
    } catch (error) {
      const { status } = getErrorHttpStatusMessage(error)

      retrievalResultEntry.responseStatus = status
      retrievalResultEntry.performanceStats.workerTtfb =
        performance.now() - workerStartedAt

      ctx.waitUntil(
        logRetrievalResult(env, {
          ...retrievalResultEntry,
          responseStatus: status,
        }),
      )

      throw error
    }
  },

  /**
   * @param {unknown} error
   * @returns
   */
  _handleError(error) {
    const { status, message } = getErrorHttpStatusMessage(error)

    if (status >= 500) {
      console.error(error)
    }
    return new Response(message, { status })
  },
}

/**
 * Extracts status and message from an error object.
 *
 * - If the error has a numeric `status`, it is used; otherwise, defaults to 500.
 * - If the status is < 500 and a string `message` exists, it's used; otherwise, a
 *   generic message is returned.
 *
 * @param {unknown} error - The error object to extract from.
 * @returns {{ status: number; message: string }}
 */
function getErrorHttpStatusMessage(error) {
  const isObject = typeof error === 'object' && error !== null
  const status =
    isObject && 'status' in error && typeof error.status === 'number'
      ? error.status
      : 500

  const message =
    isObject &&
    status < 500 &&
    'message' in error &&
    typeof error.message === 'string'
      ? error.message
      : 'Internal Server Error'

  return { status, message }
}
