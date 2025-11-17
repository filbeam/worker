import {
  isValidEthereumAddress,
  httpAssert,
  getBadBitsEntry,
  handleResponse,
} from '@filbeam/retrieval'

import { parseRequest } from '../lib/request.js'
import { retrieveFile as defaultRetrieveFile } from '../lib/retrieval.js'
import { getRetrievalCandidatesAndValidatePayer } from '../lib/store.js'

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
      return await this._fetch(request, env, ctx, { retrieveFile })
    } catch (error) {
      return this._handleError(error)
    }
  },

  /**
   * @param {Request} request
   * @param {Env} env
   * @param {ExecutionContext} ctx
   * @param {object} options
   * @param {typeof defaultRetrieveFile} [options.retrieveFile]
   * @returns
   */
  async _fetch(request, env, ctx, { retrieveFile = defaultRetrieveFile } = {}) {
    httpAssert(
      ['GET', 'HEAD'].includes(request.method),
      405,
      'Method Not Allowed',
    )
    if (URL.parse(request.url)?.pathname === '/') {
      return Response.redirect('https://filbeam.com/', 302)
    }
    if (URL.parse(request.url)?.hostname.endsWith('filcdn.io')) {
      return Response.redirect(
        request.url.replace('filcdn.io', 'filbeam.io'),
        301,
      )
    }

    const requestTimestamp = new Date().toISOString()
    const workerStartedAt = performance.now()
    const requestCountryCode = request.headers.get('CF-IPCountry')

    const { payerWalletAddress, pieceCid, botName } = parseRequest(request, env)

    httpAssert(payerWalletAddress && pieceCid, 400, 'Missing required fields')
    httpAssert(
      isValidEthereumAddress(payerWalletAddress),
      400,
      `Invalid address: ${payerWalletAddress}. Address must be a valid ethereum address.`,
    )

    // Timestamp to measure file retrieval performance (from cache and from SP)
    const fetchStartedAt = performance.now()

    const [retrievalCandidates, isBadBit] = await Promise.all([
      getRetrievalCandidatesAndValidatePayer(
        env,
        payerWalletAddress,
        pieceCid,
        env.ENFORCE_EGRESS_QUOTA,
      ),
      env.BAD_BITS_KV.get(`bad-bits:${await getBadBitsEntry(pieceCid)}`, {
        type: 'json',
      }),
    ])

    httpAssert(
      !isBadBit,
      404,
      'The requested CID was flagged by the Bad Bits Denylist at https://badbits.dwebops.pub',
    )

    httpAssert(
      retrievalCandidates.length > 0,
      500,
      'Service provider lookup failed',
    )

    let retrievalCandidate
    let retrievalResult
    const retrievalAttempts = []

    while (retrievalCandidates.length > 0) {
      const retrievalCandidateIndex = Math.floor(
        Math.random() * retrievalCandidates.length,
      )
      retrievalCandidate = retrievalCandidates[retrievalCandidateIndex]
      retrievalAttempts.push(retrievalCandidate)
      retrievalCandidates.splice(retrievalCandidateIndex, 1)
      console.log('Attempting retrieval', retrievalCandidate)
      try {
        retrievalResult = await retrieveFile(
          ctx,
          retrievalCandidate.serviceUrl,
          pieceCid,
          request,
          env.ORIGIN_CACHE_TTL,
          { signal: request.signal },
        )
        if (retrievalResult.response.ok) {
          break
        }
        console.log(
          `Retrieval attempt failed: HTTP ${retrievalResult.response.status}`,
          {
            retrievalCandidate,
            willRetry: retrievalCandidates.length > 0,
          },
        )
      } catch (err) {
        const msg =
          typeof err === 'object' && err !== null && 'message' in err
            ? err.message
            : String(err)
        console.log(`Retrieval attempt failed: ${msg}`, {
          retrievalCandidate,
          willRetry: retrievalCandidates.length > 0,
        })
      }
    }

    httpAssert(retrievalCandidate, 500, 'should never happen')

    return handleResponse(
      env,
      ctx,
      retrievalResult?.response?.ok
        ? retrievalResult.response
        : new Response(
            `No available service provider found. Attempted: ${retrievalAttempts.map((a) => `ID=${a.serviceProviderId} (Service URL=${a.serviceUrl})`).join(', ')}`,
            {
              status: 502,
              headers: new Headers({
                'X-Data-Set-ID': retrievalAttempts
                  .map((a) => a.dataSetId)
                  .join(','),
              }),
            },
          ),
      retrievalResult?.cacheMiss || null,
      requestCountryCode,
      requestTimestamp,
      retrievalCandidate.dataSetId,
      botName,
      fetchStartedAt,
      workerStartedAt,
      env.CLIENT_CACHE_TTL,
      env.ENFORCE_EGRESS_QUOTA,
    )
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
