import { isValidEthereumAddress } from '../lib/address.js'
import { parseRequest } from '../lib/request.js'
import {
  retrieveFile as defaultRetrieveFile,
  measureStreamedEgress,
  createQuotaEnforcingStream,
} from '../lib/retrieval.js'
import {
  getStorageProviderAndValidatePayer,
  logRetrievalResult,
  updateDataSetStats,
} from '../lib/store.js'
import { httpAssert } from '../lib/http-assert.js'
import { setContentSecurityPolicy } from '../lib/content-security-policy.js'
import { findInBadBits } from '../lib/bad-bits-util.js'

// We need to keep an explicit definition of RetrieverEnv because our monorepo has multiple
// worker-configuration.d.ts files, each file (re)defining the global Env interface, causing the
// final Env interface to contain only properties available to all workers.
/**
 * @typedef {{
 *   ENVIRONMENT: 'dev' | 'calibration ' | 'mainnet'
 *   ORIGIN_CACHE_TTL: 86400
 *   CLIENT_CACHE_TTL: 31536000
 *   DNS_ROOT: '.localhost' | '.calibration.filbeam.io' | '.filbeam.io'
 *   DB: D1Database
 * }} PieceRetrieverEnv
 */
export default {
  /**
   * @param {Request} request
   * @param {PieceRetrieverEnv} env
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
   * @param {PieceRetrieverEnv} env
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

    const { payerWalletAddress, pieceCid } = parseRequest(request, env)

    httpAssert(payerWalletAddress && pieceCid, 400, 'Missing required fields')
    httpAssert(
      isValidEthereumAddress(payerWalletAddress),
      400,
      `Invalid address: ${payerWalletAddress}. Address must be a valid ethereum address.`,
    )

    try {
      // Timestamp to measure file retrieval performance (from cache and from SP)
      const fetchStartedAt = performance.now()

      const [
        {
          serviceProviderId,
          serviceUrl,
          dataSetId,
          cdnEgressQuota,
          cacheMissEgressQuota,
        },
        isBadBit,
      ] = await Promise.all([
        getStorageProviderAndValidatePayer(env, payerWalletAddress, pieceCid),
        findInBadBits(env, pieceCid),
      ])

      httpAssert(
        !isBadBit,
        404,
        'The requested CID was flagged by the Bad Bits Denylist at https://badbits.dwebops.pub',
      )

      httpAssert(
        serviceProviderId,
        404,
        `Unsupported Service Provider: ${serviceProviderId}`,
      )

      const { response: originResponse, cacheMiss } = await retrieveFile(
        serviceUrl,
        pieceCid,
        env.ORIGIN_CACHE_TTL,
        { signal: request.signal },
      )

      if (!originResponse.body) {
        // The upstream response does not have any readable body
        // There is no need to measure response body size, we can
        // return the original response object.
        ctx.waitUntil(
          logRetrievalResult(env, {
            cacheMiss,
            responseStatus: originResponse.status,
            egressBytes: 0,
            requestCountryCode,
            timestamp: requestTimestamp,
            dataSetId,
          }),
        )
        const response = new Response(originResponse.body, originResponse)
        setContentSecurityPolicy(response)
        response.headers.set('X-Data-Set-ID', dataSetId)
        response.headers.set(
          'Cache-Control',
          `public, max-age=${env.CLIENT_CACHE_TTL}`,
        )
        return response
      }

      // Determine which quota to use based on cache hit/miss
      const availableQuota = cacheMiss ? cacheMissEgressQuota : cdnEgressQuota

      const firstByteAt = performance.now()

      // Apply quota enforcement and measure egress
      const quotaEnforcer = createQuotaEnforcingStream(availableQuota)
      const enforcedStream = originResponse.body.pipeThrough(
        quotaEnforcer.stream,
      )

      // Split stream: one for response, one for measurement
      const [responseStream, measurementStream] = enforcedStream.tee()

      ctx.waitUntil(
        (async () => {
          let egressBytes = 0

          try {
            // Measure bytes from the measurement stream
            const reader = measurementStream.getReader()
            egressBytes = await measureStreamedEgress(reader)
          } catch (error) {
            // Measurement might fail if stream was terminated early
            // Get the actual bytes transferred from the quota enforcer
            const status = quotaEnforcer.getStatus()
            egressBytes = status.egressBytes
          }

          // Check if quota was exceeded
          const { quotaExceeded } = quotaEnforcer.getStatus()

          const lastByteFetchedAt = performance.now()

          await logRetrievalResult(env, {
            cacheMiss,
            responseStatus: quotaExceeded ? 402 : originResponse.status,
            egressBytes,
            requestCountryCode,
            timestamp: requestTimestamp,
            performanceStats: {
              fetchTtfb: firstByteAt - fetchStartedAt,
              fetchTtlb: lastByteFetchedAt - fetchStartedAt,
              workerTtfb: firstByteAt - workerStartedAt,
            },
            dataSetId,
          })

          // Update stats and decrement quota
          await updateDataSetStats(env, { dataSetId, egressBytes, cacheMiss })
        })(),
      )

      // Return the response stream immediately
      const response = new Response(responseStream, {
        status: originResponse.status,
        statusText: originResponse.statusText,
        headers: originResponse.headers,
      })
      setContentSecurityPolicy(response)
      response.headers.set('X-Data-Set-ID', dataSetId)
      response.headers.set(
        'Cache-Control',
        `public, max-age=${env.CLIENT_CACHE_TTL}`,
      )
      return response
    } catch (error) {
      const { status } = getErrorHttpStatusMessage(error)

      ctx.waitUntil(
        logRetrievalResult(env, {
          cacheMiss: null,
          responseStatus: status,
          egressBytes: null,
          requestCountryCode,
          timestamp: requestTimestamp,
          dataSetId: null,
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
