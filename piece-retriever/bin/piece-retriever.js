import {
  isValidEthereumAddress,
  httpAssert,
  setContentSecurityPolicy,
  getBadBitsEntry,
  updateDataSetStats,
  logRetrievalResult,
} from '@filbeam/retrieval'

import { parseRequest } from '../lib/request.js'
import {
  retrieveFile as defaultRetrieveFile,
  getRetrievalUrl,
} from '../lib/retrieval.js'
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

    const { payerWalletAddress, pieceCid, botName, validateCacheMissResponse } =
      parseRequest(request, env)

    httpAssert(payerWalletAddress && pieceCid, 400, 'Missing required fields')
    httpAssert(
      isValidEthereumAddress(payerWalletAddress),
      400,
      `Invalid address: ${payerWalletAddress}. Address must be a valid ethereum address.`,
    )

    try {
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
        console.log(`Attempting retrieval via ${retrievalCandidate.serviceUrl}`)
        try {
          retrievalResult = await retrieveFile(
            ctx,
            retrievalCandidate.serviceUrl,
            pieceCid,
            request,
            env.ORIGIN_CACHE_TTL,
            {
              signal: request.signal,
              addCacheMissResponseValidation: validateCacheMissResponse,
            },
          )
          if (retrievalResult.response.ok) {
            console.log(
              `Retrieval attempt succeeded (cache ${retrievalResult.cacheMiss ? 'miss' : 'hit'})`,
            )
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

      if (!retrievalResult || retrievalResult.response.status >= 500) {
        ctx.waitUntil(
          logRetrievalResult(env, {
            cacheMiss: retrievalResult?.cacheMiss || null,
            cacheMissResponseValid: null,
            responseStatus: 502,
            egressBytes: 0,
            requestCountryCode,
            timestamp: requestTimestamp,
            dataSetId: retrievalCandidate.dataSetId,
            botName,
          }),
        )
        const response = new Response(
          `No available service provider found. Attempted: ${retrievalAttempts.map((a) => `ID=${a.serviceProviderId} (Service URL=${a.serviceUrl})`).join(', ')}`,
          {
            status: 502,
            headers: new Headers({
              'X-Data-Set-ID': retrievalAttempts
                .map((a) => a.dataSetId)
                .join(','),
            }),
          },
        )
        setContentSecurityPolicy(response)
        return response
      }

      if (!retrievalResult.response.body) {
        // The upstream response does not have any readable body
        // There is no need to measure response body size, we can
        // return the original response object.
        ctx.waitUntil(
          logRetrievalResult(env, {
            cacheMiss: retrievalResult.cacheMiss,
            cacheMissResponseValid: false,
            responseStatus: retrievalResult.response.status,
            egressBytes: 0,
            requestCountryCode,
            timestamp: requestTimestamp,
            dataSetId: retrievalCandidate.dataSetId,
            botName,
          }),
        )
        const response = new Response(
          retrievalResult.response.body,
          retrievalResult.response,
        )
        setContentSecurityPolicy(response)
        response.headers.set('X-Data-Set-ID', retrievalCandidate.dataSetId)
        response.headers.set(
          'Cache-Control',
          `public, max-age=${env.CLIENT_CACHE_TTL}`,
        )
        try {
          const contentLengthHeader =
            retrievalResult.response.headers.get('content-length')
          const estimatedEgress = contentLengthHeader
            ? Number.parseInt(contentLengthHeader, 10) || 0
            : 0
          const remainingCdn =
            retrievalCandidate.cdnEgressQuota - BigInt(estimatedEgress)
          const remainingCacheMiss =
            retrievalCandidate.cacheMissEgressQuota - BigInt(estimatedEgress)
          response.headers.set(
            'X-Cdn-Egress-Remaining',
            String(remainingCdn < 0n ? 0n : remainingCdn),
          )
          response.headers.set(
            'X-Cache-Miss-Egress-Remaining',
            String(remainingCacheMiss < 0n ? 0n : remainingCacheMiss),
          )
        } catch (e) {
          console.warn('Failed to compute egress remaining headers', e)
        }
        return response
      }

      // Stream, count bytes and validate (a cache miss)
      let egressBytes = 0
      /** @type {number | null} */
      let firstByteAt = null

      const { promise: responseFinishedPromise, resolve: responseFinished } =
        Promise.withResolvers()

      const measureStream = new TransformStream({
        transform(chunk, controller) {
          if (firstByteAt === null) {
            console.log('First byte received')
            firstByteAt = performance.now()
          }
          egressBytes += chunk.length
          controller.enqueue(chunk)
        },
      })

      const responseFinishedStream = new TransformStream({
        flush() {
          console.log('Response finished')
          responseFinished(null)
        },
      })
      const returnedStream = retrievalResult.response.body
        .pipeThrough(measureStream)
        .pipeThrough(responseFinishedStream)

      ctx.waitUntil(
        (async () => {
          await responseFinishedPromise
          const cacheMissResponseValid =
            typeof retrievalResult.validate === 'function'
              ? retrievalResult.validate()
              : null
          httpAssert(firstByteAt, 500, 'Should never happen')
          const lastByteFetchedAt = performance.now()

          if (cacheMissResponseValid === false) {
            await caches.default.delete(
              getRetrievalUrl(retrievalCandidate.serviceUrl, pieceCid),
            )
          }

          await logRetrievalResult(env, {
            cacheMiss: retrievalResult.cacheMiss,
            cacheMissResponseValid,
            responseStatus: retrievalResult.response.status,
            egressBytes,
            requestCountryCode,
            timestamp: requestTimestamp,
            performanceStats: {
              fetchTtfb: firstByteAt - fetchStartedAt,
              fetchTtlb: lastByteFetchedAt - fetchStartedAt,
              workerTtfb: firstByteAt - workerStartedAt,
            },
            dataSetId: retrievalCandidate.dataSetId,
            botName,
          })

          await updateDataSetStats(env, {
            dataSetId: retrievalCandidate.dataSetId,
            egressBytes,
            cacheMiss: retrievalResult.cacheMiss,
            cacheMissResponseValid,
            enforceEgressQuota: env.ENFORCE_EGRESS_QUOTA,
          })
        })(),
      )

      // Return immediately, proxying the transformed response
      const response = new Response(returnedStream, {
        status: retrievalResult.response.status,
        statusText: retrievalResult.response.statusText,
        headers: retrievalResult.response.headers,
      })
      setContentSecurityPolicy(response)
      response.headers.set('X-Data-Set-ID', retrievalCandidate.dataSetId)
      response.headers.set(
        'Cache-Control',
        `public, max-age=${env.CLIENT_CACHE_TTL}`,
      )
      try {
        const contentLengthHeader =
          retrievalResult.response.headers.get('content-length')
        const estimatedEgress = contentLengthHeader
          ? Number.parseInt(contentLengthHeader, 10) || 0
          : 0
        const remainingCdn =
          retrievalCandidate.cdnEgressQuota - BigInt(estimatedEgress)
        const remainingCacheMiss =
          retrievalCandidate.cacheMissEgressQuota - BigInt(estimatedEgress)
        response.headers.set(
          'X-Cdn-Egress-Remaining',
          String(remainingCdn < 0n ? 0n : remainingCdn),
        )
        response.headers.set(
          'X-Cache-Miss-Egress-Remaining',
          String(remainingCacheMiss < 0n ? 0n : remainingCacheMiss),
        )
      } catch (e) {
        console.warn('Failed to compute egress remaining headers', e)
      }
      return response
    } catch (error) {
      const { status } = getErrorHttpStatusMessage(error)

      ctx.waitUntil(
        logRetrievalResult(env, {
          cacheMiss: null,
          cacheMissResponseValid: null,
          responseStatus: status,
          egressBytes: null,
          requestCountryCode,
          timestamp: requestTimestamp,
          dataSetId: null,
          botName,
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
