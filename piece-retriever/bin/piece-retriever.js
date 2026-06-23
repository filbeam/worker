import {
  httpAssert,
  setRetrievalResponseHeaders,
  assertCidNotDenied,
  logRetrievalResult,
  recordRetrieval,
  logRetrievalError,
  handleFetchRequest,
  selectRetrievalCandidate,
  handleEmptyBodyResponse,
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
  async fetch(request, env, ctx, options) {
    return handleFetchRequest(request, () =>
      this._fetch(request, env, ctx, options),
    )
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
    if (URL.parse(request.url)?.pathname === '/') {
      return Response.redirect('https://filbeam.com/', 302)
    }

    const requestTimestamp = new Date().toISOString()
    const workerStartedAt = performance.now()
    const requestCountryCode = request.headers.get('CF-IPCountry')

    const { payerWalletAddress, pieceCid, botName, validateCacheMissResponse } =
      parseRequest(request, env)

    try {
      // Timestamp to measure file retrieval performance (from cache and from SP)
      const fetchStartedAt = performance.now()

      const [retrievalCandidates] = await Promise.all([
        getRetrievalCandidatesAndValidatePayer(
          env,
          payerWalletAddress,
          pieceCid,
          env.ENFORCE_EGRESS_QUOTA,
        ),
        assertCidNotDenied(env, pieceCid),
      ])

      httpAssert(
        retrievalCandidates.length > 0,
        500,
        'Service provider lookup failed',
      )

      const {
        failureResponse,
        candidate: retrievalCandidate,
        result: retrievalResult,
      } = await selectRetrievalCandidate(
        retrievalCandidates,
        (candidate) =>
          retrieveFile(
            ctx,
            candidate.serviceUrl,
            pieceCid,
            request,
            env.ORIGIN_CACHE_TTL,
            {
              signal: request.signal,
              addCacheMissResponseValidation: validateCacheMissResponse,
            },
          ),
        { env, ctx, requestCountryCode, timestamp: requestTimestamp, botName },
      )
      if (failureResponse) return failureResponse
      httpAssert(
        retrievalCandidate && retrievalResult,
        500,
        'should never happen',
      )

      if (!retrievalResult.response.body) {
        return handleEmptyBodyResponse(env, ctx, {
          response: retrievalResult.response,
          cacheMiss: retrievalResult.cacheMiss,
          dataSetId: retrievalCandidate.dataSetId,
          requestCountryCode,
          timestamp: requestTimestamp,
          botName,
        })
      }

      // Stream, count bytes and validate (a cache miss)
      let egressBytes = 0
      /** @type {number | null} */
      let firstByteAt = null

      /** @type {number | null} */
      let minChunkSize = null
      /** @type {number | null} */
      let maxChunkSize = null
      let bytesReceived = 0

      const logStreamStats = () => {
        console.log(
          'Stream stats ' +
            `minChunkSize=${minChunkSize} ` +
            `maxChunkSize=${maxChunkSize} ` +
            `bytesReceived=${bytesReceived} ` +
            `url=${request.url} ` +
            `cf-ray=${request.headers.get('cf-ray')}`,
        )
        minChunkSize = null
        maxChunkSize = null
        bytesReceived = 0
      }

      const iv = setInterval(logStreamStats, 10_000)

      const measureStream = new TransformStream({
        transform(chunk, controller) {
          if (firstByteAt === null) {
            console.log('First byte received')
            firstByteAt = performance.now()
          }
          egressBytes += chunk.length
          bytesReceived += chunk.length
          if (minChunkSize === null || chunk.length < minChunkSize) {
            minChunkSize = chunk.length
          }
          if (maxChunkSize === null || chunk.length > maxChunkSize) {
            maxChunkSize = chunk.length
          }
          controller.enqueue(chunk)
        },
        flush() {
          logStreamStats()
          clearInterval(iv)
        },
      })

      const returnedStream = new TransformStream()

      ctx.waitUntil(
        (async () => {
          try {
            httpAssert(
              retrievalResult.response.body,
              500,
              'Should never happen',
            )
            await Promise.all([
              retrievalResult.response.body.pipeTo(measureStream.writable),
              measureStream.readable.pipeTo(returnedStream.writable),
            ])
            console.log('Response finished')

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

            await recordRetrieval(env, {
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
              enforceEgressQuota: env.ENFORCE_EGRESS_QUOTA,
            })
          } catch (err) {
            console.error('Error in server stream:', err)
            logStreamStats()
            clearInterval(iv)

            await logRetrievalResult(env, {
              cacheMiss: retrievalResult.cacheMiss,
              cacheMissResponseValid: null,
              responseStatus: 900,
              egressBytes,
              requestCountryCode,
              timestamp: requestTimestamp,
              dataSetId: retrievalCandidate.dataSetId,
              botName,
            })
          }
        })(),
      )

      // Return immediately, proxying the transformed response
      const response = new Response(returnedStream.readable, {
        status: retrievalResult.response.status,
        statusText: retrievalResult.response.statusText,
        headers: retrievalResult.response.headers,
      })
      setRetrievalResponseHeaders(response, {
        dataSetId: retrievalCandidate.dataSetId,
        clientCacheTtl: env.CLIENT_CACHE_TTL,
      })
      return response
    } catch (error) {
      logRetrievalError(env, ctx, error, {
        requestCountryCode,
        timestamp: requestTimestamp,
        botName,
      })

      throw error
    }
  },
}
