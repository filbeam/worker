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
    request.signal.addEventListener('abort', () => {
      console.log('The request was aborted!', { url: request.url })
    })
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
        return response
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
            await retrievalResult.response.body
              .pipeThrough(measureStream)
              .pipeTo(returnedStream.writable)
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
          } catch (err) {
            console.error('Error in server stream:', err)
            logStreamStats()
            throw err
          }
        })(),
      )

      // Return immediately, proxying the transformed response
      const response = new Response(returnedStream.readable, {
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
      return response
    } catch (error) {
      const { status } = getErrorHttpStatusMessage(error)
      const statusToLog = String(error).includes('Network connection lost.')
        ? 900
        : status

      ctx.waitUntil(
        logRetrievalResult(env, {
          cacheMiss: null,
          cacheMissResponseValid: null,
          responseStatus: statusToLog,
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
