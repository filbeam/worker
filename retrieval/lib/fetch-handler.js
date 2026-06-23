import { handleError } from './http-error.js'
import { httpAssert } from './http-assert.js'
import { redirectLegacyDomain } from './redirect.js'
import { handleEmptyBodyResponse } from './empty-body-response.js'
import { setRetrievalResponseHeaders } from './response-headers.js'
import { recordRetrieval, logRetrievalResult } from './stats.js'

/**
 * The successful retrieval outcome a worker hands back to
 * {@link handleFetchRequest}: the response to serve plus the metadata needed to
 * measure egress and log the retrieval.
 *
 * @typedef {object} RetrievalOutcome
 * @property {Response} response - The response to serve. Its body is streamed
 *   to the client and measured; a `null` body is logged as a zero-egress result
 *   and returned unchanged.
 * @property {boolean} cacheMiss
 * @property {string} dataSetId
 * @property {string | undefined} botName
 * @property {number} fetchStartedAt
 * @property {(egressBytes: number) => Promise<{
 *   cacheMissEgressBytes?: number
 *   cacheMissResponseValid: boolean | null
 * }>} finalizeCacheMiss
 *   - Computes the worker-specific cache-miss accounting once the bytes served to
 *       the client are known. Runs after the response has streamed, before the
 *       retrieval is logged.
 */

/**
 * Per-request telemetry shared by the success and error logging paths.
 *
 * @typedef {object} RequestContext
 * @property {string} requestTimestamp - ISO timestamp of the request.
 * @property {string | null} requestCountryCode - The request's `CF-IPCountry`.
 * @property {number} workerStartedAt - `performance.now()` when the worker
 *   started handling the request.
 */

/**
 * Runs a worker's retrieval handler with the shared request lifecycle: log when
 * the request is aborted, reject non-GET/HEAD methods with a `405`, redirect
 * legacy `*.filcdn.io` requests to `*.filbeam.io`, and turn thrown errors into
 * HTTP responses via {@link handleError}.
 *
 * The handler returns either a plain {@link Response} (redirects, the
 * no-service-provider response, ...), which is served as-is, or a
 * {@link RetrievalOutcome}, whose body is streamed to the client while its
 * egress is measured and the retrieval is logged.
 *
 * @param {Request} request
 * @param {{
 *   DB: D1Database
 *   CLIENT_CACHE_TTL: number
 *   ENFORCE_EGRESS_QUOTA: boolean
 * }} env
 * @param {ExecutionContext} ctx
 * @param {(context: RequestContext) => Promise<Response | RetrievalOutcome>} run
 *   - Invokes the worker handler with the per-request telemetry context.
 *
 * @returns {Promise<Response>}
 */
export async function handleFetchRequest(request, env, ctx, run) {
  request.signal.addEventListener('abort', () => {
    console.log('The request was aborted!', { url: request.url })
  })

  /** @type {RequestContext} */
  const context = {
    requestTimestamp: new Date().toISOString(),
    requestCountryCode: request.headers.get('CF-IPCountry'),
    workerStartedAt: performance.now(),
  }

  try {
    httpAssert(
      ['GET', 'HEAD'].includes(request.method),
      405,
      'Method Not Allowed',
    )
    const legacyRedirect = redirectLegacyDomain(request)
    if (legacyRedirect) return legacyRedirect

    const result = await run(context)
    if (result instanceof Response) return result

    return serveRetrievalOutcome(env, ctx, result, context)
  } catch (error) {
    return handleError(error)
  }
}

/**
 * Streams a retrieval response to the client while measuring egress and logging
 * the result on the execution context. Returns the response immediately.
 *
 * @param {{
 *   DB: D1Database
 *   CLIENT_CACHE_TTL: number
 *   ENFORCE_EGRESS_QUOTA: boolean
 * }} env
 * @param {ExecutionContext} ctx
 * @param {RetrievalOutcome} result
 * @param {RequestContext} context
 * @returns {Response}
 */
function serveRetrievalOutcome(
  env,
  ctx,
  result,
  { requestCountryCode, requestTimestamp: timestamp, workerStartedAt },
) {
  const {
    response,
    cacheMiss,
    dataSetId,
    botName,
    fetchStartedAt,
    finalizeCacheMiss,
  } = result

  // No readable body (e.g. a HEAD request or an error status): nothing to
  // stream or measure.
  if (!response.body) {
    return handleEmptyBodyResponse(env, ctx, {
      response,
      cacheMiss,
      dataSetId,
      requestCountryCode,
      timestamp,
      botName,
    })
  }

  // Measure egress by piping the body through a counting transform on its way
  // to the client. This preserves backpressure: the origin is pulled only as
  // fast as the client reads, so a slow client cannot make the worker buffer
  // the whole response in memory.
  const responseBody = response.body
  let egressBytes = 0
  /** @type {number | null} */
  let firstByteAt = null
  const measureStream = new TransformStream({
    transform(chunk, controller) {
      if (firstByteAt === null) firstByteAt = performance.now()
      egressBytes += chunk.length
      controller.enqueue(chunk)
    },
  })
  const returnedStream = new TransformStream()

  ctx.waitUntil(
    (async () => {
      try {
        await Promise.all([
          responseBody.pipeTo(measureStream.writable),
          measureStream.readable.pipeTo(returnedStream.writable),
        ])
        const lastByteFetchedAt = performance.now()
        const startedAt = firstByteAt ?? lastByteFetchedAt

        const { cacheMissEgressBytes, cacheMissResponseValid } =
          await finalizeCacheMiss(egressBytes)

        await recordRetrieval(env, {
          cacheMiss,
          cacheMissResponseValid,
          cacheMissEgressBytes,
          responseStatus: response.status,
          egressBytes,
          requestCountryCode,
          timestamp,
          performanceStats: {
            fetchTtfb: startedAt - fetchStartedAt,
            fetchTtlb: lastByteFetchedAt - fetchStartedAt,
            workerTtfb: startedAt - workerStartedAt,
          },
          dataSetId,
          botName,
          enforceEgressQuota: env.ENFORCE_EGRESS_QUOTA,
        })
      } catch (err) {
        console.error('Error in server stream:', err)

        await logRetrievalResult(env, {
          cacheMiss,
          cacheMissResponseValid: null,
          responseStatus: 900,
          egressBytes,
          requestCountryCode,
          timestamp,
          dataSetId,
          botName,
        })
      }
    })(),
  )

  const proxied = new Response(returnedStream.readable, {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
  })
  setRetrievalResponseHeaders(proxied, {
    dataSetId,
    clientCacheTtl: env.CLIENT_CACHE_TTL,
  })
  return proxied
}
