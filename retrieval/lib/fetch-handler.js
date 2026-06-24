import { handleError } from './http-error.js'
import { httpAssert } from './http-assert.js'
import { redirectLegacyDomain } from './redirect.js'
import { checkBotAuthorization } from './bot-auth.js'
import { setRetrievalResponseHeaders } from './response-headers.js'
import {
  recordRetrieval,
  logRetrievalResult,
  logRetrievalError,
} from './stats.js'

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
 * A worker's retrieval step: looks up and retrieves the content. Returns the
 * {@link RetrievalOutcome} to serve, or a {@link Response} when no service
 * provider could serve the content. An error thrown here is logged as a
 * retrieval error.
 *
 * @typedef {() => Promise<Response | RetrievalOutcome>} Retrieve
 */

/**
 * Per-request telemetry shared by the success and error logging paths.
 *
 * @typedef {object} RequestContext
 * @property {string} requestTimestamp - ISO timestamp of the request.
 * @property {string | null} requestCountryCode - The request's `CF-IPCountry`.
 * @property {number} workerStartedAt - `performance.now()` when the worker
 *   started handling the request.
 * @property {string} [botName] - The bot name resolved from the request's
 *   Authorization header, or `undefined` for anonymous requests.
 */

/**
 * Runs a worker's retrieval handler with the shared request lifecycle: log when
 * the request is aborted, reject non-GET/HEAD methods with a `405`, redirect
 * legacy `*.filcdn.io` requests to `*.filbeam.io`, and turn thrown errors into
 * HTTP responses via {@link handleError}.
 *
 * The handler returns either a plain {@link Response} (redirects), served as-is,
 * or a {@link Retrieve} function. The retrieval is run inside a try/catch that
 * logs a retrieval error on failure; its result is then served: a
 * {@link Response} (the no-service-provider response) as-is, or a
 * {@link RetrievalOutcome} whose body is streamed while egress is measured and
 * the retrieval is logged.
 *
 * @param {Request} request
 * @param {{
 *   DB: D1Database
 *   CLIENT_CACHE_TTL: number
 *   ENFORCE_EGRESS_QUOTA: boolean
 *   BOT_TOKENS: string
 * }} env
 * @param {ExecutionContext} ctx
 * @param {(context: RequestContext) => Promise<Response | Retrieve>} run
 *
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

    context.botName = checkBotAuthorization(request, {
      BOT_TOKENS: env.BOT_TOKENS,
    })

    const retrieve = await run(context)
    if (retrieve instanceof Response) return retrieve

    let outcome
    try {
      outcome = await retrieve()
    } catch (error) {
      logRetrievalError(env, ctx, error, {
        requestCountryCode: context.requestCountryCode,
        timestamp: context.requestTimestamp,
        botName: context.botName,
      })
      throw error
    }

    if (outcome instanceof Response) return outcome
    return serveRetrievalOutcome(env, ctx, outcome, context)
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
  { requestCountryCode, requestTimestamp: timestamp, workerStartedAt, botName },
) {
  const { response, cacheMiss, dataSetId, fetchStartedAt, finalizeCacheMiss } =
    result

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

/**
 * Logs a zero-egress retrieval result and returns the upstream response
 * unchanged. Used when the upstream response carries no readable body (e.g. a
 * non-OK status or a `HEAD` request), so there is nothing to stream or
 * measure.
 *
 * @param {{ DB: D1Database; CLIENT_CACHE_TTL: number }} env
 * @param {ExecutionContext} ctx
 * @param {object} params
 * @param {Response} params.response - The upstream response to return as-is.
 * @param {boolean} params.cacheMiss
 * @param {string} params.dataSetId
 * @param {string | null} params.requestCountryCode
 * @param {string} params.timestamp
 * @param {string | undefined} params.botName
 * @returns {Response}
 */
function handleEmptyBodyResponse(
  env,
  ctx,
  { response, cacheMiss, dataSetId, requestCountryCode, timestamp, botName },
) {
  ctx.waitUntil(
    logRetrievalResult(env, {
      cacheMiss,
      cacheMissResponseValid: null,
      responseStatus: response.status,
      egressBytes: 0,
      cacheMissEgressBytes: 0,
      requestCountryCode,
      timestamp,
      dataSetId,
      botName,
    }),
  )

  const emptyResponse = new Response(response.body, response)
  setRetrievalResponseHeaders(emptyResponse, {
    dataSetId,
    clientCacheTtl: env.CLIENT_CACHE_TTL,
  })
  return emptyResponse
}
