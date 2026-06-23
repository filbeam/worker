import { logRetrievalResult } from './stats.js'
import { setRetrievalResponseHeaders } from './response-headers.js'

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
export function handleEmptyBodyResponse(
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
