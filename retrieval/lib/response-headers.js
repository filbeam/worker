import { setContentSecurityPolicy } from './content-security-policy.js'

/**
 * Applies the standard headers for a successful retrieval response: the content
 * security policy, the data set id, and the client cache policy.
 *
 * @param {Response} response
 * @param {object} options
 * @param {string} options.dataSetId
 * @param {number} options.clientCacheTtl - `Cache-Control` max-age in seconds.
 */
export function setRetrievalResponseHeaders(
  response,
  { dataSetId, clientCacheTtl },
) {
  setContentSecurityPolicy(response)
  response.headers.set('X-Data-Set-ID', dataSetId)
  response.headers.set('Cache-Control', `public, max-age=${clientCacheTtl}`)
}

/**
 * Builds the proxied retrieval response: a new response carrying the given body
 * and the upstream status and headers, with the standard retrieval response
 * headers applied via {@link setRetrievalResponseHeaders}.
 *
 * @param {{ CLIENT_CACHE_TTL: number }} env
 * @param {object} params
 * @param {BodyInit | null} params.body
 * @param {number} params.status
 * @param {string} params.statusText
 * @param {HeadersInit} params.headers
 * @param {string} params.dataSetId
 * @returns {Response}
 */
export function buildRetrievalResponse(
  env,
  { body, status, statusText, headers, dataSetId },
) {
  const response = new Response(body, { status, statusText, headers })
  setRetrievalResponseHeaders(response, {
    dataSetId,
    clientCacheTtl: env.CLIENT_CACHE_TTL,
  })
  return response
}
