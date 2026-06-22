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
