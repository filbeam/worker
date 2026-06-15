import assert from 'node:assert/strict'
import { createPieceCIDStream } from './piece.js'

/**
 * Retrieves the file under the pieceCID from the constructed URL.
 *
 * @param {ExecutionContext} ctx
 * @param {string} baseUrl - The base URL to service provider serving the piece.
 * @param {string} pieceCid - The CID of the piece to retrieve.
 * @param {Request} request - Worker request
 * @param {number} [cacheTtl=86400] - Cache TTL in seconds. Default is `86400`
 * @param {object} [options] - Optional parameters.
 * @param {AbortSignal} [options.signal] - An optional AbortSignal to cancel the
 *   fetch request.
 * @param {boolean} [options.addCacheMissResponseValidation=false] Default is
 *   `false`
 * @returns {Promise<{
 *   response: Response
 *   cacheMiss: boolean
 *   url: string
 *   validate: function | null
 * }>}
 *   - The response from the fetch request, the cache miss and the content length.
 */
export async function retrieveFile(
  ctx,
  baseUrl,
  pieceCid,
  request,
  cacheTtl = 86400,
  { signal, addCacheMissResponseValidation = false } = {},
) {
  const url = getRetrievalUrl(baseUrl, pieceCid)

  const cacheKey = new Request(url, request)
  let response = await caches.default.match(cacheKey)
  let cacheMiss = true
  let validate = null

  if (response) {
    cacheMiss = false
  } else {
    response = await fetch(url, {
      cf: {
        cacheTtlByStatus: {
          '200-299': cacheTtl,
          404: 0,
          '500-599': 0,
        },
        cacheEverything: true,
      },
      signal,
    })
    if (response.ok) {
      assert(response.body)

      console.log(
        `Cache miss response validation is ${addCacheMissResponseValidation ? 'enabled' : 'disabled'}`,
      )
      if (addCacheMissResponseValidation) {
        const responseStream = response.body
        const { stream: pieceCidStream, getPieceCID } = createPieceCIDStream()
        validate = () => {
          const calculatedPieceCid = getPieceCID()
          return (
            calculatedPieceCid !== null &&
            calculatedPieceCid.toString() === pieceCid
          )
        }
        response = new Response(
          responseStream.pipeThrough(pieceCidStream),
          response,
        )
      }
    }
  }

  return { response, cacheMiss, url, validate }
}

/**
 * @param {string} serviceUrl
 * @param {string} pieceCid
 * @returns {string}
 */
export function getRetrievalUrl(serviceUrl, pieceCid) {
  if (!serviceUrl.endsWith('/')) {
    serviceUrl += '/'
  }
  return `${serviceUrl}piece/${pieceCid}`
}
