import assert from 'node:assert/strict'
import { createPieceCIDStream } from './piece'

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
  { signal } = {},
) {
  const url = getRetrievalUrl(baseUrl, pieceCid)

  const cacheKey = new Request(url, request)
  let response = await caches.default.match(cacheKey)
  let cacheMiss = true
  let validate = null

  if (response) {
    cacheMiss = false
  } else {
    response = await fetch(url, { signal })
    if (response.ok) {
      assert(response.body)
      const { stream: pieceCidStream, getPieceCID } = createPieceCIDStream()
      validate = () => {
        const calculatedPieceCid = getPieceCID()
        return (
          calculatedPieceCid !== null &&
          calculatedPieceCid.toString() === pieceCid
        )
      }
      const pipelineStream = response.body.pipeThrough(pieceCidStream)
      const [body1, body2] = pipelineStream.tee() ?? [null, null]

      ctx.waitUntil(
        caches.default.put(
          url,
          new Response(body1, {
            ...response,
            headers: {
              ...Object.fromEntries(response.headers),
              'Cache-Control': `public, max-age=${cacheTtl}`,
            },
          }),
        ),
      )

      response = new Response(body2, response)
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
