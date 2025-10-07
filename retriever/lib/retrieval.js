/**
 * Retrieves the file under the pieceCID from the constructed URL.
 *
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
 * }>}
 *
 *   - The response from the fetch request, the cache miss and the content length.
 */
export async function retrieveFile(
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

  if (response) {
    cacheMiss = false
  } else {
    response = await fetch(url, { signal })
    if (response.ok) {
      const [body1, body2] = response.body?.tee() ?? [null, null]
      await caches.default.put(
        url,
        new Response(body1, {
          ...response,
          headers: {
            ...Object.fromEntries(response.headers),
            'Cache-Control': `public, max-age=${cacheTtl}`,
          },
        }),
      )
      response = new Response(body2, response)
    }
  }

  return { response, cacheMiss }
}

/**
 * Measures the egress of a request by reading from a readable stream and return
 * the total number of bytes transferred.
 *
 * @param {ReadableStreamDefaultReader<Uint8Array>} reader - The reader for the
 *   readable stream.
 * @returns {Promise<number>} - A promise that resolves to the total number of
 *   bytes transferred.
 */
export async function measureStreamedEgress(reader) {
  let total = 0
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    total += value.length
  }
  return total
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
