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
 *   egressType: 'cdn' | 'cache-miss'
 *   egressSize: number
 *   url: string
 * }>}
 *   - The response from the fetch request, the egress type, size and the content URL.
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

  if (response) {
    cacheMiss = false
  } else {
    // If not in worker cache, fetch from upstream origin.
    response = await fetch(url, { signal })
    if (response.ok) {
      // Tee the body: one stream for caching, one for the actual response.
      const [body1, body2] = response.body?.tee() ?? [null, null]
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
      // The `response` object for the current request will use the second stream.
      response = new Response(body2, response)
    }
  }

  // Determine egress type: 'cdn' for worker cache hit, 'cache-miss' for upstream fetch.
  const egressType = cacheMiss ? 'cache-miss' : 'cdn'

  let egressSize = 0
  const contentLengthHeader = response.headers.get('Content-Length')

  // Attempt to get egress size from Content-Length header first.
  if (contentLengthHeader) {
    const parsedLength = parseInt(contentLengthHeader, 10)
    if (!isNaN(parsedLength) && parsedLength >= 0) {
      egressSize = parsedLength
    }
  }

  // If Content-Length is missing, invalid, or zero (and response has a body),
  // measure egress by consuming a teed copy of the response body.
  // This ensures the main response body remains unconsumed for the client.
  if (egressSize === 0 && response.body) {
    const [measureBody, actualBody] = response.body.tee()
    egressSize = await measureStreamedEgress(measureBody.getReader())
    // Replace the response body with the unmeasured stream.
    response = new Response(actualBody, response)
  }

  return { response, egressType, egressSize, url }
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