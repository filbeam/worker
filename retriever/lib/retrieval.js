/**
 * Retrieves the file under the pieceCID from the constructed URL.
 *
 * @param {string} baseUrl - The base URL to storage provider serving the piece.
 * @param {string} pieceCid - The CID of the piece to retrieve.
 * @param {number} [cacheTtl=86400] - Cache TTL in seconds (default: 86400).
 *   Default is `86400`
 * @returns {Promise<{
 *   response: Response
 *   cacheMiss: boolean
 * }>}
 *
 *   - The response from the fetch request, the cache miss and the content length.
 */
export async function retrieveFile(baseUrl, pieceCid, cacheTtl = 86400) {
  const url = `${baseUrl}/piece/${pieceCid}`
  const response = await fetch(url, {
    cf: {
      cacheTtlByStatus: {
        '200-299': cacheTtl,
        404: 0,
        '500-599': 0,
      },
      cacheEverything: true,
    },
  })
  const cacheStatus = response.headers.get('CF-Cache-Status')
  if (!cacheStatus) {
    console.log(`CF-Cache-Status was not provided for ${url}`)
  }

  const cacheMiss = cacheStatus !== 'HIT'

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
