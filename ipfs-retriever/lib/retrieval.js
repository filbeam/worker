/**
 * Retrieves the IPFS content from the SP serving requests at the provided base
 * URL.
 *
 * @param {string} baseUrl - The base URL of service provider.
 * @param {string} ipfsRootCid - The IPFS Root CID to retrieve from.
 * @param {string} ipfsSubpath - The subpath inside the UnixFS archive to
 *   retrieve, e.g. `/favicon.ico`.
 * @param {number} [cacheTtl=86400] - Cache TTL in seconds (default: 86400).
 *   Default is `86400`
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
export async function retrieveIpfsContent(
  baseUrl,
  ipfsRootCid,
  ipfsSubpath,
  cacheTtl = 86400,
  { signal } = {},
) {
  // TODO: allow the caller to tweak Trustless GW parameters like `dag-scope` when requesting `format=car`.
  // See https://specs.ipfs.tech/http-gateways/trustless-gateway/
  // TODO: support `raw` format too, see https://github.com/filbeam/worker/issues/295
  const url = getRetrievalUrl(baseUrl, ipfsRootCid, ipfsSubpath) + '?format=car'
  const response = await fetch(url, {
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

/**
 * @param {string} serviceUrl
 * @param {string} rootCid
 * @param {string} subpath
 * @returns {string}
 */
export function getRetrievalUrl(serviceUrl, rootCid, subpath) {
  if (!serviceUrl.endsWith('/')) {
    serviceUrl += '/'
  }
  return `${serviceUrl}ipfs/${rootCid}${subpath}`
}
