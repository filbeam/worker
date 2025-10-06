/**
 * Retrieves the file under the pieceCID from the constructed URL.
 *
 * @param {string} baseUrl - The base URL to service provider serving the piece.
 * @param {string} pieceCid - The CID of the piece to retrieve.
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
export async function retrieveFile(
  baseUrl,
  pieceCid,
  cacheTtl = 86400,
  { signal } = {},
) {
  const url = getRetrievalUrl(baseUrl, pieceCid)
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
 * Creates a transform stream that enforces quota limits during streaming. This
 * stream passes data through while tracking bytes and stopping when quota is
 * exceeded.
 *
 * @param {string | null} availableQuota - The available quota in bytes (null
 *   means no limit, '0' means no data allowed)
 * @returns {{
 *   stream: TransformStream<Uint8Array, Uint8Array>
 *   getStatus: () => { egressBytes: number; quotaExceeded: boolean }
 * }}
 *   - Transform stream and status getter
 */
export function createQuotaEnforcingStream(availableQuota) {
  let egressBytes = 0
  let quotaExceeded = false

  // null means no limit (allow all), otherwise enforce the quota strictly
  const hasQuotaLimit = availableQuota !== null
  const quotaLimit = hasQuotaLimit ? BigInt(availableQuota) : null

  const stream = new TransformStream({
    transform(chunk, controller) {
      const chunkSize = chunk.length

      // If no quota limit (null), pass through all data
      if (!hasQuotaLimit) {
        controller.enqueue(chunk)
        egressBytes += chunkSize
        return
      }

      // Check if this chunk would exceed quota
      // We know quotaLimit is not null here because hasQuotaLimit is true
      const limit = /** @type {bigint} */ (quotaLimit)
      if (BigInt(egressBytes + chunkSize) > limit) {
        // Calculate how many bytes we can still transfer
        const remainingQuota = Number(limit - BigInt(egressBytes))

        if (remainingQuota > 0) {
          // Transfer only what fits in the quota
          const partialChunk = chunk.slice(0, remainingQuota)
          controller.enqueue(partialChunk)
          egressBytes += remainingQuota
        }

        // Terminate stream gracefully - don't throw error, just stop
        quotaExceeded = true
        controller.terminate()
        return
      }

      // Transfer the full chunk
      controller.enqueue(chunk)
      egressBytes += chunkSize
    },
  })

  return {
    stream,
    getStatus: () => ({ egressBytes, quotaExceeded }),
  }
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
