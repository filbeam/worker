/**
 * Cloudflare `fetch` cache options for retrieving content from a service
 * provider: cache successful responses for `cacheTtl` seconds and never cache
 * 404 or 5xx responses.
 *
 * @param {number} cacheTtl - Cache TTL in seconds for 2xx responses.
 */
export function originCacheOptions(cacheTtl) {
  return {
    cacheTtlByStatus: { '200-299': cacheTtl, 404: 0, '500-599': 0 },
    cacheEverything: true,
  }
}
