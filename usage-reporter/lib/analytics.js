/**
 * Calculate total bytes from usage data
 *
 * @param {{
 *   cdn_bytes: number
 *   cache_miss_bytes: number
 * }[]} usageData -
 *   Array of usage data
 * @returns {{
 *   totalCdnBytes: BigInt
 *   totalCacheMissBytes: BigInt
 * }}
 */
export function calculateTotalBytes(usageData) {
  let totalCdnBytes = 0n
  let totalCacheMissBytes = 0n

  for (const usage of usageData) {
    totalCdnBytes += BigInt(usage.cdn_bytes)
    totalCacheMissBytes += BigInt(usage.cache_miss_bytes)
  }

  return {
    totalCdnBytes,
    totalCacheMissBytes,
  }
}
