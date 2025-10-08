/**
 * Helper functions and constants for rate calculations Rates are configured per
 * TiB and converted to per-byte for calculations
 */

export const BYTES_PER_TIB = 1024n ** 4n

/**
 * Calculate egress quota in bytes given a lockup amount and rate per TiB
 *
 * @param {string | bigint} lockupAmount - Total lockup amount in USDFC units
 * @param {string | bigint} ratePerTiB - Rate in USDFC units per TiB
 * @returns {bigint} Quota in bytes
 */
export function calculateEgressQuota(lockupAmount, ratePerTiB) {
  const lockup = BigInt(lockupAmount)
  const rate = BigInt(ratePerTiB)

  if (rate === 0n) {
    return 0n
  }

  return (lockup * BYTES_PER_TIB) / rate
}
