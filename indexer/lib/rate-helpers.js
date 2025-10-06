/**
 * Helper functions and constants for rate calculations Rates are configured per
 * TiB and converted to per-byte for calculations
 */

// Storage unit constants
export const BYTES_PER_KIB = 1024n
export const BYTES_PER_MIB = BYTES_PER_KIB * 1024n
export const BYTES_PER_GIB = BYTES_PER_MIB * 1024n
export const BYTES_PER_TIB = BYTES_PER_GIB * 1024n // 1,099,511,627,776 bytes

// USDFC token has 18 decimal places
export const USDFC_DECIMALS = 18n
export const USDFC_UNIT = 10n ** USDFC_DECIMALS // 1e18 units per USDFC

/**
 * Convert rate per TiB to rate per byte
 *
 * @param {string | bigint} ratePerTiB - Rate in USDFC units per TiB (e.g.,
 *   "5000000000000000000" for 5 USDFC per TiB)
 * @returns {bigint} Rate in USDFC units per byte
 */
export function calculateRatePerByte(ratePerTiB) {
  const rate = BigInt(ratePerTiB)
  // Rate per byte = Rate per TiB / Bytes per TiB
  // We use BigInt to avoid precision loss
  return rate / BYTES_PER_TIB
}

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

  // Quota in bytes = (Lockup amount / Rate per TiB) * Bytes per TiB
  // Simplified: Quota = Lockup * BYTES_PER_TIB / Rate
  // This avoids intermediate division and maintains precision
  return (lockup * BYTES_PER_TIB) / rate
}

/**
 * Convert USDFC amount to its smallest unit representation (18 decimals)
 *
 * @param {number | string} amount - Amount in USDFC
 * @returns {string} Amount in USDFC smallest units as string
 */
export function formatUsdfcAmount(amount) {
  return BigInt(Math.floor(Number(amount) * 1e18)).toString()
}
