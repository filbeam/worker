/**
 * Convert Filecoin epoch to timestamp in milliseconds
 *
 * @param {bigint} epoch - Filecoin epoch number
 * @param {bigint} genesisBlockTimestampMs - Genesis block timestamp in
 *   milliseconds
 * @returns {number} Timestamp in milliseconds
 */
export function epochToTimestampMs(epoch, genesisBlockTimestampMs) {
  return Number(epoch) * 30 * 1000 + Number(genesisBlockTimestampMs)
}
/**
 * Convert timestamp in milliseconds to Filecoin epoch
 *
 * @param {number} timestampMs - Timestamp in milliseconds
 * @param {bigint} genesisBlockTimestampMs - Genesis block timestamp in
 *   milliseconds
 * @returns {number} Filecoin epoch number
 */

export function timestampMsToEpoch(timestampMs, genesisBlockTimestampMs) {
  return Math.floor(
    (timestampMs - Number(genesisBlockTimestampMs)) / (30 * 1000),
  )
}
