/**
 * Convert Filecoin epoch to timestamp in milliseconds
 *
 * @param {bigint} epoch - Filecoin epoch number
 * @param {bigint} genesisBlockTimestampSeconds - Genesis block timestamp in
 *   seconds
 * @returns {number} Timestamp in milliseconds
 */
export function epochToTimestampMs(epoch, genesisBlockTimestampSeconds) {
  return (Number(epoch) * 30 + Number(genesisBlockTimestampSeconds)) * 1000
}
/**
 * Convert timestamp in milliseconds to Filecoin epoch
 *
 * @param {number} timestampMs - Timestamp in milliseconds
 * @param {bigint} genesisBlockTimestampSeconds - Genesis block timestamp in
 *   seconds
 * @returns {number} Filecoin epoch number
 */

export function timestampMsToEpoch(timestampMs, genesisBlockTimestampSeconds) {
  return Math.floor(
    (timestampMs / 1000 - Number(genesisBlockTimestampSeconds)) / 30,
  )
}
