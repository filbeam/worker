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
