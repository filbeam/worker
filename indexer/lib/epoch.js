/**
 * Convert a Filecoin epoch to Unix timestamp in milliseconds
 *
 * @param {number | string | bigint} epoch - The Filecoin epoch number
 * @param {number | string | bigint} genesisBlockTimestampMs - The genesis block
 *   Unix timestamp in milliseconds
 * @returns {number} The Unix timestamp in milliseconds
 */
export function epochToTimestampMs(epoch, genesisBlockTimestampMs) {
  return Number(epoch) * 30 * 1000 + Number(genesisBlockTimestampMs)
}

/**
 * Convert a Unix timestamp in milliseconds to Filecoin epoch
 *
 * @param {number} timestampMs - The Unix timestamp in milliseconds
 * @param {number | string | bigint} genesisBlockTimestampMs - The genesis block
 *   Unix timestamp in milliseconds
 * @returns {number} The Filecoin epoch number
 */
export function timestampMsToEpoch(timestampMs, genesisBlockTimestampMs) {
  return Math.floor(
    (timestampMs - Number(genesisBlockTimestampMs)) / (30 * 1000),
  )
}
