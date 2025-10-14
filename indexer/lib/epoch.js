/**
 * Convert Filecoin epoch to Unix timestamp
 *
 * @param {number | string} epoch - The Filecoin epoch number
 * @param {number} genesisTimestamp - The genesis timestamp for the network (in
 *   seconds)
 * @returns {number} The corresponding Unix timestamp in seconds
 */
export function epochToTimestamp(epoch, genesisTimestamp) {
  const EPOCH_DURATION_SECONDS = 30 // Filecoin epoch duration is 30 seconds
  return Number(epoch) * EPOCH_DURATION_SECONDS + genesisTimestamp
}
