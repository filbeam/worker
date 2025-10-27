// Helper to seed a wallet
export async function withWallet(env, address, isSanctioned = false) {
  await env.DB.prepare(
    `INSERT INTO wallet_details (address, is_sanctioned, last_screened_at) VALUES (?, ?, datetime('now'))`,
  )
    .bind(address, isSanctioned)
    .run()
}

// Helper to seed a data set
export async function withDataSet(
  env,
  {
    id = '1',
    serviceProviderId = '1',
    payerAddress = '0xPayer',
    withCDN = true,
    terminateServiceTxHash = null,
    lockupUnlocksAt = null,
    usageReportedUntil = null,
  },
) {
  // Ensure service provider exists
  await env.DB.prepare(
    `INSERT OR IGNORE INTO service_providers (id, service_url) VALUES (?, ?)`,
  )
    .bind(String(serviceProviderId), 'https://example.com')
    .run()

  // Use provided values directly (they should be ISO date strings now)
  const usageReportedUntilValue =
    usageReportedUntil || '1970-01-01T00:00:00.000Z'
  const lockupUnlocksAtValue = lockupUnlocksAt

  await env.DB.prepare(
    `INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, terminate_service_tx_hash, lockup_unlocks_at, usage_reported_until) VALUES (?, ?, ?, ?, ?, ?, ?)`,
  )
    .bind(
      String(id),
      String(serviceProviderId),
      payerAddress,
      withCDN,
      terminateServiceTxHash,
      lockupUnlocksAtValue,
      usageReportedUntilValue,
    )
    .run()
}

/**
 * Creates a nextId function that returns sequential IDs starting from 1 Each
 * test file gets its own counter that resets between test files
 *
 * @returns {Function} A function that returns the next sequential ID as a
 *   string
 */
export function createNextId() {
  let counter = 0
  return () => String(++counter)
}

/**
 * Get a date that is N days ago (or in the future if negative)
 *
 * @param {number} daysAgo - Number of days ago (positive for past, negative for
 *   future)
 * @returns {string} ISO format date string
 */
export function getDaysAgo(daysAgo) {
  const date = new Date()
  date.setDate(date.getDate() - daysAgo)
  return date.toISOString()
}

// Filecoin epoch constants
const FILECOIN_GENESIS_UNIX_TIMESTAMP = 1598306400
const FILECOIN_EPOCH_DURATION_SECONDS = 30

/**
 * Converts a Filecoin epoch to a Unix timestamp
 *
 * @param {number} epoch - The Filecoin epoch number
 * @returns {number} Unix timestamp in seconds
 */
export function filecoinEpochToTimestamp(epoch) {
  return (
    epoch * FILECOIN_EPOCH_DURATION_SECONDS + FILECOIN_GENESIS_UNIX_TIMESTAMP
  )
}

/**
 * Converts a Unix timestamp to a Filecoin epoch
 *
 * @param {number} timestamp - Unix timestamp in seconds
 * @returns {number} The Filecoin epoch number
 */
export function timestampToFilecoinEpoch(timestamp) {
  return Math.floor(
    (timestamp - FILECOIN_GENESIS_UNIX_TIMESTAMP) /
      FILECOIN_EPOCH_DURATION_SECONDS,
  )
}

/**
 * Helper to insert a retrieval log entry
 *
 * @param {Object} env - Environment object with DB
 * @param {Object} params - Parameters for the retrieval log
 * @param {number} params.timestamp - Unix timestamp in seconds
 * @param {string} params.dataSetId - Data set ID
 * @param {number} params.responseStatus - HTTP response status (default: 200)
 * @param {number | null} params.egressBytes - Egress bytes (default: null)
 * @param {number} params.cacheMiss - Cache miss flag (0 or 1, default: 0)
 */
export async function withRetrievalLog(
  env,
  {
    timestamp,
    dataSetId,
    responseStatus = 200,
    egressBytes = null,
    cacheMiss = 0,
  },
) {
  return await env.DB.prepare(
    `INSERT INTO retrieval_logs (timestamp, data_set_id, response_status, egress_bytes, cache_miss)
     VALUES (datetime(?, 'unixepoch'), ?, ?, ?, ?)`,
  )
    .bind(timestamp, dataSetId, responseStatus, egressBytes, cacheMiss)
    .run()
}
