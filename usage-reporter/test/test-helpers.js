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
    usageReportedUntil = null,
    pendingRollupTxHash = null,
  },
) {
  // Ensure service provider exists
  await env.DB.prepare(
    `INSERT OR IGNORE INTO service_providers (id, service_url) VALUES (?, ?)`,
  )
    .bind(String(serviceProviderId), 'https://example.com')
    .run()

  await env.DB.prepare(
    `INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, terminate_service_tx_hash, usage_reported_until, pending_rollup_tx_hash) VALUES (?, ?, ?, ?, ?, ?, ?)`,
  )
    .bind(
      String(id),
      String(serviceProviderId),
      payerAddress,
      withCDN,
      terminateServiceTxHash,
      usageReportedUntil,
      pendingRollupTxHash,
    )
    .run()
}

export const randomId = () => String(Math.ceil(Math.random() * 1e10))

// Filecoin epoch constants
export const FILECOIN_GENESIS_UNIX_TIMESTAMP = 1598306400
const FILECOIN_EPOCH_DURATION_SECONDS = 30

/**
 * Converts a Filecoin epoch to an ISO 8601 timestamp
 *
 * @param {number} epoch - The Filecoin epoch number
 * @returns {string} ISO 8601 timestamp string
 */
export function filecoinEpochToTimestamp(epoch) {
  const unixTimestamp =
    epoch * FILECOIN_EPOCH_DURATION_SECONDS + FILECOIN_GENESIS_UNIX_TIMESTAMP
  return new Date(unixTimestamp * 1000).toISOString()
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
 * @param {string} params.timestamp - ISO 8601 timestamp string
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
     VALUES (datetime(?), ?, ?, ?, ?)`,
  )
    .bind(timestamp, dataSetId, responseStatus, egressBytes, cacheMiss)
    .run()
}
