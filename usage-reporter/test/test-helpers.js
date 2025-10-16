import { epochToUnixTimestamp } from '../lib/usage-report.js'

const FILECOIN_CALIBRATION_GENESIS_BLOCK_TIMESTAMP = 1667326380

export const EPOCH_95_TIMESTAMP = epochToUnixTimestamp(
  95n,
  BigInt(FILECOIN_CALIBRATION_GENESIS_BLOCK_TIMESTAMP),
)
export const EPOCH_98_TIMESTAMP = epochToUnixTimestamp(
  98n,
  BigInt(FILECOIN_CALIBRATION_GENESIS_BLOCK_TIMESTAMP),
)
export const EPOCH_99_TIMESTAMP = epochToUnixTimestamp(
  99n,
  BigInt(FILECOIN_CALIBRATION_GENESIS_BLOCK_TIMESTAMP),
)
export const EPOCH_100_TIMESTAMP = epochToUnixTimestamp(
  100n,
  BigInt(FILECOIN_CALIBRATION_GENESIS_BLOCK_TIMESTAMP),
)
export const EPOCH_101_TIMESTAMP = epochToUnixTimestamp(
  101n,
  BigInt(FILECOIN_CALIBRATION_GENESIS_BLOCK_TIMESTAMP),
)
export const EPOCH_95_TIMESTAMP_ISO = new Date(
  EPOCH_95_TIMESTAMP * 1000,
).toISOString()
export const EPOCH_98_TIMESTAMP_ISO = new Date(
  EPOCH_98_TIMESTAMP * 1000,
).toISOString()
export const EPOCH_99_TIMESTAMP_ISO = new Date(
  EPOCH_99_TIMESTAMP * 1000,
).toISOString()
export const EPOCH_100_TIMESTAMP_ISO = new Date(
  EPOCH_100_TIMESTAMP * 1000,
).toISOString()
export const EPOCH_101_TIMESTAMP_ISO = new Date(
  EPOCH_101_TIMESTAMP * 1000,
).toISOString()

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
    usageReportedUntil = '1970-01-01T00:00:00.000Z',
    pendingUsageReportTxHash = null,
  },
) {
  // Ensure service provider exists
  await env.DB.prepare(
    `INSERT OR IGNORE INTO service_providers (id, service_url) VALUES (?, ?)`,
  )
    .bind(String(serviceProviderId), 'https://example.com')
    .run()

  await env.DB.prepare(
    `INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, terminate_service_tx_hash, usage_reported_until, pending_usage_report_tx_hash) VALUES (?, ?, ?, ?, ?, ?, ?)`,
  )
    .bind(
      String(id),
      String(serviceProviderId),
      payerAddress,
      withCDN,
      terminateServiceTxHash,
      usageReportedUntil,
      pendingUsageReportTxHash,
    )
    .run()
}

export const randomId = () => String(Math.ceil(Math.random() * 1e10))

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
