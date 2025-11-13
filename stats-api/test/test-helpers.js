/**
 * Creates a data set with egress quotas in the database
 *
 * @param {Env} env - Environment bindings
 * @param {Object} options - Configuration options
 * @param {string} options.dataSetId - Data set ID
 * @param {string} options.serviceProviderId - Service provider ID
 * @param {string} options.payerAddress - Payer wallet address
 * @param {boolean} options.withCDN - Whether CDN is enabled
 * @param {number} options.cdnEgressQuota - CDN egress quota in bytes
 * @param {number} options.cacheMissEgressQuota - Cache miss egress quota in
 *   bytes
 */
export async function withDataSet(
  env,
  {
    dataSetId = '1',
    serviceProviderId = '1',
    payerAddress = '0x1234567890abcdef1234567890abcdef12345608',
    withCDN = true,
    cdnEgressQuota = 0,
    cacheMissEgressQuota = 0,
  } = {},
) {
  await env.DB.prepare(
    `INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn)
     VALUES (?, ?, ?, ?)`,
  )
    .bind(dataSetId, serviceProviderId, payerAddress.toLowerCase(), withCDN)
    .run()

  await env.DB.prepare(
    `INSERT INTO data_set_egress_quotas (data_set_id, cdn_egress_quota, cache_miss_egress_quota)
      VALUES (?, ?, ?)`,
  )
    .bind(dataSetId, cdnEgressQuota, cacheMissEgressQuota)
    .run()
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
