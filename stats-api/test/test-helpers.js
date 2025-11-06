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
    `INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota)
     VALUES (?, ?, ?, ?, ?, ?)`,
  )
    .bind(
      dataSetId,
      serviceProviderId,
      payerAddress.toLowerCase(),
      withCDN,
      cdnEgressQuota,
      cacheMissEgressQuota,
    )
    .run()
}
