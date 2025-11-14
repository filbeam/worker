/**
 * @param {Env} env
 * @param {Object} options
 * @param {string} options.dataSetId
 * @param {number} options.serviceProviderId
 * @param {string} options.payerAddress
 * @param {boolean} options.withCDN
 * @param {number} options.cdnEgressQuota
 * @param {number} options.cacheMissEgressQuota
 */
export async function withDataSet(
  env,
  {
    dataSetId = '0',
    serviceProviderId = 0,
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
    .bind(
      dataSetId,
      String(serviceProviderId),
      payerAddress.toLowerCase(),
      withCDN,
    )
    .run()

  await env.DB.prepare(
    `INSERT INTO data_set_egress_quotas (data_set_id, cdn_egress_quota, cache_miss_egress_quota)
      VALUES (?, ?, ?)`,
  )
    .bind(dataSetId, cdnEgressQuota, cacheMissEgressQuota)
    .run()
}
