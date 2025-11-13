/**
 * @param {{ DB: D1Database }} env - Worker environment (contains D1 binding).
 * @param {object} params - Parameters for the data set update.
 * @param {string} params.dataSetId - The ID of the data set to update.
 * @param {number} params.egressBytes - The egress bytes used for the response.
 * @param {boolean} params.cacheMiss - Whether this was a cache miss (true) or
 *   cache hit (false).
 * @param {boolean} [params.enforceEgressQuota=false] - Whether to decrement
 *   egress quotas. Default is `false`
 * @param {boolean} [params.isBotTraffic=false] - Whether the egress traffic
 *   originated from the bot. Default is `false`
 */
export async function updateDataSetStats(
  env,
  { dataSetId, egressBytes, cacheMiss, enforceEgressQuota = false },
) {
  await env.DB.prepare(
    `
    UPDATE data_sets
    SET total_egress_bytes_used = total_egress_bytes_used + ?
    WHERE id = ?
    `,
  )
    .bind(egressBytes, dataSetId)
    .run()

  if (enforceEgressQuota) {
    await env.DB.prepare(
      `
      UPDATE data_set_egress_quotas
      SET cdn_egress_quota = cdn_egress_quota - ?,
          cache_miss_egress_quota = cache_miss_egress_quota - ?
      WHERE data_set_id = ?
      `,
    )
      .bind(egressBytes, cacheMiss ? egressBytes : 0, dataSetId)
      .run()
  }
}
