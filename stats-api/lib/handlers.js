/** Request handlers for the stats API */

/**
 * Gets statistics for a specific data set
 *
 * @param {Env} env - Environment bindings including database
 * @param {string} dataSetId - The data set ID to query
 * @returns {Promise<Response>} JSON response with egress quotas or error
 */
export async function handleGetDataSetStats(env, dataSetId) {
  const quotaResult = await env.DB.prepare(
    `SELECT 
      cdn_egress_quota, 
      cache_miss_egress_quota 
    FROM data_set_egress_quotas 
    WHERE data_set_id = ?`,
  )
    .bind(dataSetId)
    .first()

  if (!quotaResult) {
    return new Response('Not Found', { status: 404 })
  }

  const response = {
    cdnEgressQuota: String(quotaResult?.cdn_egress_quota ?? 0),
    cacheMissEgressQuota: String(quotaResult?.cache_miss_egress_quota ?? 0),
  }

  return new Response(JSON.stringify(response), {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
  })
}

/**
 * Gets statistics for a specific payer
 *
 * @param {Env} env - Environment bindings including database
 * @param {string} payerAddress - The payer address to query
 * @returns {Promise<Response>} JSON response with egress stats
 */
export async function handleGetPayerStats(env, payerAddress) {
  const stats = await env.DB.prepare(
    `WITH client_quotas AS (
      SELECT
        ds.payer_address,
        SUM(dseqs.cdn_egress_quota) AS remaining_cdn_egress_bytes,
        SUM(dseqs.cache_miss_egress_quota) AS remaining_cache_miss_egress_bytes
      FROM
        data_sets ds
      JOIN
        data_set_egress_quotas dseqs ON ds.id = dseqs.data_set_id
      GROUP BY
        ds.payer_address
    ),
    client_stats AS (
      SELECT
        ds.payer_address,
        COUNT(rl.id) AS total_requests,
        SUM(CASE WHEN rl.cache_miss THEN 1 ELSE 0 END) AS cache_miss_requests,
        SUM(rl.egress_bytes) AS total_egress_bytes,
        SUM(CASE WHEN rl.cache_miss THEN rl.egress_bytes ELSE 0 END) AS cache_miss_egress_bytes
      FROM
        retrieval_logs rl
      JOIN
        data_sets ds ON ds.id = rl.data_set_id
      GROUP BY
        ds.payer_address
    )
    SELECT
      cq.remaining_cdn_egress_bytes,
      cq.remaining_cache_miss_egress_bytes,
      cs.*
    FROM
      client_quotas cq
    LEFT JOIN
      client_stats cs ON cs.payer_address = cq.payer_address
    WHERE cq.payer_address = ?`,
  )
    .bind(payerAddress)
    .first()

  if (!stats) {
    return new Response('Not Found', { status: 404 })
  }

  const response = {
    totalRequests: String(stats.total_requests ?? 0),
    cacheMissRequests: String(stats.cache_miss_requests ?? 0),
    totalEgressBytes: String(stats.total_egress_bytes ?? 0),
    cacheMissEgressBytes: String(stats.cache_miss_egress_bytes ?? 0),
    remainingCDNEgressBytes: String(stats.remaining_cdn_egress_bytes ?? 0),
    remainingCacheMissEgressBytes: String(
      stats.remaining_cache_miss_egress_bytes ?? 0,
    ),
  }

  return new Response(JSON.stringify(response), {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
  })
}
