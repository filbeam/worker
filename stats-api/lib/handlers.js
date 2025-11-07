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
