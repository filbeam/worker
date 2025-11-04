/** Request handlers for the stats API */

/**
 * Gets statistics for a specific data set
 *
 * @param {Env} env - Environment bindings including database
 * @param {string} dataSetId - The data set ID to query
 * @returns {Promise<Response>} JSON response with egress quotas or error
 */
export async function handleGetDataSetStats(env, dataSetId) {
  const result = await env.DB.prepare(
    'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?',
  )
    .bind(dataSetId)
    .first()

  if (!result) {
    return new Response('Not Found', { status: 404 })
  }

  const response = {
    cdn_egress_quota: String(result.cdn_egress_quota ?? 0),
    cache_miss_egress_quota: String(result.cache_miss_egress_quota ?? 0),
  }

  return new Response(JSON.stringify(response), {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
  })
}
