/**
 * @param {{ DB: D1Database }} env - Worker environment (contains D1 binding).
 * @param {object} params - Parameters for the data set update.
 * @param {string} params.dataSetId - The ID of the data set to update.
 * @param {number} params.egressBytes - The egress bytes used for the response.
 * @param {boolean} params.cacheMiss - Whether this was a cache miss (true) or
 *   cache hit (false).
 * @param {boolean} [params.cacheMissResponseValid]
 * @param {boolean} [params.enforceEgressQuota=false] - Whether to decrement
 *   egress quotas. Default is `false`
 * @param {boolean} [params.isBotTraffic=false] - Whether the egress traffic
 *   originated from the bot. Default is `false`
 */
export async function updateDataSetStats(
  env,
  {
    dataSetId,
    egressBytes,
    cacheMiss,
    cacheMissResponseValid,
    enforceEgressQuota = false,
  },
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
      .bind(
        egressBytes,
        cacheMiss && cacheMissResponseValid ? egressBytes : 0,
        dataSetId,
      )
      .run()
  }
}

/**
 * Logs the result of a file retrieval attempt to the D1 database.
 *
 * @param {{ DB: D1Database }} env - Worker environment (contains D1 binding).
 * @param {object} params - Parameters for the retrieval log.
 * @param {number | null} params.egressBytes - The egress bytes of the response.
 * @param {number} params.responseStatus - The HTTP response status code.
 * @param {boolean | null} params.cacheMiss - Whether the retrieval was a cache
 *   miss.
 * @param {boolean | null} params.cacheMissResponseValid
 * @param {{
 *   fetchTtfb: number
 *   fetchTtlb: number
 *   workerTtfb: number
 * } | null} [params.performanceStats]
 *   - Performance statistics.
 *
 * @param {string} params.timestamp - The timestamp of the retrieval.
 * @param {string | null} params.requestCountryCode - The country code where the
 *   request originated from
 * @param {string | null} params.dataSetId - The data set ID associated with the
 *   retrieval
 * @param {string | undefined} params.botName - The name of the bot making the
 *   request, or null for anonymous requests
 * @returns {Promise<void>} - A promise that resolves when the log is inserted.
 */
export async function logRetrievalResult(env, params) {
  const {
    cacheMiss,
    cacheMissResponseValid,
    egressBytes,
    responseStatus,
    timestamp,
    performanceStats,
    requestCountryCode,
    dataSetId,
    botName,
  } = params

  try {
    await env.DB.prepare(
      `
      INSERT INTO retrieval_logs (
        timestamp,
        response_status,
        egress_bytes,
        cache_miss,
        cache_miss_response_valid,
        fetch_ttfb,
        fetch_ttlb,
        worker_ttfb,
        request_country_code,
        data_set_id,
        bot_name
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
    )
      .bind(
        timestamp,
        responseStatus,
        egressBytes,
        cacheMiss,
        cacheMissResponseValid,
        performanceStats?.fetchTtfb ?? null,
        performanceStats?.fetchTtlb ?? null,
        performanceStats?.workerTtfb ?? null,
        requestCountryCode,
        dataSetId,
        botName ?? null,
      )
      .run()
  } catch (error) {
    console.error(`Error inserting log: ${error}`)
    // TODO: Handle specific SQL error codes if needed
    throw error
  }
}
