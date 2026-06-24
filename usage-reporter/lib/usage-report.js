/**
 * Aggregate usage data per data set, between each data set's last reported
 * timestamp and a target timestamp.
 *
 * Usage is reported per data set: `cdn_bytes` is all egress (cache hits and
 * misses), `cache_miss_bytes` is the cache-miss subset. The FilBeamOperator
 * contract resolves each data set to its shared `cdn_rail_id` and aggregates
 * the bandwidth onto that rail, so multiple data sets in one CDN subscription
 * settle bandwidth once. Cache-miss stays per data set (each copy is served by
 * a different provider, a different payee).
 *
 * @param {D1Database} db
 * @param {number} upToTimestampMs - Target timestamp in milliseconds
 * @returns {Promise<
 *   {
 *     data_set_id: string
 *     cdn_bytes: number
 *     cache_miss_bytes: number
 *   }[]
 * >}
 */
export async function aggregateUsageByDataSet(db, upToTimestampMs) {
  const upToTimestampIso = new Date(upToTimestampMs).toISOString()
  const query = `
    SELECT
      rl.data_set_id,
      -- cdn_bytes tracks all egress (cache hits + cache misses)
      SUM(rl.egress_bytes) as cdn_bytes,
      SUM(CASE WHEN rl.cache_miss = 1 AND rl.cache_miss_response_valid = 1 THEN rl.egress_bytes ELSE 0 END) as cache_miss_bytes
    FROM retrieval_logs rl
    INNER JOIN data_sets ds ON rl.data_set_id = ds.id
    WHERE rl.timestamp > datetime(ds.usage_reported_until)
      AND rl.timestamp <= datetime(?)
      AND rl.egress_bytes IS NOT NULL
      AND rl.bot_name IS NULL
      AND ds.pending_usage_report_tx_hash IS NULL
    GROUP BY rl.data_set_id
    HAVING cdn_bytes > 0
  `

  const results = /**
   * @type {{
   *   data_set_id: string
   *   cdn_bytes: number
   *   cache_miss_bytes: number
   * }[]}
   */ (
    /** @type {any[]} */ (
      (await db.prepare(query).bind(upToTimestampIso).all()).results
    )
  )

  return results
}

/**
 * Aggregate usage data, for all data sets, between last reported timestamp and
 * a target timestamp.
 *
 * Usage is reported per data set. dataSetIds lists every data set that
 * contributed, used to advance the per-data-set usage_reported_until
 * watermark.
 *
 * @param {D1Database} db
 * @param {number} upToTimestampMs - Target timestamp in milliseconds
 * @returns {Promise<{
 *   usageByDataSet: {
 *     data_set_id: string
 *     cdn_bytes: number
 *     cache_miss_bytes: number
 *   }[]
 *   dataSetIds: string[]
 * }>}
 */
export async function aggregateUsageData(db, upToTimestampMs) {
  const usageByDataSet = await aggregateUsageByDataSet(db, upToTimestampMs)
  const dataSetIds = usageByDataSet.map((usage) => String(usage.data_set_id))

  return { usageByDataSet, dataSetIds }
}

/**
 * Prepare usage report data for the FilBeam contract call.
 *
 * Produces three parallel arrays aligned by data set, matching the contract's
 * `recordUsageRollups(toEpoch, dataSetIds, cdnBytesUsed, cacheMissBytesUsed)`.
 *
 * @param {{
 *   usageByDataSet: {
 *     data_set_id: string
 *     cdn_bytes: number
 *     cache_miss_bytes: number
 *   }[]
 * }} usageData
 * @returns {{
 *   dataSetIds: string[]
 *   cdnBytesUsed: bigint[]
 *   cacheMissBytesUsed: bigint[]
 * }}
 */
export function prepareUsageReportData({ usageByDataSet }) {
  const dataSetIds = []
  const cdnBytesUsed = []
  const cacheMissBytesUsed = []

  for (const usage of usageByDataSet) {
    dataSetIds.push(usage.data_set_id)
    cdnBytesUsed.push(BigInt(usage.cdn_bytes))
    cacheMissBytesUsed.push(BigInt(usage.cache_miss_bytes))
  }

  return {
    dataSetIds,
    cdnBytesUsed,
    cacheMissBytesUsed,
  }
}
