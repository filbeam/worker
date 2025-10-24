/**
 * Aggregate usage data, for all data sets, between last reported timestamp and
 * a target timestamp
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
export async function aggregateUsageData(db, upToTimestampMs) {
  // Query aggregates total usage data between usage_reported_until and upToTimestampMs
  // Returns sum of CDN bytes, cache-miss bytes, and max timestamp for each dataset
  // Excludes datasets with pending transactions to prevent double-counting
  const upToTimestampIso = new Date(upToTimestampMs).toISOString()
  const query = `
    SELECT
      rl.data_set_id,
      -- Note: cdn_bytes tracks all egress (cache hits + cache misses)
      -- cache_miss_bytes tracks only cache misses (subset of cdn_bytes)
      SUM(rl.egress_bytes) as cdn_bytes,
      SUM(CASE WHEN rl.cache_miss = 1 THEN rl.egress_bytes ELSE 0 END) as cache_miss_bytes
    FROM retrieval_logs rl
    INNER JOIN data_sets ds ON rl.data_set_id = ds.id
    WHERE rl.timestamp > datetime(ds.usage_reported_until)
      AND rl.timestamp <= datetime(?)
      AND rl.egress_bytes IS NOT NULL
      AND ds.pending_usage_report_tx_hash IS NULL
    GROUP BY rl.data_set_id
    HAVING (cdn_bytes > 0 OR cache_miss_bytes > 0)
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
 * Prepare usage report data for FilBeam contract call
 *
 * @param {{
 *   data_set_id: string
 *   cdn_bytes: number
 *   cache_miss_bytes: number
 * }[]} usageData
 * @returns {{
 *   dataSetIds: string[]
 *   cdnBytesUsed: bigint[]
 *   cacheMissBytesUsed: bigint[]
 * }}
 */
export function prepareUsageReportData(usageData) {
  const dataSetIds = []
  const cdnBytesUsed = []
  const cacheMissBytesUsed = []

  for (const usage of usageData) {
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
