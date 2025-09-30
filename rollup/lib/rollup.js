/**
 * Aggregate usage data for all datasets that need reporting
 *
 * @param {D1Database} db
 * @param {number} targetEpoch - Target epoch to aggregate data up to
 * @returns {Promise<
 *   Map<string, { cdnBytes: number; cacheMissBytes: number; epoch: number }>
 * >}
 */
export async function aggregateUsageData(db, targetEpoch) {
  // Query aggregates total usage data between last_reported_epoch and targetEpoch
  // Returns sum of CDN bytes, cache-miss bytes, and max epoch for each dataset
  const query = `
    SELECT
      rl.data_set_id,
      SUM(CASE WHEN rl.cache_miss = 0 THEN rl.egress_bytes ELSE 0 END) as cdn_bytes,
      SUM(CASE WHEN rl.cache_miss = 1 THEN rl.egress_bytes ELSE 0 END) as cache_miss_bytes,
      MAX(CAST((strftime('%s', rl.timestamp) - 1598306400) / 30 AS INTEGER)) as max_epoch
    FROM retrieval_logs rl
    INNER JOIN data_sets ds ON rl.data_set_id = ds.id
    WHERE CAST((strftime('%s', rl.timestamp) - 1598306400) / 30 AS INTEGER) > COALESCE(ds.last_reported_epoch, -1)
      AND CAST((strftime('%s', rl.timestamp) - 1598306400) / 30 AS INTEGER) <= ?
      AND rl.egress_bytes IS NOT NULL
    GROUP BY rl.data_set_id
    HAVING max_epoch <= ?
  `

  const result = await db.prepare(query).bind(targetEpoch, targetEpoch).all()

  const usageMap = new Map()
  for (const row of result.results) {
    usageMap.set(row.data_set_id, {
      cdnBytes: row.cdn_bytes || 0,
      cacheMissBytes: row.cache_miss_bytes || 0,
      epoch: row.max_epoch,
    })
  }

  return usageMap
}

/**
 * Prepare batch data for FilBeam contract call
 *
 * @param {Map<
 *   string,
 *   { cdnBytes: number; cacheMissBytes: number; epoch: number }
 * >} usageData
 * @returns {{
 *   dataSetIds: string[]
 *   epochs: number[]
 *   cdnBytesUsed: bigint[]
 *   cacheMissBytesUsed: bigint[]
 * }}
 */
export function prepareBatchData(usageData) {
  const dataSetIds = []
  const epochs = []
  const cdnBytesUsed = []
  const cacheMissBytesUsed = []

  for (const [dataSetId, usage] of usageData) {
    // Skip datasets with zero usage
    if (usage.cdnBytes === 0 && usage.cacheMissBytes === 0) {
      continue
    }

    dataSetIds.push(dataSetId)
    epochs.push(usage.epoch)
    cdnBytesUsed.push(BigInt(usage.cdnBytes))
    cacheMissBytesUsed.push(BigInt(usage.cacheMissBytes))
  }

  return {
    dataSetIds,
    epochs,
    cdnBytesUsed,
    cacheMissBytesUsed,
  }
}
