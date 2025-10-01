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
    WITH retrieval_logs_with_epoch AS (
      SELECT
        rl.data_set_id,
        rl.cache_miss,
        rl.egress_bytes,
        CAST((strftime('%s', rl.timestamp) - 1598306400) / 30 AS INTEGER) as epoch
      FROM retrieval_logs rl
      WHERE rl.egress_bytes IS NOT NULL
    )
    SELECT
      rle.data_set_id,
      SUM(CASE WHEN rle.cache_miss = 0 THEN rle.egress_bytes ELSE 0 END) as cdn_bytes,
      SUM(CASE WHEN rle.cache_miss = 1 THEN rle.egress_bytes ELSE 0 END) as cache_miss_bytes,
      MAX(rle.epoch) as max_epoch
    FROM retrieval_logs_with_epoch rle
    INNER JOIN data_sets ds ON rle.data_set_id = ds.id
    WHERE rle.epoch > COALESCE(ds.last_reported_epoch, -1)
      AND rle.epoch <= ?
      AND rle.egress_bytes IS NOT NULL
    GROUP BY rle.data_set_id
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
