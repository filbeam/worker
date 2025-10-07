/**
 * Aggregate usage data for all datasets that need reporting
 *
 * @param {D1Database} db
 * @param {number} targetEpoch - Target epoch to aggregate data up to
 * @param {number} genesisBlockTimestamp - Genesis block timestamp for epoch
 *   calculation
 * @returns {Promise<
 *   {
 *     data_set_id: string
 *     cdn_bytes: number
 *     cache_miss_bytes: number
 *     max_epoch: number
 *   }[]
 * >}
 */
export async function aggregateUsageData(
  db,
  targetEpoch,
  genesisBlockTimestamp,
) {
  // Query aggregates total usage data between last_reported_epoch and targetEpoch
  // Returns sum of CDN bytes, cache-miss bytes, and max epoch for each dataset
  const query = `
    WITH retrieval_logs_with_epoch AS (
      SELECT
        rl.data_set_id,
        rl.cache_miss,
        rl.egress_bytes,
        CAST((strftime('%s', rl.timestamp) - ?) / 30 AS INTEGER) as epoch
      FROM retrieval_logs rl
      WHERE rl.egress_bytes IS NOT NULL
    )
    SELECT
      rle.data_set_id,
      -- Note: cdn_bytes tracks all egress (cache hits + cache misses)
      -- cache_miss_bytes tracks only cache misses (subset of cdn_bytes)
      SUM(rle.egress_bytes) as cdn_bytes,
      SUM(CASE WHEN rle.cache_miss = 1 THEN rle.egress_bytes ELSE 0 END) as cache_miss_bytes,
      MAX(rle.epoch) as max_epoch
    FROM retrieval_logs_with_epoch rle
    INNER JOIN data_sets ds ON rle.data_set_id = ds.id
    WHERE rle.epoch > COALESCE(ds.last_rollup_reported_at_epoch, -1)
      AND rle.epoch <= ?
      AND rle.egress_bytes IS NOT NULL
    GROUP BY rle.data_set_id
    HAVING max_epoch <= ?
      AND (cdn_bytes > 0 OR cache_miss_bytes > 0)
  `

  const { results } = await db
    .prepare(query)
    .bind(genesisBlockTimestamp, targetEpoch, targetEpoch)
    .all()

  return results
}

/**
 * Prepare usage rollup data for FilBeam contract call
 *
 * @param {{
 *   data_set_id: string
 *   cdn_bytes: number
 *   cache_miss_bytes: number
 *   max_epoch: number
 * }[]} usageData
 * @returns {{
 *   dataSetIds: string[]
 *   epochs: number[]
 *   cdnBytesUsed: bigint[]
 *   cacheMissBytesUsed: bigint[]
 * }}
 */
export function prepareUsageRollupData(usageData) {
  const dataSetIds = []
  const epochs = []
  const cdnBytesUsed = []
  const cacheMissBytesUsed = []

  for (const usage of usageData) {
    dataSetIds.push(usage.data_set_id)
    epochs.push(usage.max_epoch)
    cdnBytesUsed.push(BigInt(usage.cdn_bytes))
    cacheMissBytesUsed.push(BigInt(usage.cache_miss_bytes))
  }

  return {
    dataSetIds,
    epochs,
    cdnBytesUsed,
    cacheMissBytesUsed,
  }
}
