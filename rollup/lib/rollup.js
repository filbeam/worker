/**
 * Convert Filecoin epoch to ISO 8601 timestamp
 *
 * @param {bigint} epoch - Filecoin epoch number
 * @param {bigint} genesisBlockTimestamp - Genesis block timestamp in seconds
 * @returns {string} ISO 8601 timestamp string
 */
export function epochToTimestamp(epoch, genesisBlockTimestamp) {
  const unixTimestamp = Number(epoch) * 30 + Number(genesisBlockTimestamp)
  return new Date(unixTimestamp * 1000).toISOString()
}

/**
 * Aggregate usage data, for all data sets, between last reported timestamp and
 * a target timestamp
 *
 * @param {D1Database} db
 * @param {string} upToTimestamp - Target ISO 8601 timestamp string
 * @param {bigint} genesisBlockTimestamp - Genesis block timestamp in seconds
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
  upToTimestamp,
  genesisBlockTimestamp,
) {
  // Query aggregates total usage data between usage_reported_until and upToTimestamp
  // Returns sum of CDN bytes, cache-miss bytes, and max epoch for each dataset
  // Excludes datasets with pending transactions to prevent double-counting
  const query = `
    SELECT
      rl.data_set_id,
      -- Note: cdn_bytes tracks all egress (cache hits + cache misses)
      -- cache_miss_bytes tracks only cache misses (subset of cdn_bytes)
      SUM(rl.egress_bytes) as cdn_bytes,
      SUM(CASE WHEN rl.cache_miss = 1 THEN rl.egress_bytes ELSE 0 END) as cache_miss_bytes,
      -- Convert max timestamp to epoch: (unix_timestamp - genesis) / 30
      MAX((strftime('%s', rl.timestamp) - ?) / 30) as max_epoch
    FROM retrieval_logs rl
    INNER JOIN data_sets ds ON rl.data_set_id = ds.id
    WHERE rl.timestamp > datetime(COALESCE(ds.usage_reported_until, '1970-01-01T00:00:00.000Z'))
      AND rl.timestamp <= datetime(?)
      AND rl.egress_bytes IS NOT NULL
      AND ds.pending_rollup_tx_hash IS NULL
    GROUP BY rl.data_set_id
    HAVING (cdn_bytes > 0 OR cache_miss_bytes > 0)
  `

  const { results } = await db
    .prepare(query)
    .bind(Number(genesisBlockTimestamp), upToTimestamp)
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
 *   cdnBytesUsed: bigint[]
 *   cacheMissBytesUsed: bigint[]
 *   maxEpochs: number[]
 * }}
 */
export function prepareUsageRollupData(usageData) {
  const dataSetIds = []
  const cdnBytesUsed = []
  const cacheMissBytesUsed = []
  const maxEpochs = []

  for (const usage of usageData) {
    dataSetIds.push(usage.data_set_id)
    cdnBytesUsed.push(BigInt(usage.cdn_bytes))
    cacheMissBytesUsed.push(BigInt(usage.cache_miss_bytes))
    maxEpochs.push(usage.max_epoch)
  }

  return {
    dataSetIds,
    cdnBytesUsed,
    cacheMissBytesUsed,
    maxEpochs,
  }
}
