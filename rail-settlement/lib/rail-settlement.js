/**
 * Fetches data sets that need CDN payment rail settlement
 *
 * @param {D1Database} db - The database connection
 * @param {bigint} currentEpoch - The current block number (epoch)
 * @returns {Promise<string[]>} Array of data set IDs that need settlement
 */
export async function getDataSetsForSettlement(db, currentEpoch) {
  const result = await db
    .prepare(
      `
      SELECT id
      FROM data_sets
      WHERE (with_cdn = 1 OR settle_up_to_epoch IS NOT NULL)
        AND (settle_up_to_epoch IS NULL OR settle_up_to_epoch >= ?)
    `,
    )
    .bind(String(currentEpoch))
    .all()

  return result.results.map((row) => String(row.id))
}
