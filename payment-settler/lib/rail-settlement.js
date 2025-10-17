/**
 * Fetches data sets that need CDN payment rail settlement
 *
 * @param {D1Database} db - The database connection
 * @returns {Promise<string[]>} Array of data set IDs that need settlement
 */
export async function getDataSetsForSettlement(db) {
  const result = await db
    .prepare(
      `
      SELECT id
      FROM data_sets
      WHERE (with_cdn = 1 OR lockup_unlocks_at >= datetime('now'))
        AND terminate_service_tx_hash IS NULL
        AND usage_reported_until >= datetime('now', '-30 days')
      `,
    )
    .all()

  return result.results.map((row) => String(row.id))
}
