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
      SELECT data_sets.id
      FROM data_sets
      LEFT JOIN wallet_details ON data_sets.payer_address = wallet_details.address
      WHERE (data_sets.with_cdn = 1 OR data_sets.lockup_unlocks_at >= datetime('now'))
        AND data_sets.terminate_service_tx_hash IS NULL
        AND data_sets.usage_reported_until >= datetime('now', '-30 days')
        AND (wallet_details.is_sanctioned IS NULL OR wallet_details.is_sanctioned = 0)
      `,
    )
    .all()

  return result.results.map((row) => String(row.id))
}
