/**
 * @param {{ DB: D1Database }} env
 * @param {string} dataSetId
 * @param {string} pieceId
 * @param {string} pieceCid
 * @param {string | null} ipfsRootCid
 * @param {string | null} x402Price
 */
export async function insertDataSetPiece(
  env,
  dataSetId,
  pieceId,
  pieceCid,
  ipfsRootCid,
  x402Price,
) {
  await env.DB.prepare(
    `INSERT INTO pieces (
      id,
      data_set_id,
      cid,
      ipfs_root_cid,
      x402_price
    ) VALUES (?, ?, ?, ?, ?)
    ON CONFLICT DO UPDATE SET
      cid = excluded.cid,
      ipfs_root_cid = excluded.ipfs_root_cid,
      x402_price = excluded.x402_price
    `,
  )
    .bind(pieceId, dataSetId, pieceCid, ipfsRootCid, x402Price)
    .run()
}

/**
 * @param {{ DB: D1Database }} env
 * @param {number | string} dataSetId
 * @param {(number | string)[]} pieceIds
 */
export async function removeDataSetPieces(env, dataSetId, pieceIds) {
  // Cloudflare D1 has a limit of 100 bound parameters per query
  // Each piece requires 2 parameters (pieceId, dataSetId), so batch size is 50
  const BATCH_SIZE = 50

  // Process pieces in batches
  for (let i = 0; i < pieceIds.length; i += BATCH_SIZE) {
    const batch = pieceIds.slice(i, i + BATCH_SIZE)
    await env.DB.prepare(
      `
      INSERT INTO pieces (id, data_set_id, is_deleted)
      VALUES ${new Array(batch.length)
        .fill(null)
        .map(() => '(?, ?, TRUE)')
        .join(', ')}
      ON CONFLICT DO UPDATE set is_deleted = true
      `,
    )
      .bind(...batch.flatMap((pieceId) => [pieceId, dataSetId]))
      .run()
  }
}
