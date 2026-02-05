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

// D1 has a limit of 100 bound parameters per query
// https://developers.cloudflare.com/d1/platform/limits/
// With 2 parameters per piece (pieceId, dataSetId), we can process 50 pieces per batch
const REMOVE_PIECES_BATCH_SIZE = 50

/**
 * @param {{ DB: D1Database }} env
 * @param {number | string} dataSetId
 * @param {(number | string)[]} pieceIds
 */
export async function removeDataSetPieces(env, dataSetId, pieceIds) {
  const statements = []
  for (let i = 0; i < pieceIds.length; i += REMOVE_PIECES_BATCH_SIZE) {
    const batch = pieceIds.slice(i, i + REMOVE_PIECES_BATCH_SIZE)
    statements.push(
      env.DB.prepare(
        `
        INSERT INTO pieces (id, data_set_id, is_deleted)
        VALUES ${batch.map(() => '(?, ?, TRUE)').join(', ')}
        ON CONFLICT DO UPDATE set is_deleted = true
        `,
      ).bind(...batch.flatMap((pieceId) => [pieceId, dataSetId])),
    )
  }

  await env.DB.batch(statements)
}
