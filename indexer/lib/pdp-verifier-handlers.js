/**
 * @param {{ DB: D1Database }} env
 * @param {string} dataSetId
 * @param {string} pieceId
 * @param {string} pieceCid
 * @param {string | null} ipfsRootCid
 */
export async function insertDataSetPiece(
  env,
  dataSetId,
  pieceId,
  pieceCid,
  ipfsRootCid,
) {
  await env.DB.prepare(
    `INSERT INTO pieces (
      id,
      data_set_id,
      cid,
      ipfs_root_cid
    ) VALUES (?, ?, ?, ?)
    ON CONFLICT DO NOTHING
    `,
  )
    .bind(pieceId, dataSetId, pieceCid, ipfsRootCid)
    .run()
}

/**
 * @param {{ DB: D1Database }} env
 * @param {number | string} dataSetId
 * @param {(number | string)[]} pieceIds
 */
export async function removeDataSetPieces(env, dataSetId, pieceIds) {
  await env.DB.prepare(
    `
    INSERT INTO pieces (id, data_set_id, is_deleted)
    VALUES ${new Array(pieceIds.length)
      .fill(null)
      .map(() => '(?, ?, TRUE)')
      .join(', ')}
    ON CONFLICT DO UPDATE set is_deleted = true
    `,
  )
    .bind(...pieceIds.flatMap((pieceId) => [pieceId, dataSetId]))
    .run()
}
