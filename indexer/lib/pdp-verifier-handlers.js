/**
 * @param {Env} env
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
    ON CONFLICT DO UPDATE SET
      ipfs_root_cid = excluded.ipfs_root_cid
    `,
  )
    .bind(pieceId, dataSetId, pieceCid, ipfsRootCid)
    .run()
}

/**
 * @param {Env} env
 * @param {number | string} dataSetId
 * @param {(number | string)[]} pieceIds
 */
export async function removeDataSetPieces(env, dataSetId, pieceIds) {
  await env.DB.prepare(
    `
    DELETE FROM pieces
    WHERE data_set_id = ? AND id IN (${new Array(pieceIds.length)
      .fill(null)
      .map(() => '?')
      .join(', ')})
    `,
  )
    .bind(String(dataSetId), ...pieceIds.map(String))
    .run()
}
