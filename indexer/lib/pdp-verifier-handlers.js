/**
 * @param {Env} env
 * @param {string} dataSetId
 * @param {string} pieceId
 * @param {string} pieceCid
 */
export async function insertDataSetPiece(env, dataSetId, pieceId, pieceCid) {
  await env.DB.prepare(
    `INSERT INTO pieces (
      id,
      data_set_id,
      cid
    ) VALUES (?, ?, ?)
    ON CONFLICT DO NOTHING
    `,
  )
    .bind(pieceId, dataSetId, pieceCid)
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
