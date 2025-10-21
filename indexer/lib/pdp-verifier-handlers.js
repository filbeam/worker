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
 * @param {{ DB: D1Database; INDEX_CACHE_KV: KVNamespace }} env
 * @param {number | string} dataSetId
 * @param {(number | string)[]} pieceIds
 */
export async function removeDataSetPieces(env, dataSetId, pieceIds) {
  await clearDataSetPiecesIndexCache(env, dataSetId, pieceIds)
  await env.DB.prepare(
    `
    DELETE FROM pieces
    WHERE data_set_id = ? AND id IN (${sqlPlaceholders(pieceIds.length)})
    `,
  )
    .bind(String(dataSetId), ...pieceIds.map(String))
    .run()
}

/**
 * @param {{ DB: D1Database; INDEX_CACHE_KV: KVNamespace }} env
 * @param {number | string} dataSetId
 * @param {(number | string)[]} pieceIds
 */
async function clearDataSetPiecesIndexCache(env, dataSetId, pieceIds) {
  const { results } = await env.DB.prepare(
    `
      SELECT data_sets.payer_address AS payerAddress, pieces.cid AS pieceCID
      FROM data_sets
      INNER JOIN pieces ON pieces.data_set_id = data_sets.id
      WHERE data_sets.id = ? AND pieces.id IN (${sqlPlaceholders(pieceIds.length)})
    `,
  )
    .bind(String(dataSetId), ...pieceIds.map(String))
    .run()
  await Promise.all(
    results.map(async ({ payerAddress, pieceCID }) => {
      await env.INDEX_CACHE_KV.delete(`${payerAddress}/${pieceCID}`)
    }),
  )
}

/**
 * @param {number} count
 * @returns String
 */
const sqlPlaceholders = (count) =>
  new Array(count)
    .fill(null)
    .map(() => '?')
    .join(', ')
