/**
 * @param {{ DB: D1Database; X402_METADATA_KV: KVNamespace }} env
 * @param {string} dataSetId
 * @param {string} pieceId
 * @param {string} pieceCid
 * @param {string | null} ipfsRootCid
 * @param {string | null} x402Price
 * @param {number | undefined} blockNumber
 */
export async function insertDataSetPiece(
  env,
  dataSetId,
  pieceId,
  pieceCid,
  ipfsRootCid,
  x402Price,
  blockNumber,
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

  if (!x402Price || !blockNumber) {
    return
  }

  /** @type {{ payer_address: string | null } | null} */
  const dataSet = await env.DB.prepare(
    'SELECT payer_address FROM data_sets WHERE id = ?',
  )
    .bind(dataSetId)
    .first()

  const payerAddress = dataSet?.payer_address ?? null
  if (!payerAddress) return

  const metadataKey = `${payerAddress}:${pieceCid}`

  /** @type {{ price: string; blockNumber: number } | null} */
  const existingMetadata = await env.X402_METADATA_KV.get(metadataKey, 'json')

  if (existingMetadata && blockNumber < Number(existingMetadata.blockNumber)) {
    return
  }

  await env.X402_METADATA_KV.put(
    metadataKey,
    JSON.stringify({
      price: x402Price,
      blockNumber: String(blockNumber),
    }),
  )
}

/**
 * Removes pieces from a dataset and cleans up orphaned KV metadata.
 *
 * @param {{ DB: D1Database; X402_METADATA_KV: KVNamespace }} env
 * @param {number | string} dataSetId
 * @param {(number | string)[]} pieceIds
 */
export async function removeDataSetPieces(env, dataSetId, pieceIds) {
  if (pieceIds.length === 0) return

  /** @type{{ results: { cid: string }[] }} */
  const deletedPieces = await env.DB.prepare(
    `
    INSERT INTO pieces (id, data_set_id, is_deleted)
    VALUES ${new Array(pieceIds.length)
      .fill(null)
      .map(() => '(?, ?, TRUE)')
      .join(', ')}
    ON CONFLICT DO UPDATE SET is_deleted = TRUE
    RETURNING cid
    `,
  )
    .bind(...pieceIds.flatMap((pieceId) => [pieceId, dataSetId]))
    .all()

  /** @type {{ payer_address: string | null } | null} */
  const dataSet = await env.DB.prepare(
    'SELECT payer_address FROM data_sets WHERE id = ?',
  )
    .bind(dataSetId)
    .first()

  const payerAddress = dataSet?.payer_address ?? null
  if (!payerAddress) return

  const deletedCids = [...new Set(deletedPieces.results.map((p) => p.cid))]
  if (deletedCids.length === 0) return

  const remainingPieces = await env.DB.prepare(
    `
    SELECT DISTINCT p.cid FROM pieces p
    JOIN data_sets d ON p.data_set_id = d.id
    WHERE p.cid IN (${deletedCids.map(() => '?').join(', ')})
      AND d.payer_address = ?
      AND p.is_deleted = FALSE
    `,
  )
    .bind(...deletedCids, payerAddress)
    .all()

  const remainingPiecesCids = new Set(remainingPieces.results.map((r) => r.cid))
  const orphanedCids = deletedCids.filter(
    (cid) => !remainingPiecesCids.has(cid),
  )

  await Promise.all(
    orphanedCids.map((cid) =>
      env.X402_METADATA_KV.delete(`${payerAddress}:${cid}`),
    ),
  )
}
