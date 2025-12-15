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

  // Query payer_address from data_sets table
  const dataSet = await env.DB.prepare(
    'SELECT payer_address FROM data_sets WHERE id = ?',
  )
    .bind(dataSetId)
    .first()
  const payerAddress = /** @type {string | null} */ (dataSet?.payer_address)

  if (!payerAddress) return

  const key = `${payerAddress}:${pieceCid}`
  const existing = /** @type {{ price: string; block: number } | null} */ (
    await env.X402_METADATA_KV.get(key, 'json')
  )

  // Only update if new block > existing block
  if (!existing || blockNumber > existing.block) {
    await env.X402_METADATA_KV.put(
      key,
      JSON.stringify({
        price: x402Price,
        block: blockNumber,
      }),
    )
  }
}

/**
 * @param {{ DB: D1Database; X402_METADATA_KV: KVNamespace }} env
 * @param {number | string} dataSetId
 * @param {(number | string)[]} pieceIds
 */
export async function removeDataSetPieces(env, dataSetId, pieceIds) {
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

  const dataSet = await env.DB.prepare(
    'SELECT payer_address FROM data_sets WHERE id = ?',
  )
    .bind(dataSetId)
    .first()
  const payerAddress = /** @type {string | null} */ (dataSet?.payer_address)

  if (!payerAddress) return

  // For each piece, check if any non-deleted copies remain and delete KV if not
  for (const piece of deletedPieces.results) {
    const cid = /** @type {string | null} */ (piece.cid)

    if (!cid) {
      continue
    }

    const remaining = await env.DB.prepare(
      `
        SELECT COUNT(*) as count FROM pieces p
        JOIN data_sets d ON p.data_set_id = d.id
        WHERE p.cid = ? AND d.payer_address = ? AND p.is_deleted = FALSE
        `,
    )
      .bind(cid, payerAddress)
      .first()

    if (remaining && /** @type {number} */ (remaining.count) === 0) {
      await env.X402_METADATA_KV.delete(`${payerAddress}:${cid}`)
    }
  }
}
