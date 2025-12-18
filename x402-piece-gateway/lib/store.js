/**
 * @param {Env} env
 * @param {string} pieceCid
 * @param {string} payerAddress
 * @returns {Promise<{ price: string; is_sanctioned: boolean } | null>}
 */
export async function getPieceX402Metadata(env, pieceCid, payerAddress) {
  return await env.DB.prepare(
    `SELECT
        MAX(pieces.x402_price) price,
        wallet_details.is_sanctioned
      FROM pieces
      LEFT JOIN data_sets ON pieces.data_set_id = data_sets.id
      LEFT JOIN wallet_details ON data_sets.payer_address = wallet_details.address
      WHERE
        pieces.cid = ? AND
        pieces.is_deleted IS FALSE AND
        data_sets.payer_address = ?`,
  )
    .bind(pieceCid, payerAddress)
    .first()
}
