export const DEFAULT_DELIVERY_LAG_BUFFER_SECONDS = 1800

/** @type {Record<string, number>} */
export const HEALTH_HTTP_STATUS = {
  healthy: 200,
  idle: 200,
  delivery_lagging: 503,
  d1_unhealthy: 503,
  subgraph_unknown: 503,
}

/**
 * Checks whether events indexed by the Goldsky subgraph are being delivered to
 * this worker (via Goldsky pipeline webhooks) and written to D1.
 *
 * The newest PieceAdded event that is older than the buffer period must have a
 * matching row with a CID in the `pieces` table. Piece removals can create
 * tombstones without a CID, while PieceAdded delivery always populates it.
 *
 * Statuses:
 *
 * - `healthy`: the newest eligible on-chain piece is present in D1
 * - `idle`: no on-chain PieceAdded events older than the buffer period exist, so
 *   delivery cannot be verified (nothing is missing either)
 * - `delivery_lagging`: the newest eligible on-chain piece is missing from D1
 * - `d1_unhealthy`: the D1 query failed
 * - `subgraph_unknown`: the subgraph could not be queried or returned an unusable
 *   response, so delivery cannot be verified
 *
 * @param {{
 *   DB: D1Database
 *   GOLDSKY_SUBGRAPH_URL: string
 *   DELIVERY_LAG_BUFFER_SECONDS?: string | number
 * }} env
 * @param {object} [options]
 * @param {typeof globalThis.fetch} [options.fetch]
 * @param {number} [options.now] Current time in milliseconds
 * @returns {Promise<{
 *   status:
 *     | 'healthy'
 *     | 'idle'
 *     | 'delivery_lagging'
 *     | 'd1_unhealthy'
 *     | 'subgraph_unknown'
 *   checked?: { dataSetId: string; pieceId: string; blockTimestamp: string }
 * }>}
 */
export async function checkDeliveryHealth(
  env,
  { fetch = globalThis.fetch, now = Date.now() } = {},
) {
  const bufferSeconds = parseBufferSeconds(env.DELIVERY_LAG_BUFFER_SECONDS)
  const cutoff = Math.floor(now / 1000) - bufferSeconds

  const query = `{
    pieceAddeds(
      first: 1
      orderBy: blockTimestamp
      orderDirection: desc
      where: { blockTimestamp_lte: "${cutoff}" }
    ) {
      dataSetId
      pieceId
      blockTimestamp
    }
  }`

  /**
   * @type {{
   *   dataSetId: string
   *   pieceId: string
   *   blockTimestamp: string
   * }[]}
   */
  let pieceAddeds
  try {
    const res = await fetch(env.GOLDSKY_SUBGRAPH_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query }),
    })
    if (!res.ok) {
      console.warn(`Delivery health: subgraph returned HTTP ${res.status}`)
      return { status: 'subgraph_unknown' }
    }
    const body = await res.json()
    if (
      (Array.isArray(body.errors) && body.errors.length > 0) ||
      !Array.isArray(body.data?.pieceAddeds)
    ) {
      console.warn(
        'Delivery health: unusable subgraph response',
        JSON.stringify(body).slice(0, 500),
      )
      return { status: 'subgraph_unknown' }
    }
    pieceAddeds = body.data.pieceAddeds
  } catch (err) {
    console.warn(`Delivery health: subgraph request failed: ${err}`)
    return { status: 'subgraph_unknown' }
  }

  if (pieceAddeds.length === 0) {
    return { status: 'idle' }
  }

  const [piece] = pieceAddeds
  /** @type {unknown} */
  let row
  try {
    row = await env.DB.prepare(
      `SELECT 1 FROM pieces
       WHERE id = ? AND data_set_id = ? AND cid IS NOT NULL`,
    )
      .bind(piece.pieceId, piece.dataSetId)
      .first()
  } catch (err) {
    console.warn(`Delivery health: D1 query failed: ${err}`)
    return { status: 'd1_unhealthy' }
  }

  const checked = {
    dataSetId: piece.dataSetId,
    pieceId: piece.pieceId,
    blockTimestamp: piece.blockTimestamp,
  }
  if (row) {
    return { status: 'healthy', checked }
  }
  return { status: 'delivery_lagging', checked }
}

/**
 * @param {unknown} value
 * @returns {number}
 */
function parseBufferSeconds(value) {
  const parsed = Number(value)
  if (Number.isFinite(parsed) && parsed > 0) {
    return parsed
  }
  return DEFAULT_DELIVERY_LAG_BUFFER_SECONDS
}
