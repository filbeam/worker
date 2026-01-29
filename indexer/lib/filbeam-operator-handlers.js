import { epochToTimestampMs } from './epoch.js'

/**
 * @param {Env} env
 * @param {{ data_set_id: string; block_number: number }} payload
 */
export async function handleCdnPaymentSettled(env, payload) {
  const timestampMs = epochToTimestampMs(
    payload.block_number,
    Number(env.FILECOIN_GENESIS_BLOCK_TIMESTAMP_MS),
  )
  const timestampISO = new Date(timestampMs).toISOString()

  await env.DB.prepare(
    `INSERT INTO data_sets (id, with_cdn, payments_settled_until)
     VALUES (?, TRUE, ?)
     ON CONFLICT (id) DO UPDATE
     SET payments_settled_until = MAX(data_sets.payments_settled_until, excluded.payments_settled_until)`,
  )
    .bind(payload.data_set_id, timestampISO)
    .run()
}
