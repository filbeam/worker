/**
 * @param {Env} env
 * @param {{ data_set_id: string; to_epoch: string }} payload
 */
export async function handleUsageReported(env, payload) {
  const toEpoch = Number(payload.to_epoch)
  await env.DB.prepare(
    `INSERT INTO data_sets_settlements (data_set_id, usage_reported_until)
     VALUES (?, ?)
     ON CONFLICT (data_set_id) DO UPDATE
     SET usage_reported_until = MAX(data_sets_settlements.usage_reported_until, excluded.usage_reported_until)`,
  )
    .bind(payload.data_set_id, toEpoch)
    .run()
}

/**
 * @param {Env} env
 * @param {{ data_set_id: string; block_number: number }} payload
 */
export async function handleCdnPaymentSettled(env, payload) {
  await env.DB.prepare(
    `INSERT INTO data_sets_settlements (data_set_id, payments_settled_until)
     VALUES (?, ?)
     ON CONFLICT (data_set_id) DO UPDATE
     SET payments_settled_until = MAX(data_sets_settlements.payments_settled_until, excluded.payments_settled_until)`,
  )
    .bind(payload.data_set_id, payload.block_number)
    .run()
}
