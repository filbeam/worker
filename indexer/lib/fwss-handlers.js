import { checkIfAddressIsSanctioned as defaultCheckIfAddressIsSanctioned } from './chainalysis.js'

/**
 * Handle proof set rail creation
 *
 * @param {{ CHAINALYSIS_API_KEY: string; DB: D1Database }} env
 * @param {any} payload
 * @param {object} opts
 * @param {typeof defaultCheckIfAddressIsSanctioned} opts.checkIfAddressIsSanctioned
 * @throws {Error} If there is an error with fetching payer's address sanction
 *   status or during the database operation
 */
export async function handleFWSSDataSetCreated(
  env,
  payload,
  { checkIfAddressIsSanctioned = defaultCheckIfAddressIsSanctioned },
) {
  const { CHAINALYSIS_API_KEY } = env

  const withCDN = payload.metadata_keys.includes('withCDN')
  const withIPFSIndexing = payload.metadata_keys.includes('withIPFSIndexing')

  if (withCDN) {
    const isPayerSanctioned = await checkIfAddressIsSanctioned(payload.payer, {
      CHAINALYSIS_API_KEY,
    })

    await env.DB.prepare(
      `
      INSERT INTO wallet_details (address, is_sanctioned, last_screened_at)
      VALUES (?, ?, datetime('now'))
      ON CONFLICT (address) DO UPDATE SET
        is_sanctioned = excluded.is_sanctioned,
        last_screened_at = excluded.last_screened_at
      `,
    )
      .bind(payload.payer.toLowerCase(), isPayerSanctioned)
      .run()
  }

  await env.DB.prepare(
    `
      INSERT INTO data_sets (
        id,
        service_provider_id,
        payer_address,
        with_cdn,
        with_ipfs_indexing
      )
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT DO NOTHING
    `,
  )
    .bind(
      String(payload.data_set_id),
      String(payload.provider_id),
      payload.payer.toLowerCase(),
      withCDN,
      withIPFSIndexing,
    )
    .run()
}

/**
 * Handle Filecoin Warm Storage Service service termination
 *
 * @param {{ DEFAULT_LOCKUP_PERIOD?: number; DB: D1Database }} env
 * @param {any} payload
 * @param {bigint} currentEpoch - The current block height (epoch)
 * @throws {Error}
 */
export async function handleFWSSServiceTerminated(env, payload, currentEpoch) {
  const DEFAULT_LOCKUP_PERIOD = BigInt(env.DEFAULT_LOCKUP_PERIOD || 86400) // 30 days in epochs
  const settleUpToEpoch = currentEpoch + DEFAULT_LOCKUP_PERIOD

  await env.DB.prepare(
    `
      UPDATE data_sets
      SET with_cdn = false,
          lockup_unlocks_at_epoch = ?
      WHERE id = ?
    `,
  )
    .bind(String(settleUpToEpoch), String(payload.data_set_id))
    .run()
}
