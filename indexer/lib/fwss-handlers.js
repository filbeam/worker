import { checkIfAddressIsSanctioned as defaultCheckIfAddressIsSanctioned } from './chainalysis.js'
import { epochToTimestamp } from './epoch.js'

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
 * @param {{
 *   DEFAULT_LOCKUP_PERIOD_DAYS?: number
 *   GENESIS_BLOCK_TIMESTAMP?: number
 *   DB: D1Database
 * }} env
 * @param {object} payload
 * @param {string} payload.data_set_id
 * @param {string} payload.block_number
 * @throws {Error}
 */
export async function handleFWSSServiceTerminated(env, payload) {
  const DEFAULT_LOCKUP_PERIOD_DAYS = env.DEFAULT_LOCKUP_PERIOD_DAYS || 30 // 30 days default
  const GENESIS_BLOCK_TIMESTAMP = env.GENESIS_BLOCK_TIMESTAMP || 1667326380 // Calibration genesis as fallback

  // Convert block_number (epoch) to Unix timestamp (in seconds)
  const epochTimestamp = epochToTimestamp(
    payload.block_number,
    GENESIS_BLOCK_TIMESTAMP,
  )

  // Calculate lockup unlock timestamp based on the epoch timestamp
  const lockupUnlocksAtUnix =
    epochTimestamp + DEFAULT_LOCKUP_PERIOD_DAYS * 24 * 60 * 60
  const lockupUnlocksAt = new Date(lockupUnlocksAtUnix * 1000)
  const lockupUnlocksAtISO = lockupUnlocksAt.toISOString()

  await env.DB.prepare(
    `
      UPDATE data_sets
      SET with_cdn = false,
          lockup_unlocks_at = datetime(?)
      WHERE id = ?
    `,
  )
    .bind(lockupUnlocksAtISO, payload.data_set_id)
    .run()
}
