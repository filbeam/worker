import { checkIfAddressIsSanctioned as defaultCheckIfAddressIsSanctioned } from './chainalysis.js'
import { epochToTimestampMs } from './epoch.js'
import { BYTES_PER_TIB } from './constants.js'
import { isEventProcessed, markEventProcessed } from './processed-events.js'

/**
 * Handle proof set rail creation
 *
 * @param {Env} env
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
      ON CONFLICT DO UPDATE SET
        service_provider_id = excluded.service_provider_id,
        payer_address = excluded.payer_address,
        with_ipfs_indexing = excluded.with_ipfs_indexing
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
 * @param {Env} env
 * @param {any} payload
 * @throws {Error}
 */
export async function handleFWSSServiceTerminated(env, payload) {
  const epochTimestampMs = epochToTimestampMs(
    payload.block_number,
    Number(env.FILECOIN_GENESIS_BLOCK_TIMESTAMP_MS),
  )

  // Calculate lockup unlock timestamp based on the epoch timestamp (in milliseconds)
  const lockupUnlocksAtMs =
    epochTimestampMs +
    Number(env.DEFAULT_LOCKUP_PERIOD_DAYS) * 24 * 60 * 60 * 1000
  const lockupUnlocksAt = new Date(lockupUnlocksAtMs)
  const lockupUnlocksAtISO = lockupUnlocksAt.toISOString()

  await env.DB.prepare(
    `
      INSERT INTO data_sets (
        id, with_cdn, lockup_unlocks_at
      ) VALUES (?, FALSE, datetime(?))
      ON CONFLICT DO UPDATE SET
        with_cdn = excluded.with_cdn,
        lockup_unlocks_at = excluded.lockup_unlocks_at
    `,
  )
    .bind(payload.data_set_id, lockupUnlocksAtISO)
    .run()
}

/**
 * Handle CDN Payment Rails Topped Up event Calculates and updates egress quotas
 * for a data set based on lockup amounts
 *
 * @param {Env} env
 * @param {object} payload
 * @param {string} payload.id - Unique entity ID from subgraph (txHash +
 *   logIndex)
 * @param {string} payload.data_set_id
 * @param {string} payload.cdn_amount_added
 * @param {string} payload.cache_miss_amount_added
 * @throws {Error} If there is an error during the database operation
 */
export async function handleFWSSCDNPaymentRailsToppedUp(env, payload) {
  const { CDN_RATE_PER_TIB, CACHE_MISS_RATE_PER_TIB, PROCESSED_EVENTS } = env

  if (await isEventProcessed(PROCESSED_EVENTS, 'cdn_top_up', payload.id)) {
    console.log(`Event already processed. Entity id: ${payload.id}`)
    return
  }

  const cdnEgressQuotaAdded =
    (BigInt(payload.cdn_amount_added) * BYTES_PER_TIB) /
    BigInt(CDN_RATE_PER_TIB)

  const cacheMissEgressQuotaAdded =
    (BigInt(payload.cache_miss_amount_added) * BYTES_PER_TIB) /
    BigInt(CACHE_MISS_RATE_PER_TIB)

  await env.DB.prepare(
    `
    INSERT INTO data_set_egress_quotas (
      data_set_id,
      cdn_egress_quota,
      cache_miss_egress_quota
    )
    VALUES (?, CAST(? AS INTEGER), CAST(? AS INTEGER))
    ON CONFLICT (data_set_id) DO UPDATE SET
      cdn_egress_quota = data_set_egress_quotas.cdn_egress_quota + excluded.cdn_egress_quota,
      cache_miss_egress_quota = data_set_egress_quotas.cache_miss_egress_quota + excluded.cache_miss_egress_quota
    `,
  )
    .bind(
      payload.data_set_id,
      cdnEgressQuotaAdded.toString(),
      cacheMissEgressQuotaAdded.toString(),
    )
    .run()

  // Mark as processed after successful DB operation
  await markEventProcessed(PROCESSED_EVENTS, 'cdn_top_up', payload.id)
}
