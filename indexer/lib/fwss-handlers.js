import { checkIfAddressIsSanctioned as defaultCheckIfAddressIsSanctioned } from './chainalysis.js'
import { BYTES_PER_TIB } from './constants.js'

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
 * @param {Env} env
 * @param {any} payload
 * @throws {Error}
 */
export async function handleFWSSServiceTerminated(env, payload) {
  await env.DB.prepare(
    `
      UPDATE data_sets
      SET with_cdn = false
      WHERE id = ?
    `,
  )
    .bind(String(payload.data_set_id))
    .run()
}

/**
 * Handle CDN Payment Rails Topped Up event Calculates and updates egress quotas
 * for a data set based on lockup amounts
 *
 * @param {{
 *   CDN_RATE_PER_TIB: string
 *   CACHE_MISS_RATE_PER_TIB: string
 *   DB: D1Database
 * }} env
 * @param {object} payload
 * @param {string} payload.data_set_id
 * @param {string} payload.cdn_amount_added
 * @param {string} payload.cache_miss_amount_added
 * @throws {Error} If there is an error during the database operation
 */
export async function handleFWSSCDNPaymentRailsToppedUp(env, payload) {
  const { CDN_RATE_PER_TIB, CACHE_MISS_RATE_PER_TIB } = env

  /**
   * @type {{
   *   cdn_egress_quota: string
   *   cache_miss_egress_quota: string
   * } | null}
   */
  const currentDataSet = await env.DB.prepare(
    `SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?`,
  )
    .bind(payload.data_set_id)
    .first()

  if (!currentDataSet) {
    return new Response(`Data set ${payload.data_set_id} not found`, {
      status: 404,
    })
  }

  const currentCdnQuota = BigInt(currentDataSet.cdn_egress_quota)
  const currentCacheMissQuota = BigInt(currentDataSet.cache_miss_egress_quota)
  const cdnEgressQuotaAdded =
    (BigInt(payload.cdn_amount_added) * BYTES_PER_TIB) /
    BigInt(CDN_RATE_PER_TIB)

  const cacheMissEgressQuotaAdded =
    (BigInt(payload.cache_miss_amount_added) * BYTES_PER_TIB) /
    BigInt(CACHE_MISS_RATE_PER_TIB)

  const newCdnQuota = currentCdnQuota + cdnEgressQuotaAdded
  const newCacheMissQuota = currentCacheMissQuota + cacheMissEgressQuotaAdded

  await env.DB.prepare(
    `
    UPDATE data_sets
    SET cdn_egress_quota = ?,
        cache_miss_egress_quota = ?
    WHERE id = ?
    `,
  )
    .bind(
      newCdnQuota.toString(),
      newCacheMissQuota.toString(),
      payload.data_set_id,
    )
    .run()
}
