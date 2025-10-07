import { checkIfAddressIsSanctioned as defaultCheckIfAddressIsSanctioned } from './chainalysis.js'
import { calculateEgressQuota, formatUsdfcAmount } from './rate-helpers.js'

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
 *   CDN_RATE_DOLLARS_PER_TIB: string
 *   CACHE_MISS_RATE_DOLLARS_PER_TIB: string
 *   DB: D1Database
 * }} env
 * @param {any} payload
 * @throws {Error} If there is an error during the database operation
 */
export async function handleFWSSCDNPaymentRailsToppedUp(env, payload) {
  const { CDN_RATE_DOLLARS_PER_TIB, CACHE_MISS_RATE_DOLLARS_PER_TIB } = env

  const dataSetId = String(payload.data_set_id)
  const totalCdnLockup = payload.total_cdn_lockup || '0'
  const totalCacheMissLockup = payload.total_cache_miss_lockup || '0'

  // Convert dollar rates to USDFC units
  const cdnRatePerTiB = formatUsdfcAmount(CDN_RATE_DOLLARS_PER_TIB)
  const cacheMissRatePerTiB = formatUsdfcAmount(CACHE_MISS_RATE_DOLLARS_PER_TIB)

  // Calculate quotas in bytes using the helper function
  // This handles the conversion from rate per TiB to actual quota
  const cdnEgressQuota = calculateEgressQuota(totalCdnLockup, cdnRatePerTiB)
  const cacheMissEgressQuota = calculateEgressQuota(
    totalCacheMissLockup,
    cacheMissRatePerTiB,
  )

  // Store as strings since quotas can be very large uint256 values
  await env.DB.prepare(
    `
    UPDATE data_sets
    SET cdn_egress_quota = ?,
        cache_miss_egress_quota = ?
    WHERE id = ?
    `,
  )
    .bind(cdnEgressQuota.toString(), cacheMissEgressQuota.toString(), dataSetId)
    .run()

  console.log(
    `Updated egress quotas for data_set ${dataSetId}: ` +
      `cdn_quota=${cdnEgressQuota} bytes, cache_miss_quota=${cacheMissEgressQuota} bytes`,
  )
}
