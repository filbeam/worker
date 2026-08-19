/** @import {PublicClient, WalletClient, PrivateKeyAccount} from 'viem' */
import filbeamAbi from '../lib/FilBeamOperator.abi.json'

/**
 * Fetches data sets that need cache-miss payment rail settlement.
 *
 * Only settle data sets with recently reported usage (within last 30 days).
 * This prevents unnecessary settlement attempts for inactive or abandoned data
 * sets that are no longer generating egress traffic. If a data set hasn't
 * reported usage in 30 days, it's likely inactive and doesn't need settlement
 * processing. Reference:
 * https://github.com/filbeam/worker/pull/324#discussion_r2416210457
 *
 * @param {D1Database} db - The database connection
 * @returns {Promise<string[]>} Array of data set IDs that need settlement
 */
export async function getDataSetsForSettlement(db) {
  const result = await db
    .prepare(
      `
      SELECT data_sets.id
      FROM data_sets
      LEFT JOIN wallet_details ON data_sets.payer_address = wallet_details.address
      WHERE (data_sets.with_cdn = 1 OR data_sets.lockup_unlocks_at >= datetime('now'))
        AND data_sets.terminate_service_tx_hash IS NULL
        AND data_sets.usage_reported_until >= datetime('now', '-30 days')
        AND (wallet_details.is_sanctioned IS NULL OR wallet_details.is_sanctioned = 0)
      `,
    )
    .all()

  return result.results.map((row) => String(row.id))
}

/**
 * Fetches the distinct shared bandwidth rails that need settlement.
 *
 * A rail is eligible when at least one of its data sets is eligible for
 * settlement (same criteria as {@link getDataSetsForSettlement}). Because data
 * sets in a CDN group share one cdn_rail_id, the bandwidth is settled once per
 * rail rather than once per data set.
 *
 * @param {D1Database} db - The database connection
 * @returns {Promise<string[]>} Array of cdn_rail_id values that need settlement
 */
export async function getCDNRailsForSettlement(db) {
  const result = await db
    .prepare(
      `
      SELECT DISTINCT data_sets.cdn_rail_id
      FROM data_sets
      LEFT JOIN wallet_details ON data_sets.payer_address = wallet_details.address
      WHERE (data_sets.with_cdn = 1 OR data_sets.lockup_unlocks_at >= datetime('now'))
        AND data_sets.terminate_service_tx_hash IS NULL
        AND data_sets.usage_reported_until >= datetime('now', '-30 days')
        AND data_sets.cdn_rail_id IS NOT NULL
        AND (wallet_details.is_sanctioned IS NULL OR wallet_details.is_sanctioned = 0)
      `,
    )
    .all()

  return result.results.map((row) => String(row.cdn_rail_id))
}

/**
 * Settles cache-miss payment rails for a batch of data sets.
 *
 * @param {object} args
 * @param {Env} args.env
 * @param {string} args.batchId
 * @param {PublicClient} args.publicClient
 * @param {WalletClient} args.walletClient
 * @param {PrivateKeyAccount} args.account
 * @param {string[]} args.dataSetIds
 * @returns {Promise<`0x${string}`>}
 */
export async function settleCacheMissPaymentRails({
  env,
  batchId,
  publicClient,
  walletClient,
  account,
  dataSetIds,
}) {
  console.log(
    `[${batchId}] Settling cache-miss for ${dataSetIds.length} data sets...`,
  )

  const { request } = await publicClient.simulateContract({
    account,
    abi: filbeamAbi,
    address: env.FILBEAM_OPERATOR_CONTRACT_ADDRESS,
    functionName: 'settleCacheMissPaymentRails',
    args: [dataSetIds.map((id) => BigInt(id))],
  })

  const txHash = await walletClient.writeContract(request)
  console.log(`[${batchId}] Cache-miss settlement transaction sent: ${txHash}`)
  return txHash
}

/**
 * Settles the shared bandwidth rails for a batch of cdn_rail_ids.
 *
 * @param {object} args
 * @param {Env} args.env
 * @param {string} args.batchId
 * @param {PublicClient} args.publicClient
 * @param {WalletClient} args.walletClient
 * @param {PrivateKeyAccount} args.account
 * @param {string[]} args.cdnRailIds
 * @returns {Promise<`0x${string}`>}
 */
export async function settleCDNBandwidthRails({
  env,
  batchId,
  publicClient,
  walletClient,
  account,
  cdnRailIds,
}) {
  console.log(
    `[${batchId}] Settling bandwidth for ${cdnRailIds.length} rails...`,
  )

  const { request } = await publicClient.simulateContract({
    account,
    abi: filbeamAbi,
    address: env.FILBEAM_OPERATOR_CONTRACT_ADDRESS,
    functionName: 'settleCDNBandwidthRails',
    args: [cdnRailIds.map((id) => BigInt(id))],
  })

  const txHash = await walletClient.writeContract(request)
  console.log(`[${batchId}] Bandwidth settlement transaction sent: ${txHash}`)
  return txHash
}
