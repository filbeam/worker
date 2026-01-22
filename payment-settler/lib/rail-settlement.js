/** @import {PublicClient, WalletClient, PrivateKeyAccount} from 'viem' */
import filbeamAbi from '../lib/FilBeamOperator.abi.json'

/**
 * Fetches data sets that need CDN payment rail settlement
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
 * @param {object} args
 * @param {Env} args.env
 * @param {PublicClient} args.publicClient
 * @param {WalletClient} args.walletClient
 * @param {PrivateKeyAccount} args.account
 * @param {string[]} args.dataSetIds
 * @returns {Promise<`0x${string}`>}
 */
export async function settleCDNPaymentRails({
  env,
  publicClient,
  walletClient,
  account,
  dataSetIds,
}) {
  const contractParams = {
    account,
    abi: filbeamAbi,
    address: env.FILBEAM_OPERATOR_CONTRACT_ADDRESS,
    functionName: 'settleCDNPaymentRails',
    args: [dataSetIds.map((id) => BigInt(id))],
  }

  const estimatedGas = await publicClient.estimateContractGas(contractParams)
  const gasLimit = (estimatedGas * 120n) / 100n

  const { request } = await publicClient.simulateContract({
    ...contractParams,
    gas: gasLimit,
  })

  return await walletClient.writeContract(request)
}
