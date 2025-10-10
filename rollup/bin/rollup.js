import { getChainClient as defaultGetChainClient } from '../lib/chain.js'
import { abi } from '../lib/filbeam.js'
import {
  aggregateUsageData,
  prepareUsageRollupData,
  epochToTimestamp,
} from '../lib/rollup.js'

/**
 * @typedef {{
 *   ENVIRONMENT: 'dev' | 'calibration' | 'mainnet'
 *   RPC_URL: string
 *   FILBEAM_CONTRACT_ADDRESS: string
 *   FILBEAM_CONTROLLER_PRIVATE_KEY: string
 *   GENESIS_BLOCK_TIMESTAMP: string
 *   DB: D1Database
 * }} RollupEnv
 */

export default {
  /**
   * @param {any} _controller
   * @param {RollupEnv} env
   * @param {ExecutionContext} _ctx
   * @param {{ getChainClient?: typeof defaultGetChainClient }} [options]
   */
  async scheduled(
    _controller,
    env,
    _ctx,
    { getChainClient = defaultGetChainClient } = {},
  ) {
    console.log('Starting rollup worker scheduled run')

    try {
      // Get chain client and current epoch from chain
      const { publicClient, walletClient, account } = getChainClient(env)
      const currentEpoch = await publicClient.getBlockNumber()
      const targetEpoch = currentEpoch - 1n // Report up to previous epoch
      console.log(
        `Current epoch: ${currentEpoch}, reporting up to epoch: ${targetEpoch}`,
      )

      // Convert target epoch to ISO timestamp for SQL query
      const upToTimestamp = epochToTimestamp(
        targetEpoch,
        BigInt(env.GENESIS_BLOCK_TIMESTAMP),
      )
      console.log(`Aggregating usage data up to timestamp: ${upToTimestamp}`)

      // Aggregate usage data for all datasets that need reporting
      const usageData = await aggregateUsageData(
        env.DB,
        upToTimestamp,
        BigInt(env.GENESIS_BLOCK_TIMESTAMP),
      )

      if (usageData.length === 0) {
        console.log('No usage data found')
        return
      }

      console.log(`Found usage data for ${usageData.length} data sets`)

      // Prepare usage rollup data for contract call
      const usageRollupData = prepareUsageRollupData(usageData)

      console.log(
        `Reporting usage for ${usageRollupData.dataSetIds.length} data sets`,
      )

      const { request } = await publicClient.simulateContract({
        address: env.FILBEAM_CONTRACT_ADDRESS,
        abi,
        functionName: 'recordUsageRollups',
        args: [
          usageRollupData.dataSetIds,
          usageRollupData.maxEpochs,
          usageRollupData.cdnBytesUsed,
          usageRollupData.cacheMissBytesUsed,
        ],
        account,
      })

      const hash = await walletClient.writeContract(request)
      console.log(`Transaction submitted: ${hash}`)

      // Store transaction hash to prevent double-counting
      // Multiple datasets might share the same transaction, so update all of them
      await env.DB.batch(
        usageRollupData.dataSetIds.map((dataSetId) =>
          env.DB.prepare(
            `UPDATE data_sets SET pending_rollup_tx_hash = ? WHERE id = ?`,
          ).bind(hash, dataSetId),
        ),
      )
      console.log(
        `Stored pending transaction hash for ${usageRollupData.dataSetIds.length} datasets`,
      )

      // Note: The transaction will be confirmed and usage_reported_until will be updated
      // by a separate webhook or indexer process when the transaction is mined
    } catch (error) {
      console.error('Error in rollup worker:', error)
      throw error
    }
  },
}
