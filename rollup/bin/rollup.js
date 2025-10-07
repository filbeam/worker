import { getChainClient as defaultGetChainClient } from '../lib/chain.js'
import { abi } from '../lib/filbeam.js'
import { aggregateUsageData, prepareBatchData } from '../lib/rollup.js'

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

      // Aggregate usage data for all datasets that need reporting
      const allUsageData = await aggregateUsageData(
        env.DB,
        Number(targetEpoch),
        Number(env.GENESIS_BLOCK_TIMESTAMP),
      )

      if (allUsageData.length === 0) {
        console.log('No usage data to report')
        return
      }

      console.log(`Found usage data for ${allUsageData.length} datasets`)

      // Prepare batch data for contract call
      const batchData = prepareBatchData(allUsageData)

      if (batchData.dataSetIds.length === 0) {
        console.log('No datasets with non-zero usage to report')
        return
      }

      console.log(`Reporting usage for ${batchData.dataSetIds.length} datasets`)

      const { request } = await publicClient.simulateContract({
        address: env.FILBEAM_CONTRACT_ADDRESS,
        abi,
        functionName: 'recordUsageRollups',
        args: [
          batchData.dataSetIds,
          batchData.epochs,
          batchData.cdnBytesUsed,
          batchData.cacheMissBytesUsed,
        ],
        account,
      })

      const hash = await walletClient.writeContract(request)
      console.log(`Transaction submitted: ${hash}`)
    } catch (error) {
      console.error('Error in rollup worker:', error)
      throw error
    }
  },
}
