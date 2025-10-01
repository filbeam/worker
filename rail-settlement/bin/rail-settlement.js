import { getChainClient as defaultGetChainClient } from '../lib/chain.js'
import { abi } from '../lib/filbeam.js'
import { getDataSetsForSettlement } from '../lib/rail-settlement.js'

/**
 * @typedef {{
 *   ENVIRONMENT: 'dev' | 'calibration' | 'mainnet'
 *   RPC_URL: string
 *   FILBEAM_CONTRACT_ADDRESS: string
 *   FILBEAM_CONTROLLER_ADDRESS_PRIVATE_KEY: string
 *   DB: D1Database
 * }} RailSettlementEnv
 */

export default {
  /**
   * @param {any} _controller
   * @param {RailSettlementEnv} env
   * @param {ExecutionContext} _ctx
   * @param {{ getChainClient?: Function }} [options]
   */
  async scheduled(_controller, env, _ctx, options = {}) {
    const { getChainClient = defaultGetChainClient } = options
    console.log('Starting rail settlement worker')

    try {
      // Get chain client for contract interactions
      const { publicClient, walletClient, account } = getChainClient(env)

      // Get current epoch from chain
      const currentEpoch = await publicClient.getBlockNumber()
      console.log(`Current epoch: ${currentEpoch}`)

      // Fetch data sets that need settlement
      const dataSetIds = await getDataSetsForSettlement(env.DB, currentEpoch)

      if (dataSetIds.length === 0) {
        console.log('No active data sets found for settlement')
        return
      }

      console.log(
        `Found ${dataSetIds.length} data sets for settlement:`,
        dataSetIds,
      )

      // Convert data set IDs to BigInt array for the contract call
      const dataSetIdsBigInt = dataSetIds.map((id) => BigInt(id))

      // Simulate the transaction first to check for errors
      const { request } = await publicClient.simulateContract({
        account,
        abi,
        address: env.FILBEAM_CONTRACT_ADDRESS,
        functionName: 'settleCDNPaymentRailBatch',
        args: [dataSetIdsBigInt],
      })

      // Send the actual transaction without waiting for receipt
      const hash = await walletClient.writeContract(request)

      console.log(`Settlement transaction sent: ${hash}`)
      console.log(`Settled ${dataSetIds.length} data sets`)
    } catch (error) {
      console.error('Settlement process failed:', error)

      // Log more details if it's a contract revert
      if (error.cause?.reason) {
        console.error('Contract revert reason:', error.cause.reason)
      }

      throw error
    }
  },
}
