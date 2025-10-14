/** @import {MessageBatch} from 'cloudflare:workers' */
import { getChainClient as defaultGetChainClient } from '../lib/chain.js'
import { abi } from '../lib/filbeam.js'
import { getDataSetsForSettlement } from '../lib/rail-settlement.js'
import { TransactionMonitorWorkflow } from '@filbeam/workflows'
import { handleTransactionRetryQueueMessage as defaultHandleTransactionRetryQueueMessage } from '../lib/queue-handlers.js'

/**
 * @typedef {{
 *   type: 'transaction-retry'
 *   transactionHash: string
 * }} TransactionRetryMessage
 */

/**
 * @typedef {{
 *   ENVIRONMENT: 'dev' | 'calibration' | 'mainnet'
 *   RPC_URL: string
 *   FILBEAM_CONTRACT_ADDRESS: string
 *   FILBEAM_CONTROLLER_ADDRESS_PRIVATE_KEY: string
 *   DB: D1Database
 *   TRANSACTION_MONITOR_WORKFLOW: import('cloudflare:workers').WorkflowEntrypoint
 *   TRANSACTION_QUEUE: import('cloudflare:workers').Queue<TransactionRetryMessage>
 * }} RailSettlementEnv
 */

export default {
  /**
   * @param {any} _controller
   * @param {RailSettlementEnv} env
   * @param {ExecutionContext} _ctx
   * @param {{ getChainClient?: Function }} [options]
   */
  async scheduled(
    _controller,
    env,
    _ctx,
    { getChainClient = defaultGetChainClient } = {},
  ) {
    console.log('Starting rail settlement worker')

    try {
      const { publicClient, walletClient, account } = getChainClient(env)

      const dataSetIds = await getDataSetsForSettlement(env.DB)

      if (dataSetIds.length === 0) {
        console.log('No active data sets found for settlement')
        return
      }

      console.log(
        `Found ${dataSetIds.length} data sets for settlement:`,
        dataSetIds,
      )

      const { request } = await publicClient.simulateContract({
        account,
        abi,
        address: env.FILBEAM_CONTRACT_ADDRESS,
        functionName: 'settleCDNPaymentRailBatch',
        args: dataSetIds.map((id) => BigInt(id)),
      })

      const hash = await walletClient.writeContract(request)

      console.log(`Settlement transaction sent: ${hash}`)

      // Start transaction monitor workflow
      await env.TRANSACTION_MONITOR_WORKFLOW.create({
        id: `settlement-tx-monitor-${hash}-${Date.now()}`,
        params: {
          transactionHash: hash,
          metadata: {
            // Only retry data, no success handling needed since we don't store in DB
            retryData: {},
          },
        },
      })

      console.log(
        `Started transaction monitor workflow for settlement transaction: ${hash}`,
      )
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

  /**
   * Queue consumer for transaction-related messages
   *
   * @param {MessageBatch<TransactionRetryMessage>} batch
   * @param {RailSettlementEnv} env
   * @param {ExecutionContext} ctx
   */
  async queue(
    batch,
    env,
    ctx,
    {
      handleTransactionRetryQueueMessage = defaultHandleTransactionRetryQueueMessage,
    } = {},
  ) {
    for (const message of batch.messages) {
      console.log(
        `Processing transaction queue message of type: ${message.body.type}`,
      )
      try {
        switch (message.body.type) {
          case 'transaction-retry':
            await handleTransactionRetryQueueMessage(message.body, env)
            break
          default:
            throw new Error(`Unknown message type: ${message.body.type}`)
        }
        message.ack()
      } catch (error) {
        console.error(`Failed to process queue message, retrying:`, error)
        message.retry()
      }
    }
  },
}

// Cloudflare worker runtime requires that you export workflows from the entrypoint file
export { TransactionMonitorWorkflow }
