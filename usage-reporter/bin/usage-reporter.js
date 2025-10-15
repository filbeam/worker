/** @import {MessageBatch} from 'cloudflare:workers' */
import { getChainClient as defaultGetChainClient } from '../lib/chain.js'
import filbeamAbi from '../lib/filbeam.abi.js'
import {
  aggregateUsageData,
  epochToTimestamp,
  prepareUsageReportData,
} from '../lib/usage-report.js'
import { TransactionMonitorWorkflow } from '@filbeam/workflows'
import {
  handleTransactionRetryQueueMessage as defaultHandleTransactionRetryQueueMessage,
  handleTransactionConfirmedQueueMessage as defaultHandleTransactionConfirmedQueueMessage,
} from '../lib/queue-handlers.js'

/**
 * @typedef {{
 *   type: 'transaction-retry'
 *   transactionHash: string
 *   upToTimestamp: string
 * }} TransactionRetryMessage
 */

/**
 * @typedef {{
 *   type: 'transaction-confirmed'
 *   transactionHash: string
 *   upToTimestamp: string
 * }} TransactionConfirmedMessage
 */

/**
 * @typedef {{
 *   ENVIRONMENT: 'dev' | 'calibration' | 'mainnet'
 *   RPC_URL: string
 *   FILBEAM_CONTRACT_ADDRESS: string
 *   FILBEAM_CONTROLLER_PRIVATE_KEY: string
 *   GENESIS_BLOCK_TIMESTAMP: string
 *   DB: D1Database
 *   TRANSACTION_MONITOR_WORKFLOW: import('cloudflare:workers').WorkflowEntrypoint
 *   TRANSACTION_QUEUE: import('cloudflare:workers').Queue<
 *     TransactionRetryMessage | TransactionConfirmedMessage
 *   >
 * }} UsageReporterEnv
 */

export default {
  /**
   * @param {any} _controller
   * @param {UsageReporterEnv} env
   * @param {ExecutionContext} _ctx
   * @param {{ getChainClient?: typeof defaultGetChainClient }} [options]
   */
  async scheduled(
    _controller,
    env,
    _ctx,
    { getChainClient = defaultGetChainClient } = {},
  ) {
    console.log('Starting usage reporter worker scheduled run')

    try {
      const { publicClient, walletClient, account } = getChainClient(env)
      const currentEpoch = await publicClient.getBlockNumber()
      const targetEpoch = currentEpoch - 1n // Report up to previous epoch
      console.log(
        `Current epoch: ${currentEpoch}, reporting up to epoch: ${targetEpoch}`,
      )

      const upToTimestamp = epochToTimestamp(
        targetEpoch,
        BigInt(env.GENESIS_BLOCK_TIMESTAMP),
      )
      console.log(`Aggregating usage data up to timestamp: ${upToTimestamp}`)

      // Aggregate usage data for all datasets that need reporting
      const usageData = await aggregateUsageData(env.DB, upToTimestamp)

      if (usageData.length === 0) {
        console.log('No usage data found')
        return
      }

      console.log(`Found usage data for ${usageData.length} data sets`)

      // Prepare usage report data for contract call
      const usageReportData = prepareUsageReportData(
        usageData,
        BigInt(env.GENESIS_BLOCK_TIMESTAMP),
      )

      console.log(
        `Reporting usage for ${usageReportData.dataSetIds.length} data sets`,
      )

      // Create contract call
      const { request } = await publicClient.simulateContract({
        account,
        address: env.FILBEAM_CONTRACT_ADDRESS,
        abi: filbeamAbi,
        functionName: 'recordUsageRollups',
        args: [
          usageReportData.dataSetIds,
          usageReportData.maxEpochs,
          usageReportData.cdnBytesUsed,
          usageReportData.cacheMissBytesUsed,
        ],
      })

      console.log(
        `Sending recordUsageRollups transaction for ${usageReportData.dataSetIds.length} data sets`,
      )

      // Send transaction
      const hash = await walletClient.writeContract(request)

      console.log(`Transaction sent: ${hash}`)

      // Store transaction hash to prevent double-counting
      await env.DB.batch(
        usageReportData.dataSetIds.map((dataSetId) =>
          env.DB.prepare(
            `UPDATE data_sets SET pending_usage_report_tx_hash = ? WHERE id = ?`,
          ).bind(hash, dataSetId),
        ),
      )

      console.log(
        `Stored pending transaction hash for ${usageReportData.dataSetIds.length} data sets`,
      )

      // Start transaction monitor workflow
      await env.TRANSACTION_MONITOR_WORKFLOW.create({
        id: `usage-report-tx-monitor-${hash}-${Date.now()}`,
        params: {
          transactionHash: hash,
          metadata: {
            onSuccess: 'transaction-confirmed',
            successData: { upToTimestamp },
            retryData: { upToTimestamp },
          },
        },
      })

      console.log(
        `Started transaction monitor workflow for transaction: ${hash}`,
      )
    } catch (error) {
      console.error('Error in usage reporter worker:', error)
      throw error
    }
  },

  /**
   * Queue consumer for transaction-related messages
   *
   * @param {MessageBatch<
   *   TransactionRetryMessage | TransactionConfirmedMessage
   * >} batch
   * @param {UsageReporterEnv} env
   * @param {ExecutionContext} ctx
   */
  async queue(
    batch,
    env,
    ctx,
    {
      handleTransactionRetryQueueMessage = defaultHandleTransactionRetryQueueMessage,
      handleTransactionConfirmedQueueMessage = defaultHandleTransactionConfirmedQueueMessage,
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
          case 'transaction-confirmed':
            await handleTransactionConfirmedQueueMessage(message.body, env)
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
