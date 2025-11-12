import { getChainClient as defaultGetChainClient } from '../lib/chain.js'
import filbeamAbi from '../lib/FilBeamOperator.abi.json'
import {
  aggregateUsageData,
  prepareUsageReportData,
} from '../lib/usage-report.js'
import { epochToTimestampMs } from '../lib/epoch.js'
import { TransactionMonitorWorkflow } from '@filbeam/workflows'
import {
  handleTransactionRetryQueueMessage as defaultHandleTransactionRetryQueueMessage,
  handleTransactionConfirmedQueueMessage as defaultHandleTransactionConfirmedQueueMessage,
} from '../lib/queue-handlers.js'
import { calculateTotalBytes } from '../lib/analytics.js'

/**
 * @typedef {{
 *   type: 'transaction-retry'
 *   transactionHash: `0x${string}`
 *   upToTimestamp: string
 * }} TransactionRetryMessage
 */

/**
 * @typedef {{
 *   type: 'transaction-confirmed'
 *   transactionHash: `0x${string}`
 *   upToTimestamp: string
 * }} TransactionConfirmedMessage
 */

export default {
  /**
   * @param {any} _controller
   * @param {Env} env
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
      const upToEpoch = currentEpoch - 1n // Report up to previous epoch
      console.log(
        `Current epoch: ${currentEpoch}, reporting up to epoch: ${upToEpoch}`,
      )

      const upToTimestampMs = epochToTimestampMs(
        upToEpoch,
        BigInt(env.FILECOIN_GENESIS_BLOCK_TIMESTAMP_MS),
      )
      console.log(`Aggregating usage data up to timestamp: ${upToTimestampMs}`)

      // Aggregate usage data for all datasets that need reporting
      const usageData = await aggregateUsageData(env.DB, upToTimestampMs)

      if (usageData.length === 0) {
        console.log('No usage data found')
        return
      }

      console.log(`Found usage data for ${usageData.length} data sets`)

      // Calculate total bytes for analytics
      const { totalCdnBytes, totalCacheMissBytes } =
        calculateTotalBytes(usageData)

      // Prepare usage report data for contract call
      const usageReportData = prepareUsageReportData(usageData)

      console.log(
        `Reporting usage for ${usageReportData.dataSetIds.length} data sets`,
      )

      // Create contract call
      // We assume all args fit into max calldata size
      // See https://github.com/filbeam/worker/issues/340
      const { request } = await publicClient.simulateContract({
        account,
        address: env.FILBEAM_OPERATOR_CONTRACT_ADDRESS,
        abi: filbeamAbi,
        functionName: 'recordUsageRollups',
        args: [
          upToEpoch,
          usageReportData.dataSetIds,
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

      const upToTimestamp = new Date(upToTimestampMs).toISOString()
      // Start transaction monitor workflow
      await env.TRANSACTION_MONITOR_WORKFLOW.create({
        id: `usage-reporter-${hash}-${Date.now()}`,
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

      // Report metrics to Analytics Engine
      if (env.USAGE_REPORTER_ANALYTICS) {
        env.USAGE_REPORTER_ANALYTICS.writeDataPoint({
          indexes: [
            env.ENVIRONMENT || 'unknown', // index1: environment
            'usage_report', // index2: report type
          ],
          doubles: [
            usageReportData.dataSetIds.length, // double1: number of datasets
            Date.now(), // double2: timestamp when report was made
          ],
          blobs: [
            totalCdnBytes.toString(), // blob1: total CDN bytes (as string)
            totalCacheMissBytes.toString(), // blob2: total cache miss bytes (as string)
            upToEpoch.toString(), // blob3: epoch reported up to (as string)
          ],
        })

        console.log(
          `Analytics metrics reported: ${usageReportData.dataSetIds.length} datasets, ` +
            `${totalCdnBytes} CDN bytes, ${totalCacheMissBytes} cache miss bytes`,
        )
      } else {
        console.warn(
          'Analytics Engine not configured, skipping metrics reporting',
        )
      }
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
   * @param {Env} env
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
            throw new Error(
              `Unknown message type: ${JSON.stringify(message.body)}`,
            )
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
