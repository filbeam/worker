import { getChainClient as defaultGetChainClient } from '../lib/chain.js'
import {
  getDataSetsForSettlement,
  settleCDNPaymentRails,
} from '../lib/rail-settlement.js'
import { TransactionMonitorWorkflow } from '@filbeam/workflows'
import {
  handleTransactionRetryQueueMessage as defaultHandleTransactionRetryQueueMessage,
  handleSettlementConfirmedQueueMessage as defaultHandleSettlementConfirmedQueueMessage,
} from '../lib/queue-handlers.js'

/**
 * @typedef {{
 *   type: 'transaction-retry'
 *   transactionHash: `0x${string}`
 *   dataSetIds: string[]
 * }} TransactionRetryMessage
 */

/**
 * @typedef {{
 *   type: 'settlement-confirmed'
 *   transactionHash: `0x${string}`
 *   blockNumber: string
 *   dataSetIds: string[]
 * }} SettlementConfirmedMessage
 */

export default {
  /**
   * @param {any} _controller
   * @param {Env} env
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
      const dataSetIds = await getDataSetsForSettlement(env.DB)

      if (dataSetIds.length === 0) {
        console.log('No active data sets found for settlement')
        return
      }

      console.log(
        `Found ${dataSetIds.length} data sets for settlement:`,
        dataSetIds,
      )

      const batches = []
      for (let i = 0; i < dataSetIds.length; i += env.SETTLEMENT_BATCH_SIZE) {
        batches.push(dataSetIds.slice(i, i + env.SETTLEMENT_BATCH_SIZE))
      }

      console.log(`Prepared ${batches.length} batches for settlement`)

      const chainClient = getChainClient(env)

      const results = await Promise.allSettled(
        batches.map((batch, ix) =>
          settleCDNPaymentRails({
            env,
            batchId: `batch-${ix}`,
            dataSetIds: batch,
            ...chainClient,
          }),
        ),
      )

      /** @type {{ transactionHash: `0x${string}`; dataSetIds: string[] }[]} */
      const successfulBatches = []
      for (let i = 0; i < results.length; i++) {
        const result = results[i]
        if (result.status === 'fulfilled') {
          successfulBatches.push({
            transactionHash: result.value,
            dataSetIds: batches[i],
          })
        } else {
          console.error(
            `Failed to settle batch ${i + 1} (data sets: ${batches[i].join(', ')}):`,
            result.reason,
          )
        }
      }

      if (successfulBatches.length > 0) {
        await env.TRANSACTION_MONITOR_WORKFLOW.createBatch(
          successfulBatches.map(({ transactionHash, dataSetIds }) => ({
            id: `payment-settler-${transactionHash}-${Date.now()}`,
            params: {
              transactionHash,
              metadata: {
                onSuccess: 'settlement-confirmed',
                successData: { dataSetIds },
                retryData: { dataSetIds },
              },
            },
          })),
        )
      }

      console.log(
        `Settled ${successfulBatches.length} of ${batches.length} batches`,
      )
    } catch (error) {
      console.error('Settlement process failed:', error)
      throw error
    }
  },

  /**
   * Queue consumer for transaction-related messages
   *
   * @param {MessageBatch<
   *   TransactionRetryMessage | SettlementConfirmedMessage
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
      handleSettlementConfirmedQueueMessage = defaultHandleSettlementConfirmedQueueMessage,
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
          case 'settlement-confirmed':
            await handleSettlementConfirmedQueueMessage(message.body, env)
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
