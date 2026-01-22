import { getChainClient as defaultGetChainClient } from '../lib/chain.js'
import {
  getDataSetsForSettlement,
  settleCDNPaymentRails,
} from '../lib/rail-settlement.js'
import { TransactionMonitorWorkflow } from '@filbeam/workflows'
import { handleTransactionRetryQueueMessage as defaultHandleTransactionRetryQueueMessage } from '../lib/queue-handlers.js'

/**
 * @typedef {{
 *   type: 'transaction-retry'
 *   transactionHash: `0x${string}`
 * }} TransactionRetryMessage
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
        batches.map((batch) =>
          settleCDNPaymentRails({ env, dataSetIds: batch, ...chainClient }),
        ),
      )

      /** @type {`0x${string}`[]} */
      const transactionHashes = []
      for (let i = 0; i < results.length; i++) {
        const result = results[i]
        if (result.status === 'fulfilled') {
          transactionHashes.push(result.value)
        } else {
          console.error(
            `Failed to settle batch ${i + 1} (data sets: ${batches[i].join(', ')}):`,
            result.reason,
          )
        }
      }

      console.log(`Settlement transactions sent: ${transactionHashes}`)
      if (transactionHashes.length > 0) {
        await env.TRANSACTION_MONITOR_WORKFLOW.createBatch(
          transactionHashes.map((transactionHash) => ({
            id: `payment-settler-${transactionHash}-${Date.now()}`,
            params: {
              transactionHash,
              metadata: {},
            },
          })),
        )
      }

      console.log(
        `Settled ${transactionHashes.length} of ${batches.length} batches`,
      )
    } catch (error) {
      console.error('Settlement process failed:', error)
      throw error
    }
  },

  /**
   * Queue consumer for transaction-related messages
   *
   * @param {MessageBatch<TransactionRetryMessage>} batch
   * @param {Env} env
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
