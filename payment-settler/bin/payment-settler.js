import { getChainClient as defaultGetChainClient } from '../lib/chain.js'
import {
  getDataSetsForSettlement,
  getCDNRailsForSettlement,
  settleCacheMissPaymentRails,
  settleCDNBandwidthRails,
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
 *   settlementType: 'cache-miss' | 'bandwidth'
 *   ids: string[]
 * }} TransactionRetryMessage
 */

/**
 * @typedef {{
 *   type: 'settlement-confirmed'
 *   transactionHash: `0x${string}`
 *   blockNumber: string
 *   settlementType: 'cache-miss' | 'bandwidth'
 *   ids: string[]
 * }} SettlementConfirmedMessage
 */

/**
 * Splits ids into batches, settles each batch, and returns metadata for the
 * batches that succeeded.
 *
 * @param {object} args
 * @param {Env} args.env
 * @param {string[]} args.ids
 * @param {(batch: string[], batchId: string) => Promise<`0x${string}`>} args.settle
 * @param {'cache-miss' | 'bandwidth'} args.settlementType
 * @returns {Promise<
 *   {
 *     transactionHash: `0x${string}`
 *     settlementType: 'cache-miss' | 'bandwidth'
 *     ids: string[]
 *   }[]
 * >}
 */
async function settleInBatches({ env, ids, settle, settlementType }) {
  if (ids.length === 0) return []

  const batches = []
  for (let i = 0; i < ids.length; i += env.SETTLEMENT_BATCH_SIZE) {
    batches.push(ids.slice(i, i + env.SETTLEMENT_BATCH_SIZE))
  }

  console.log(
    `Prepared ${batches.length} ${settlementType} batches for settlement`,
  )

  const results = await Promise.allSettled(
    batches.map((batch, ix) => settle(batch, `${settlementType}-batch-${ix}`)),
  )

  const successfulBatches = []
  for (let i = 0; i < results.length; i++) {
    const result = results[i]
    if (result.status === 'fulfilled') {
      successfulBatches.push({
        transactionHash: result.value,
        settlementType,
        ids: batches[i],
      })
    } else {
      console.error(
        `Failed to settle ${settlementType} batch ${i + 1} (${batches[i].join(', ')}):`,
        result.reason,
      )
    }
  }

  return successfulBatches
}

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
      const chainClient = getChainClient(env)

      const [dataSetIds, cdnRailIds] = await Promise.all([
        getDataSetsForSettlement(env.DB),
        getCDNRailsForSettlement(env.DB),
      ])

      if (dataSetIds.length === 0 && cdnRailIds.length === 0) {
        console.log('No active data sets found for settlement')
        return
      }

      console.log(
        `Found ${dataSetIds.length} data sets and ${cdnRailIds.length} bandwidth rails for settlement`,
      )

      const [cacheMissBatches, bandwidthBatches] = await Promise.all([
        // Cache-miss settlement: once per data set.
        settleInBatches({
          env,
          ids: dataSetIds,
          settle: (batch, batchId) =>
            settleCacheMissPaymentRails({
              env,
              batchId,
              dataSetIds: batch,
              ...chainClient,
            }),
          settlementType: 'cache-miss',
        }),
        // Bandwidth settlement: once per shared cdn_rail_id.
        settleInBatches({
          env,
          ids: cdnRailIds,
          settle: (batch, batchId) =>
            settleCDNBandwidthRails({
              env,
              batchId,
              cdnRailIds: batch,
              ...chainClient,
            }),
          settlementType: 'bandwidth',
        }),
      ])

      const workflows = [...cacheMissBatches, ...bandwidthBatches]

      if (workflows.length > 0) {
        await env.TRANSACTION_MONITOR_WORKFLOW.createBatch(
          workflows.map(({ transactionHash, settlementType, ids }) => ({
            id: `payment-settler-${transactionHash}-${Date.now()}`,
            params: {
              transactionHash,
              metadata: {
                onSuccess: 'settlement-confirmed',
                successData: { settlementType, ids },
                retryData: { settlementType, ids },
              },
            },
          })),
        )
      }

      console.log(`Created ${workflows.length} settlement monitor workflows`)
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
