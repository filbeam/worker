/** @import {MessageBatch} from 'cloudflare:workers' */
import { TransactionMonitorWorkflow } from '@filbeam/workflows'
import { terminateCDNServiceForSanctionedWallets as defaultTerminateCDNServiceForSanctionedWallets } from '../lib/terminate-cdn-service.js'
import {
  handleTerminateCdnServiceQueueMessage as defaultHandleTerminateCdnServiceQueueMessage,
  handleTransactionRetryQueueMessage as defaultHandleTransactionRetryQueueMessage,
} from '../lib/queue-handlers.js'

/**
 * @typedef {{
 *   type: 'terminate-cdn-service'
 *   dataSetId: string
 * }} TerminateServiceMessage
 */

/**
 * @typedef {{
 *   type: 'transaction-retry'
 *   transactionHash: string
 * }} TransactionRetryMessage
 */

/**
 * @typedef {{
 *   GLIF_TOKEN: string
 *   ENVIRONMENT: 'dev' | 'calibration' | 'mainnet'
 *   RPC_URL:
 *     | 'https://api.calibration.node.glif.io/'
 *     | 'https://api.node.glif.io/'
 *   FILECOIN_WARM_STORAGE_SERVICE_ADDRESS: string
 *   FILCDN_CONTROLLER_ADDRESS_PRIVATE_KEY: string
 *   DB: D1Database
 *   TRANSACTION_QUEUE: import('cloudflare:workers').Queue<
 *     TerminateServiceMessage | TransactionRetryMessage
 *   >
 *   TRANSACTION_MONITOR_WORKFLOW: import('cloudflare:workers').WorkflowEntrypoint
 * }} TerminatorEnv
 */

export default {
  /**
   * @param {any} _controller
   * @param {TerminatorEnv} env
   * @param {ExecutionContext} _ctx
   * @param {object} options
   * @param {typeof defaultTerminateCDNServiceForSanctionedWallets} [options.terminateCDNServiceForSanctionedWallets]
   */
  async scheduled(
    _controller,
    env,
    _ctx,
    {
      terminateCDNServiceForSanctionedWallets = defaultTerminateCDNServiceForSanctionedWallets,
    } = {},
  ) {
    await terminateCDNServiceForSanctionedWallets(env)
  },

  /**
   * Queue consumer for all transaction-related messages
   *
   * @param {MessageBatch<TerminateServiceMessage | TransactionRetryMessage>} batch
   * @param {TerminatorEnv} env
   * @param {ExecutionContext} ctx
   */
  async queue(
    batch,
    env,
    ctx,
    {
      handleTerminateCdnServiceQueueMessage = defaultHandleTerminateCdnServiceQueueMessage,
      handleTransactionRetryQueueMessage = defaultHandleTransactionRetryQueueMessage,
    } = {},
  ) {
    const unknownMessageErrors = []
    for (const message of batch.messages) {
      console.log(
        `Processing transaction queue message of type: ${message.body.type}`,
      )
      try {
        switch (message.body.type) {
          case 'terminate-cdn-service':
            await handleTerminateCdnServiceQueueMessage(message.body, env)
            break

          case 'transaction-retry':
            await handleTransactionRetryQueueMessage(message.body, env)
            break

          default:
            unknownMessageErrors.push(
              new Error(`Unknown message type: ${message.body.type}`),
            )
        }
        message.ack()
      } catch (error) {
        console.error(`Failed to process queue message, retrying:`, error)
        message.retry()
      }
    }

    if (unknownMessageErrors.length === 1) {
      throw unknownMessageErrors[0]
    } else if (unknownMessageErrors.length) {
      throw new AggregateError(unknownMessageErrors, 'Unknown message types')
    }
  },
}

// Cloudflare worker runtime requires that you export workflows from the entrypoint file
export { TransactionMonitorWorkflow }
