/** @import {WorkflowEvent, WorkflowStep} from 'cloudflare:workers' */
import { WorkflowEntrypoint } from 'cloudflare:workers'
import { getChainClient } from './chain.js'

/**
 * Generalized workflow that monitors a transaction and handles success/failure
 * scenarios
 *
 * @example // Simple retry-only usage await
 * env.TRANSACTION_MONITOR_WORKFLOW.create({ id:
 * `transaction-monitor-${hash}-${Date.now()}`, params: { transactionHash: hash,
 * metadata: {} // No success handling needed } })
 *
 * @example // With confirmation and retry data await
 * env.TRANSACTION_MONITOR_WORKFLOW.create({ id:
 * `rollup-tx-monitor-${hash}-${Date.now()}`, params: { transactionHash: hash,
 * metadata: { onSuccess: 'transaction-confirmed', successData: { upToTimestamp
 * }, retryData: { upToTimestamp } } } })
 */
export class TransactionMonitorWorkflow extends WorkflowEntrypoint {
  /**
   * @param {WorkflowEvent} event
   * @param {WorkflowStep} step
   */
  async run({ payload }, step) {
    const { transactionHash, metadata = {} } = payload

    try {
      // Wait for transaction receipt with timeout
      await step.do(
        `wait for transaction receipt ${transactionHash}`,
        {
          timeout: `5 minutes`,
          retries: {
            limit: 3,
          },
        },
        async () => {
          const { publicClient } = getChainClient(this.env)
          return await publicClient.waitForTransactionReceipt({
            hash: transactionHash,
          })
        },
      )

      // Handle success if onSuccess message type is provided
      if (metadata.onSuccess) {
        await step.do(
          'send confirmation to queue',
          { timeout: '30 seconds' },
          async () => {
            await this.env.TRANSACTION_QUEUE.send({
              type: metadata.onSuccess,
              transactionHash,
              ...metadata.successData,
            })

            console.log(
              `Sent ${metadata.onSuccess} message to queue for transaction ${transactionHash}`,
            )
          },
        )
      }
    } catch (error) {
      // Handle failure - always send retry
      await step.do(
        'send to retry queue',
        { timeout: '30 seconds' },
        async () => {
          await this.env.TRANSACTION_QUEUE.send({
            type: 'transaction-retry',
            transactionHash,
            ...metadata.retryData,
          })

          console.log(
            `Sent retry message to queue for transaction ${transactionHash}`,
          )
        },
      )
    }
  }
}
