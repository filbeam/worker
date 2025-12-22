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
 * `usage-reporter-${hash}-${Date.now()}`, params: { transactionHash: hash,
 * metadata: { onSuccess: 'transaction-confirmed', successData: { upToTimestamp
 * }, retryData: { upToTimestamp } } } })
 */
export class TransactionMonitorWorkflow extends WorkflowEntrypoint {
  /**
   * @param {WorkflowEvent<{
   *   transactionHash: `0x${string}`
   *   metadata?: {
   *     onSuccess?: string
   *     successData?: object
   *     retryData?: object
   *   }
   * }>} event
   * @param {WorkflowStep} step
   */
  async run(event, step) {
    try {
      // Wait for transaction receipt with timeout
      await step.do(
        `wait for transaction receipt ${event.payload.transactionHash}`,
        {
          timeout: '10 minutes',
          retries: {
            limit: 5,
            delay: '10 seconds',
            backoff: 'exponential',
          },
        },
        async () => {
          const { publicClient } = getChainClient(this.env)
          return await publicClient.waitForTransactionReceipt({
            hash: event.payload.transactionHash,
            retryCount: 5,
            retryDelay: 10_000,
            timeout: 600_000,
            pollingInterval: 5_000,
          })
        },
      )

      // Handle success if onSuccess message type is provided
      if (event.payload.metadata?.onSuccess) {
        await step.do(
          'send confirmation to queue',
          { timeout: '30 seconds' },
          async () => {
            await this.env.TRANSACTION_QUEUE.send({
              type: event.payload.metadata?.onSuccess,
              transactionHash: event.payload.transactionHash,
              ...event.payload.metadata?.successData,
            })

            console.log(
              `Sent ${event.payload.metadata?.onSuccess} message to queue for transaction ${event.payload.transactionHash}`,
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
            transactionHash: event.payload.transactionHash,
            ...event.payload.metadata?.retryData,
          })

          console.log(
            `Sent retry message to queue for transaction ${event.payload.transactionHash}`,
          )
        },
      )
    }
  }
}
