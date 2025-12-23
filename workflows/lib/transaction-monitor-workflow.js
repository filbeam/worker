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
  async run({ payload }, step) {
    const { transactionHash, metadata } = payload

    const transactionResult = await step.do(
      `wait for transaction receipt ${transactionHash}`,
      async () => {
        try {
          const { publicClient } = getChainClient(this.env)
          const receipt = await publicClient.waitForTransactionReceipt({
            hash: transactionHash,
            retryCount: 5,
            retryDelay: 10_000,
            timeout: 600_000,
            pollingInterval: 5_000,
          })
          return { success: true, receipt }
        } catch (error) {
          const errorMessage =
            error instanceof Error ? error.message : String(error)
          console.error(`Transaction ${transactionHash} failed:`, error)
          return { success: false, error: errorMessage }
        }
      },
    )

    if (transactionResult.success && metadata?.onSuccess) {
      await step.do('send confirmation to queue', async () => {
        await this.env.TRANSACTION_QUEUE.send({
          type: metadata.onSuccess,
          transactionHash,
          ...metadata.successData,
        })

        console.log(
          `Sent ${metadata.onSuccess} message to queue for transaction ${transactionHash}`,
        )
      })
    }

    if (!transactionResult.success) {
      await step.do('send to retry queue', async () => {
        await this.env.TRANSACTION_QUEUE.send({
          type: 'transaction-retry',
          transactionHash,
          ...metadata?.retryData,
        })

        console.log(
          `Sent retry message to queue for transaction ${transactionHash}`,
        )
      })
    }
  }
}
