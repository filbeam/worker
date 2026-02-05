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

    const { publicClient } = getChainClient(this.env)

    try {
      // Wait for transaction receipt with timeout
      const receiptInfo = await step.do(
        `wait for transaction receipt ${transactionHash}`,
        {
          timeout: `5 minutes`,
          retries: {
            delay: '10 seconds',
            limit: 3,
          },
        },
        async () => {
          console.log('Starting waitForTransactionReceipt', { transactionHash })

          const receipt = await publicClient.waitForTransactionReceipt({
            hash: transactionHash,
          })

          console.log('Transaction receipt received', {
            transactionHash,
            status: receipt.status,
            blockNumber: receipt.blockNumber?.toString(),
            gasUsed: receipt.gasUsed.toString(),
          })

          return {
            status: receipt.status,
            blockNumber: receipt.blockNumber?.toString() ?? null,
            gasUsed: receipt.gasUsed.toString(),
            transactionHash: receipt.transactionHash,
          }
        },
      )

      // Handle success if onSuccess message type is provided
      if (metadata?.onSuccess) {
        const { blockNumber } = receiptInfo
        await step.do(
          'send confirmation to queue',
          { timeout: '30 seconds' },
          async () => {
            const message = {
              type: metadata.onSuccess,
              transactionHash,
              blockNumber,
              ...metadata?.successData,
            }

            console.log(`Sending ${metadata.onSuccess}  message to queue`, {
              message,
            })
            await this.env.TRANSACTION_QUEUE.send(message)

            console.log(
              `Sent ${metadata.onSuccess} message to queue for transaction ${transactionHash}`,
            )
          },
        )
      }
    } catch (error) {
      console.error('Workflow execution failed', {
        transactionHash,
        errorType: typeof error,
        errorMessage: error instanceof Error ? error.message : String(error),
        errorStack: error instanceof Error ? error.stack : undefined,
        error,
      })

      // Handle failure - always send retry
      await step.do(
        'send to retry queue',
        { timeout: '30 seconds' },
        async () => {
          const message = {
            type: 'transaction-retry',
            transactionHash,
            ...metadata?.retryData,
          }
          console.log('Sending transaction-retry message to queue', { message })
          await this.env.TRANSACTION_QUEUE.send(message)

          console.log(
            `Sent transaction-retry message to queue for transaction ${transactionHash}`,
          )
        },
      )
    }
  }
}

// Suppress the following warning when running `wrangler types`:
//
// The entrypoint lib/transaction-monitor-workflow.js has exports like an ES Module,
// but hasn't defined a default export like a module worker normally would.
// Building the worker using "service-worker" format...
export default {}
