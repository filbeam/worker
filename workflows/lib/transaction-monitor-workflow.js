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

    // Log environment diagnostics at workflow entry
    console.log('TransactionMonitorWorkflow starting', {
      transactionHash,
      metadata,
      envKeys: Object.keys(this.env || {}),
      hasControllerPrivateKey: !!this.env?.FILBEAM_CONTROLLER_PRIVATE_KEY,
      hasPaymentSettlerPrivateKey:
        !!this.env?.FILBEAM_OPERATOR_PAYMENT_SETTLER_PRIVATE_KEY,
      hasTransactionQueue: !!this.env?.TRANSACTION_QUEUE,
    })

    // Initialize chain client outside the step to catch initialization errors
    let publicClient
    try {
      publicClient = getChainClient(this.env).publicClient
      console.log('Chain client initialized successfully')
    } catch (err) {
      console.error('Failed to initialize chain client', err)
      throw err
    }

    try {
      // Wait for transaction receipt with timeout
      await step.do(
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
          try {
            const receipt = await publicClient.waitForTransactionReceipt({
              hash: transactionHash,
            })
            console.log('Transaction receipt received', {
              transactionHash,
              status: receipt.status,
              blockNumber: receipt.blockNumber?.toString(),
            })
            return receipt
          } catch (error) {
            console.error('RPC error in waitForTransactionReceipt', {
              transactionHash,
              error,
            })
            throw error
          }
        },
      )

      // Handle success if onSuccess message type is provided
      if (metadata?.onSuccess) {
        await step.do(
          'send confirmation to queue',
          { timeout: '30 seconds' },
          async () => {
            const message = {
              type: metadata.onSuccess,
              transactionHash,
              ...metadata?.successData,
            }
            console.log('Sending success message to queue', { message })
            try {
              await this.env.TRANSACTION_QUEUE.send(message)
              console.log('Success message sent to queue')
            } catch (error) {
              console.error('Failed to send success message to queue', error)
              throw error
            }
          },
        )
      }
    } catch (error) {
      console.error('Workflow caught error', {
        transactionHash,
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
          console.log('Sending retry message to queue', { message })
          try {
            await this.env.TRANSACTION_QUEUE.send(message)
            console.log('Retry message sent to queue')
          } catch (error) {
            console.error('Failed to send retry message to queue', error)
            throw error
          }
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
