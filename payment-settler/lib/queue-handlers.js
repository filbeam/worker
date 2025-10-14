import assert from 'node:assert'
import { getChainClient as defaultGetChainClient } from './chain.js'

/**
 * @typedef {{
 *   type: 'transaction-retry'
 *   transactionHash: string
 * }} TransactionRetryMessage
 */

/**
 * @typedef {{
 *   ENVIRONMENT: 'dev' | 'calibration' | 'mainnet'
 *   RPC_URL: string
 *   FILBEAM_CONTRACT_ADDRESS: string
 *   FILBEAM_CONTROLLER_PRIVATE_KEY: string
 *   DB: D1Database
 *   TRANSACTION_MONITOR_WORKFLOW: import('cloudflare:workers').WorkflowEntrypoint
 *   TRANSACTION_QUEUE: import('cloudflare:workers').Queue<TransactionRetryMessage>
 * }} Env
 */

/**
 * Handles transaction retry queue messages
 *
 * @param {TransactionRetryMessage} message
 * @param {Env} env
 */
export async function handleTransactionRetryQueueMessage(
  message,
  env,
  { getChainClient = defaultGetChainClient } = {},
) {
  const { transactionHash } = message
  assert(transactionHash)

  console.log(`Processing transaction retry for hash: ${transactionHash}`)

  try {
    const { publicClient, walletClient, account } = getChainClient(env)

    // First check if the original transaction is still pending
    try {
      const receipt = await publicClient.getTransactionReceipt({
        hash: transactionHash,
      })

      if (receipt && receipt.blockNumber && receipt.blockNumber > 0n) {
        console.log(
          `Transaction ${transactionHash} is no longer pending, retry not needed`,
        )

        return
      }
    } catch (error) {
      // Transaction not found or still pending, continue with retry
      console.log(
        `Transaction ${transactionHash} is still pending, proceeding with retry`,
      )
    }

    // Get the original transaction
    const originalTx = await publicClient.getTransaction({
      hash: transactionHash,
    })

    console.log(`Retrieved original transaction ${transactionHash} for retry`)

    // Increase gas fees by 25% and round up
    const newMaxPriorityFeePerGas =
      (originalTx.maxPriorityFeePerGas * 1252n + 1000n) / 1000n

    const newGasLimit = BigInt(
      Math.min(
        Math.ceil(Number(originalTx.gasLimit) * 1.1),
        1e10, // block gas limit
      ),
    )

    // Use the higher of the increased priority fee or the original max fee
    const newMaxFeePerGas =
      newMaxPriorityFeePerGas > originalTx.maxFeePerGas
        ? newMaxPriorityFeePerGas
        : originalTx.maxFeePerGas

    // Replace the transaction by sending a new one with the same nonce but higher gas fees
    const retryHash = await walletClient.sendTransaction({
      account,
      to: originalTx.to,
      nonce: originalTx.nonce,
      value: originalTx.value,
      input: originalTx.input,
      gasLimit: newGasLimit,
      maxFeePerGas: newMaxFeePerGas,
      maxPriorityFeePerGas: newMaxPriorityFeePerGas,
    })

    console.log(
      `Sent retry transaction ${retryHash} for original transaction ${transactionHash}`,
    )

    // Start transaction monitor workflow
    await env.TRANSACTION_MONITOR_WORKFLOW.create({
      id: `settlement-tx-monitor-${retryHash}-${Date.now()}`,
      params: {
        transactionHash: retryHash,
        metadata: {
          // Only retry data, no success handling needed since we don't store in DB
          retryData: {},
        },
      },
    })

    console.log(`Started transaction monitor workflow for retry: ${retryHash}`)
  } catch (error) {
    console.error(
      `Failed to process transaction retry for hash: ${transactionHash}`,
      error,
    )
    throw error
  }
}
