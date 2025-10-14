import assert from 'node:assert'
import { getChainClient as defaultGetChainClient } from './chain.js'

/**
 * @typedef {{
 *   type: 'transaction-retry'
 *   transactionHash: string
 *   upToTimestamp: string
 * }} TransactionRetryMessage
 */

/**
 * @typedef {{
 *   type: 'transaction-confirmed'
 *   transactionHash: string
 *   upToTimestamp: string
 * }} TransactionConfirmedMessage
 */

/**
 * @typedef {{
 *   ENVIRONMENT: 'dev' | 'calibration' | 'mainnet'
 *   RPC_URL: string
 *   FILBEAM_CONTRACT_ADDRESS: string
 *   FILBEAM_CONTROLLER_PRIVATE_KEY: string
 *   DB: D1Database
 *   TRANSACTION_MONITOR_WORKFLOW: import('cloudflare:workers').WorkflowEntrypoint
 *   TRANSACTION_QUEUE: import('cloudflare:workers').Queue<
 *     TransactionRetryMessage | TransactionConfirmedMessage
 *   >
 * }} Env
 */

/**
 * Handles transaction confirmed queue messages
 *
 * @param {TransactionConfirmedMessage} message
 * @param {Env} env
 */
export async function handleTransactionConfirmedQueueMessage(message, env) {
  const { transactionHash, upToTimestamp } = message
  assert(transactionHash)
  assert(upToTimestamp)

  console.log(
    `Processing transaction confirmation for hash: ${transactionHash}`,
  )

  try {
    // Update all datasets with this pending transaction hash
    await env.DB.prepare(
      `
      UPDATE data_sets
      SET usage_reported_until = datetime(?),
          pending_usage_reporting_tx_hash = NULL
      WHERE pending_usage_reporting_tx_hash = ?
      `,
    )
      .bind(upToTimestamp, transactionHash)
      .run()

    console.log(
      `Updated usage_reported_until to ${upToTimestamp} for transaction ${transactionHash}`,
    )
  } catch (error) {
    console.error(
      `Failed to process transaction confirmation for hash: ${transactionHash}`,
      error,
    )
    throw error
  }
}

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
  const { transactionHash, upToTimestamp } = message
  assert(transactionHash)
  assert(upToTimestamp)

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
        // Transaction already confirmed - send confirmation message to queue
        await env.TRANSACTION_QUEUE.send({
          type: 'transaction-confirmed',
          transactionHash,
          upToTimestamp,
        })
        console.log(
          `Sent confirmation message to queue for already confirmed transaction ${transactionHash}`,
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

    // Update database with new transaction hash
    await env.DB.prepare(
      `UPDATE data_sets SET pending_usage_reporting_tx_hash = ? WHERE pending_usage_reporting_tx_hash = ?`,
    )
      .bind(retryHash, transactionHash)
      .run()

    // Start a new transaction monitor workflow for the retry transaction
    await env.TRANSACTION_MONITOR_WORKFLOW.create({
      id: `rollup-tx-monitor-${retryHash}-${Date.now()}`,
      params: {
        transactionHash: retryHash,
        metadata: {
          onSuccess: 'transaction-confirmed',
          successData: { upToTimestamp },
          retryData: { upToTimestamp },
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
