import assert from 'node:assert'
import { getChainClient as defaultGetChainClient } from './chain.js'
import { getRecentSendMessage as defaultGetRecentSendMessage } from './filfox.js'

/**
 * @typedef {{
 *   type: 'transaction-retry'
 *   transactionHash: `0x${string}`
 *   upToTimestamp: string
 * }} TransactionRetryMessage
 */

/**
 * @typedef {{
 *   type: 'transaction-confirmed'
 *   transactionHash: `0x${string}`
 *   upToTimestamp: string
 * }} TransactionConfirmedMessage
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
      SET usage_reported_until = ?,
          pending_usage_report_tx_hash = NULL
      WHERE pending_usage_report_tx_hash = ?
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
  {
    getChainClient = defaultGetChainClient,
    getRecentSendMessage = defaultGetRecentSendMessage,
  } = {},
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

    // Get the original transaction to determine gas price
    const originalTx = await publicClient.getTransaction({
      hash: transactionHash,
    })

    console.log(`Retrieved original transaction ${transactionHash} for retry`)

    const recentSendMessage = await getRecentSendMessage()
    console.log(
      `Calculating gas fees from the recent Send message ${recentSendMessage.cid}`,
    )

    const originalMaxPriorityFeePerGas = originalTx.maxPriorityFeePerGas
    assert(
      originalMaxPriorityFeePerGas !== undefined,
      'originalTx.maxPriorityFeePerGas is null',
    )

    // Increase by 25% + 1 attoFIL (easier: 25.2%) and round up
    const newMaxPriorityFeePerGas =
      (originalMaxPriorityFeePerGas * 1252n + 1000n) / 1000n

    const newGasLimit = BigInt(
      Math.min(
        Math.ceil(
          Math.max(Number(originalTx.gas), recentSendMessage.gasLimit) * 1.1,
        ),
        1e10, // block gas limit
      ),
    )

    const recentGasFeeCap = BigInt(recentSendMessage.gasFeeCap)
    const newMaxFeePerGas =
      newMaxPriorityFeePerGas > recentGasFeeCap
        ? newMaxPriorityFeePerGas
        : recentGasFeeCap

    // Replace the transaction by sending a new one with the same nonce but higher gas fees
    const retryHash = await walletClient.sendTransaction({
      account,
      to: originalTx.to,
      nonce: originalTx.nonce,
      value: originalTx.value,
      input: originalTx.input,
      gas: newGasLimit,
      maxFeePerGas: newMaxFeePerGas,
      maxPriorityFeePerGas: newMaxPriorityFeePerGas,
    })

    console.log(
      `Sent retry transaction ${retryHash} for original transaction ${transactionHash}`,
    )

    // Update database with new transaction hash
    await env.DB.prepare(
      `UPDATE data_sets SET pending_usage_report_tx_hash = ? WHERE pending_usage_report_tx_hash = ?`,
    )
      .bind(retryHash, transactionHash)
      .run()

    // Start a new transaction monitor workflow for the retry transaction
    await env.TRANSACTION_MONITOR_WORKFLOW.create({
      id: `usage-reporter-${retryHash}-${Date.now()}`,
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
