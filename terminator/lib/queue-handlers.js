import assert from 'node:assert'
import { abi as fwssAbi } from './fwss.js'
import { getChainClient as defaultGetChainClient } from './chain.js'
import { getRecentSendMessage as defaultGetRecentSendMessage } from './filfox.js'

/**
 * @typedef {{
 *   type: 'terminate-cdn-service'
 *   dataSetId: string
 * }} TerminateServiceMessage
 */

/**
 * @typedef {{
 *   type: 'transaction-retry'
 *   transactionHash: `0x${string}`
 * }} TransactionRetryMessage
 */

/**
 * Handles terminate CDN service queue messages
 *
 * @param {TerminateServiceMessage} message
 * @param {Env} env
 */
export async function handleTerminateCdnServiceQueueMessage(
  message,
  env,
  { getChainClient = defaultGetChainClient } = {},
) {
  const { dataSetId } = message
  assert(dataSetId)

  console.log(
    `Processing terminate CDN service request for dataSetId: ${dataSetId}`,
  )

  try {
    const { walletClient, publicClient, account } = getChainClient(env)

    // Create contract call
    const { request } = await publicClient.simulateContract({
      account,
      abi: fwssAbi,
      address: /** @type {`0x${string}`} */ (
        env.FILECOIN_WARM_STORAGE_SERVICE_ADDRESS
      ),
      functionName: 'terminateCDNService',
      args: [BigInt(dataSetId)],
    })

    console.log(
      `Sending terminateCDNService transaction for dataSetId: ${dataSetId}`,
    )

    // Send transaction
    const hash = await walletClient.writeContract(request)

    console.log(`Transaction sent for dataSetId: ${dataSetId}, hash: ${hash}`)

    await env.DB.prepare(
      `
    UPDATE data_sets
    SET terminate_service_tx_hash = ?
    WHERE id = ?
    `,
    )
      .bind(hash, String(dataSetId))
      .run()

    // Start transaction monitor workflow
    await env.TRANSACTION_MONITOR_WORKFLOW.create({
      id: `transaction-monitor-${hash}-${Date.now()}`,
      params: {
        transactionHash: hash,
      },
    })

    console.log(`Started transaction monitor workflow for transaction: ${hash}`)
  } catch (error) {
    console.error(
      `Failed to process terminate CDN service for dataSetId: ${dataSetId}`,
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
          // THIS REQUIRES CAREFUL REVIEW
          // Viem does not provide `originalTx.gasLimit`, only `gas`.
          // Is this new calculation still correct?
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

    await env.DB.prepare(
      `UPDATE data_sets SET terminate_service_tx_hash = ? WHERE terminate_service_tx_hash = ?`,
    )
      .bind(retryHash, transactionHash)
      .run()

    // Start a new transaction monitor workflow for the retry transaction
    await env.TRANSACTION_MONITOR_WORKFLOW.create({
      id: `transaction-monitor-${retryHash}-${Date.now()}`,
      params: {
        transactionHash: retryHash,
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
