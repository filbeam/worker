import assert from 'node:assert'
import { getChainClient as defaultGetChainClient } from './chain.js'
import { getRecentSendMessage as defaultGetRecentSendMessage } from './filfox.js'
import { epochToTimestampMs } from './epoch.js'

/**
 * @typedef {{
 *   type: 'transaction-retry'
 *   transactionHash: `0x${string}`
 *   settlementType: 'cache-miss' | 'bandwidth'
 *   ids: string[]
 * }} TransactionRetryMessage
 */

/**
 * @typedef {{
 *   type: 'settlement-confirmed'
 *   transactionHash: `0x${string}`
 *   blockNumber: string
 *   settlementType: 'cache-miss' | 'bandwidth'
 *   ids: string[]
 * }} SettlementConfirmedMessage
 */

/**
 * Handles settlement confirmed queue messages.
 *
 * Cache-miss settlement advances the per-data-set watermark
 * (data_sets.cdn_payments_settled_until), bandwidth settlement advances the
 * per-rail watermark (cdn_rail_settlement_state.cdn_payments_settled_until).
 *
 * @param {SettlementConfirmedMessage} message
 * @param {Env} env
 */
export async function handleSettlementConfirmedQueueMessage(message, env) {
  const { transactionHash, blockNumber, settlementType, ids } = message
  assert(transactionHash, 'transactionHash is required')
  assert(blockNumber, 'blockNumber is required')
  assert(settlementType, 'settlementType is required')
  assert(ids, 'ids is required')

  console.log(`Processing settlement confirmation for hash: ${transactionHash}`)

  try {
    const settledUntil = new Date(
      epochToTimestampMs(
        blockNumber,
        Number(env.FILECOIN_GENESIS_BLOCK_TIMESTAMP_MS),
      ),
    ).toISOString()

    if (settlementType === 'bandwidth') {
      const valuePlaceholders = ids.map(() => '(?, ?)').join(', ')
      const bindings = ids.flatMap((id) => [id, settledUntil])
      await env.DB.prepare(
        `
        INSERT INTO cdn_rail_settlement_state (cdn_rail_id, cdn_payments_settled_until)
        VALUES ${valuePlaceholders}
        ON CONFLICT (cdn_rail_id) DO UPDATE SET
          cdn_payments_settled_until = MAX(
            cdn_rail_settlement_state.cdn_payments_settled_until,
            excluded.cdn_payments_settled_until
          )
        `,
      )
        .bind(...bindings)
        .run()

      console.log(
        `Updated bandwidth cdn_payments_settled_until to ${settledUntil} for ${ids.length} rails`,
      )
    } else {
      const placeholders = ids.map(() => '?').join(', ')
      await env.DB.prepare(
        `
        UPDATE data_sets
        SET cdn_payments_settled_until = ?
        WHERE id IN (${placeholders})
          AND cdn_payments_settled_until < ?
        `,
      )
        .bind(settledUntil, ...ids, settledUntil)
        .run()

      console.log(
        `Updated cache-miss cdn_payments_settled_until to ${settledUntil} for ${ids.length} data sets`,
      )
    }
  } catch (error) {
    console.error(
      `Failed to process settlement confirmation for hash: ${transactionHash}`,
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
  const { transactionHash, settlementType, ids } = message
  assert(transactionHash, 'transactionHash is required')
  assert(settlementType, 'settlementType is required')
  assert(ids, 'ids is required')

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
          type: 'settlement-confirmed',
          transactionHash,
          blockNumber: receipt.blockNumber.toString(),
          settlementType,
          ids,
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

    // Start transaction monitor workflow
    await env.TRANSACTION_MONITOR_WORKFLOW.create({
      id: `payment-settler-${retryHash}-${Date.now()}`,
      params: {
        transactionHash: retryHash,
        metadata: {
          onSuccess: 'settlement-confirmed',
          successData: { settlementType, ids },
          retryData: { settlementType, ids },
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
