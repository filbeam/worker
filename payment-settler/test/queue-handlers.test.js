import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  handleSettlementConfirmedQueueMessage,
  handleTransactionRetryQueueMessage,
} from '../lib/queue-handlers.js'
import { env } from 'cloudflare:test'
import { createNextId, withDataSet, getDaysAgo } from './test-helpers.js'

const nextId = createNextId()

const FILECOIN_GENESIS_BLOCK_TIMESTAMP_MS = 1667326380000
const BLOCK_NUMBER_2000 = '2000'
const TIMESTAMP_AT_BLOCK_2000 = '2022-11-02T10:53:00.000Z'
const TIMESTAMP_AT_BLOCK_3000 = '2022-11-02T19:13:00.000Z'

describe('handleSettlementConfirmedQueueMessage', () => {
  const testEnv = { ...env, FILECOIN_GENESIS_BLOCK_TIMESTAMP_MS }
  const date = new Date(2000, 1, 1, 13)

  beforeEach(async () => {
    vi.clearAllMocks()
    vi.useFakeTimers()
    vi.setSystemTime(date)
    await env.DB.exec('DELETE FROM data_sets')
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('updates single dataset', async () => {
    const dataSetId = nextId()
    const transactionHash = '0xTest'
    const message = {
      type: 'settlement-confirmed',
      transactionHash,
      blockNumber: BLOCK_NUMBER_2000,
      dataSetIds: [dataSetId],
    }

    await withDataSet(env, {
      id: dataSetId,
      withCDN: true,
      usageReportedUntil: getDaysAgo(5),
    })

    await handleSettlementConfirmedQueueMessage(message, testEnv)

    const dataSet = await env.DB.prepare(
      'SELECT cdn_payments_settled_until FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .first()

    expect(dataSet).toStrictEqual({
      cdn_payments_settled_until: TIMESTAMP_AT_BLOCK_2000,
    })
  })

  it('updates multiple datasets', async () => {
    const transactionHash = '0xTest'
    const dataSetIds = [nextId(), nextId(), nextId()]
    const message = {
      type: 'settlement-confirmed',
      transactionHash,
      blockNumber: BLOCK_NUMBER_2000,
      dataSetIds,
    }

    for (const id of dataSetIds) {
      await withDataSet(env, {
        id,
        withCDN: true,
        usageReportedUntil: getDaysAgo(5),
      })
    }

    await handleSettlementConfirmedQueueMessage(message, testEnv)

    const { results } = await env.DB.prepare(
      'SELECT id, cdn_payments_settled_until FROM data_sets ORDER BY id',
    ).all()

    expect(results).toStrictEqual(
      dataSetIds.sort().map((id) => ({
        id,
        cdn_payments_settled_until: TIMESTAMP_AT_BLOCK_2000,
      })),
    )
  })

  it('does not regress if timestamp is already newer (idempotency)', async () => {
    const dataSetId = nextId()
    const transactionHash = '0xTest'
    const message = {
      type: 'settlement-confirmed',
      transactionHash,
      blockNumber: BLOCK_NUMBER_2000,
      dataSetIds: [dataSetId],
    }

    await withDataSet(env, {
      id: dataSetId,
      withCDN: true,
      usageReportedUntil: getDaysAgo(5),
    })

    // First update with newer timestamp (block 3000)
    await env.DB.prepare(
      'UPDATE data_sets SET cdn_payments_settled_until = ? WHERE id = ?',
    )
      .bind(TIMESTAMP_AT_BLOCK_3000, dataSetId)
      .run()

    // Now try to update with older block (2000) - should not regress
    await handleSettlementConfirmedQueueMessage(message, testEnv)

    const dataSet = await env.DB.prepare(
      'SELECT cdn_payments_settled_until FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .first()

    expect(dataSet).toStrictEqual({
      cdn_payments_settled_until: TIMESTAMP_AT_BLOCK_3000,
    })
  })

  it('handles duplicate calls gracefully', async () => {
    const dataSetId = nextId()
    const transactionHash = '0xTest'
    const message = {
      type: 'settlement-confirmed',
      transactionHash,
      blockNumber: BLOCK_NUMBER_2000,
      dataSetIds: [dataSetId],
    }

    await withDataSet(env, {
      id: dataSetId,
      withCDN: true,
      usageReportedUntil: getDaysAgo(5),
    })

    // Call twice
    await handleSettlementConfirmedQueueMessage(message, testEnv)
    await handleSettlementConfirmedQueueMessage(message, testEnv)

    const dataSet = await env.DB.prepare(
      'SELECT cdn_payments_settled_until FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .first()

    expect(dataSet).toStrictEqual({
      cdn_payments_settled_until: TIMESTAMP_AT_BLOCK_2000,
    })
  })

  it('throws on missing transactionHash', async () => {
    const message = {
      type: 'settlement-confirmed',
      blockNumber: BLOCK_NUMBER_2000,
      dataSetIds: ['1'],
    }

    await expect(
      handleSettlementConfirmedQueueMessage(message, testEnv),
    ).rejects.toThrow('transactionHash')
  })

  it('throws on missing blockNumber', async () => {
    const message = {
      type: 'settlement-confirmed',
      transactionHash: '0xTest',
      dataSetIds: ['1'],
    }

    await expect(
      handleSettlementConfirmedQueueMessage(message, testEnv),
    ).rejects.toThrow('blockNumber')
  })

  it('throws on missing dataSetIds', async () => {
    const message = {
      type: 'settlement-confirmed',
      transactionHash: '0xTest',
      blockNumber: BLOCK_NUMBER_2000,
    }

    await expect(
      handleSettlementConfirmedQueueMessage(message, testEnv),
    ).rejects.toThrow('dataSetIds')
  })
})

describe('handleTransactionRetryQueueMessage', () => {
  const date = new Date(2000, 1, 1, 13)

  beforeEach(async () => {
    vi.clearAllMocks()
    vi.useFakeTimers()
    vi.setSystemTime(date)
    await env.DB.exec('DELETE FROM data_sets')
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  const mockAccount = { address: '0xMockAccountAddress' }

  const createMockPublicClient = (
    { txConfirmed } = { txConfirmed: false },
  ) => ({
    getTransactionReceipt: vi.fn().mockImplementation(async () => {
      if (txConfirmed) {
        return { blockNumber: 12345n }
      }
      throw new Error('Transaction not found')
    }),
    getTransaction: vi.fn().mockResolvedValue({
      to: '0xContractAddress',
      nonce: 5,
      value: 0n,
      input: '0x123456',
      gas: 100000n,
      maxPriorityFeePerGas: 1000000000n,
    }),
  })

  const createMockWalletClient = () => ({
    sendTransaction: vi.fn().mockResolvedValue('0xNewRetryHash'),
  })

  const createMockGetChainClient = (options) => {
    const mockPublicClient = createMockPublicClient(options)
    const mockWalletClient = createMockWalletClient()
    return {
      mock: vi.fn().mockReturnValue({
        publicClient: mockPublicClient,
        walletClient: mockWalletClient,
        account: mockAccount,
      }),
      mockPublicClient,
      mockWalletClient,
    }
  }

  const mockGetRecentSendMessage = vi.fn().mockResolvedValue({
    cid: 'mockCid',
    gasLimit: 50000,
    gasFeeCap: '2000000000',
  })

  it('sends settlement-confirmed message when original TX is already confirmed', async () => {
    const dataSetIds = ['1', '2']
    const transactionHash = '0xOriginalHash'
    const message = {
      type: 'transaction-retry',
      transactionHash,
      dataSetIds,
    }

    const { mock: mockGetChainClient } = createMockGetChainClient({
      txConfirmed: true,
    })

    const mockQueue = {
      send: vi.fn().mockResolvedValue(undefined),
    }

    const mockEnv = {
      ...env,
      TRANSACTION_QUEUE: mockQueue,
    }

    await handleTransactionRetryQueueMessage(message, mockEnv, {
      getChainClient: mockGetChainClient,
      getRecentSendMessage: mockGetRecentSendMessage,
    })

    expect(mockQueue.send).toHaveBeenCalledWith({
      type: 'settlement-confirmed',
      transactionHash,
      blockNumber: '12345',
      dataSetIds,
    })
  })

  it('passes metadata to new workflow when retrying', async () => {
    const dataSetIds = ['1', '2']
    const transactionHash = '0xOriginalHash'
    const message = {
      type: 'transaction-retry',
      transactionHash,
      dataSetIds,
    }

    const { mock: mockGetChainClient } = createMockGetChainClient({
      txConfirmed: false,
    })

    const mockWorkflow = {
      create: vi.fn().mockResolvedValue(undefined),
    }

    const mockEnv = {
      ...env,
      TRANSACTION_MONITOR_WORKFLOW: mockWorkflow,
    }

    await handleTransactionRetryQueueMessage(message, mockEnv, {
      getChainClient: mockGetChainClient,
      getRecentSendMessage: mockGetRecentSendMessage,
    })

    expect(mockWorkflow.create).toHaveBeenCalledWith({
      id: expect.stringMatching(/^payment-settler-0xNewRetryHash-\d+$/),
      params: {
        transactionHash: '0xNewRetryHash',
        metadata: {
          onSuccess: 'settlement-confirmed',
          successData: { dataSetIds },
          retryData: { dataSetIds },
        },
      },
    })
  })
})
