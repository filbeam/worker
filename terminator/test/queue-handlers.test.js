import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  handleTerminateCdnServiceQueueMessage,
  handleTransactionRetryQueueMessage,
} from '../lib/queue-handlers.js'
import { env } from 'cloudflare:test'
import { abi as filBeamAbi } from '../lib/filbeam.js'
import { randomId, withDataSet } from './test-helpers.js'
import { privateKeyToAccount } from 'viem/accounts'

// Test fixtures and helpers
const createMockEnv = (env) => ({
  ...env,
  ENVIRONMENT: 'calibration',
  RPC_URL: 'https://api.calibration.node.glif.io/',
  FILBEAM_OPERATOR_CONTRACT_ADDRESS: '0xcontract',
  FILBEAM_OPERATOR_CONTROLLER_PRIVATE_KEY: '0xprivatekey',
  TRANSACTION_MONITOR_WORKFLOW: {
    create: vi.fn().mockResolvedValue(undefined),
  },
  TRANSACTION_QUEUE: {
    send: vi.fn().mockResolvedValue(undefined),
  },
})

const mockAccount = privateKeyToAccount(
  '0xdead000000000000000000000000000000000000000000000000000000000000',
)

const createMockChainClient = (env) => ({
  walletClient: {
    writeContract: vi.fn().mockResolvedValue('0xtxhash123'),
  },
  publicClient: {
    simulateContract: vi.fn().mockResolvedValue({}),
    getTransactionReceipt: vi.fn().mockResolvedValue({
      blockHash: '0xblockhash',
      blockNumber: BigInt(123),
      from: '0xfrom',
      to: '0xto',
      value: '1000000000000000000',
    }),
    getTransaction: vi.fn().mockResolvedValue({
      blockHash: '0xblockhash',
      blockNumber: BigInt(123),
      from: '0xfrom',
      to: '0xto',
      value: '1000000000000000000',
    }),
  },
  account: mockAccount,
})

describe('handleTerminateCdnServiceQueueMessage', () => {
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

  it('processes terminate service message successfully', async () => {
    const dataSetId = randomId()
    const message = { dataSetId }
    const mockEnv = createMockEnv(env)
    const mockChainClient = createMockChainClient(env)
    const mockRequest = { address: '0xcontract' }
    mockChainClient.publicClient.simulateContract.mockResolvedValue({
      request: mockRequest,
    })
    mockChainClient.walletClient.writeContract.mockResolvedValue('0xtxhash123')
    await withDataSet(mockEnv, { id: dataSetId.toString(), withCDN: true })

    await handleTerminateCdnServiceQueueMessage(message, mockEnv, {
      getChainClient: (env) => mockChainClient,
    })

    const { results: dataSets } = await env.DB.prepare(
      'SELECT terminate_service_tx_hash FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .all()

    expect(dataSets).toStrictEqual([
      { terminate_service_tx_hash: '0xtxhash123' },
    ])

    expect(mockChainClient.publicClient.simulateContract).toHaveBeenCalledWith({
      account: mockAccount,
      address: '0xcontract',
      abi: filBeamAbi,
      functionName: 'terminateCDNPaymentRails',
      args: [BigInt(dataSetId)],
    })

    expect(mockChainClient.walletClient.writeContract).toHaveBeenCalledWith(
      mockRequest,
    )

    expect(mockEnv.TRANSACTION_MONITOR_WORKFLOW.create).toHaveBeenCalledWith({
      id: `transaction-monitor-0xtxhash123-${date.getTime()}`,
      params: {
        transactionHash: '0xtxhash123',
      },
    })
  })

  it('handles contract simulation failure', async () => {
    const dataSetId = randomId()
    const message = { dataSetId }
    const error = new Error('Contract simulation failed')
    const mockEnv = createMockEnv(env)
    const mockChainClient = createMockChainClient()

    mockChainClient.publicClient.simulateContract.mockRejectedValue(error)

    await expect(
      handleTerminateCdnServiceQueueMessage(message, mockEnv, {
        getChainClient: (env) => mockChainClient,
      }),
    ).rejects.toThrow('Contract simulation failed')
    expect(mockChainClient.walletClient.writeContract).not.toHaveBeenCalled()
    expect(mockEnv.TRANSACTION_MONITOR_WORKFLOW.create).not.toHaveBeenCalled()
  })

  it('handles contract call failure', async () => {
    const message = { dataSetId: randomId() }
    const error = new Error('Contract call failed')
    const mockEnv = createMockEnv(env)
    const mockChainClient = createMockChainClient()

    mockChainClient.walletClient.writeContract.mockRejectedValue(error)

    await expect(
      handleTerminateCdnServiceQueueMessage(message, mockEnv, {
        getChainClient: (env) => mockChainClient,
      }),
    ).rejects.toThrow('Contract call failed')
    expect(mockEnv.TRANSACTION_MONITOR_WORKFLOW.create).not.toHaveBeenCalled()
  })

  it('handles workflow creation failure', async () => {
    const message = { dataSetId: randomId() }
    const error = new Error('Workflow creation failed')
    const mockEnv = createMockEnv(env)

    mockEnv.TRANSACTION_MONITOR_WORKFLOW.create.mockRejectedValue(error)

    await expect(
      handleTerminateCdnServiceQueueMessage(message, mockEnv, {
        getChainClient: createMockChainClient,
      }),
    ).rejects.toThrow('Workflow creation failed')
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

  describe('when original transaction is still pending', () => {
    it('retries pending transaction successfully -- get receipt raises error', async () => {
      const transactionHash = '0xoriginalhash123'
      const retryTransactionHash = '0xtxhash123'
      const message = {
        transactionHash,
      }
      await withDataSet(env, { terminateServiceTxHash: transactionHash })
      const mockEnv = createMockEnv(env)
      const mockChainClient = createMockChainClient(env)

      // Mock that original transaction is still pending
      mockChainClient.publicClient.getTransactionReceipt.mockRejectedValue(
        new Error('Transaction not found'),
      )
      // Mock original transaction details
      mockChainClient.publicClient.getTransaction.mockResolvedValue({
        to: '0xcontract',
        nonce: 42,
        gasPrice: 1000000000n,
        gas: 1n,
        maxFeePerGas: 2000000000n,
        maxPriorityFeePerGas: 10n,
        value: 0n,
        input: '0x',
      })
      mockChainClient.walletClient.sendTransaction = vi
        .fn()
        .mockResolvedValue(retryTransactionHash)

      await handleTransactionRetryQueueMessage(message, mockEnv, {
        getChainClient: () => mockChainClient,
        getRecentSendMessage: vi.fn().mockResolvedValue({
          cid: 'bafy2bzacedx',
          gasLimit: 250000,
          gasFeeCap: 3000000000n,
        }),
      })

      const { results: dataSets } = await env.DB.prepare(
        'SELECT terminate_service_tx_hash FROM data_sets WHERE terminate_service_tx_hash = ?',
      )
        .bind(transactionHash)
        .all()

      expect(dataSets).toStrictEqual([])

      const { results: updatedDataSets } = await env.DB.prepare(
        'SELECT terminate_service_tx_hash FROM data_sets WHERE terminate_service_tx_hash = ?',
      )
        .bind(retryTransactionHash)
        .all()

      expect(updatedDataSets).toStrictEqual([
        {
          terminate_service_tx_hash: retryTransactionHash,
        },
      ])

      expect(mockChainClient.walletClient.sendTransaction).toHaveBeenCalledWith(
        {
          account: mockAccount,
          to: '0xcontract',
          value: 0n,
          nonce: 42,
          input: '0x',
          gas: 275000n,
          maxFeePerGas: 3000000000n,
          maxPriorityFeePerGas: 13n,
        },
      )

      expect(mockEnv.TRANSACTION_MONITOR_WORKFLOW.create).toHaveBeenCalledWith({
        id: `transaction-monitor-0xtxhash123-${date.getTime()}`,
        params: {
          transactionHash: '0xtxhash123',
        },
      })
    })

    it('retries pending transaction successfully -- receipt has no block number', async () => {
      const message = {
        transactionHash: '0xoriginalhash123',
      }
      const mockEnv = createMockEnv(env)
      const mockChainClient = createMockChainClient(env)

      // Mock that original transaction is still pending
      mockChainClient.publicClient.getTransactionReceipt.mockReturnValue(
        Promise.resolve({
          blockNumber: 0n,
        }),
      )
      // Mock original transaction details
      mockChainClient.publicClient.getTransaction.mockResolvedValue({
        to: '0xcontract',
        nonce: 42,
        gasPrice: 1000000000n,
        gas: 1n,
        maxFeePerGas: 2000000000n,
        maxPriorityFeePerGas: 10n,
        value: 0n,
        input: '0x',
      })
      mockChainClient.walletClient.sendTransaction = vi
        .fn()
        .mockResolvedValue('0xtxhash123')

      await handleTransactionRetryQueueMessage(message, mockEnv, {
        getChainClient: () => mockChainClient,
        getRecentSendMessage: vi.fn().mockResolvedValue({
          cid: 'bafy2bzacedx',
          gasLimit: 250000,
          gasFeeCap: 3000000000n,
        }),
      })

      expect(mockChainClient.walletClient.sendTransaction).toHaveBeenCalledWith(
        {
          account: mockAccount,
          to: '0xcontract',
          value: 0n,
          nonce: 42,
          input: '0x',
          gas: 275000n, // uses the gas limit from recent message + 10%
          maxFeePerGas: 3000000000n, // uses the gas fee cap from recent message
          maxPriorityFeePerGas: 13n, // increase by 25% + 1 attoFIL
        },
      )

      expect(mockEnv.TRANSACTION_MONITOR_WORKFLOW.create).toHaveBeenCalledWith({
        id: `transaction-monitor-0xtxhash123-${date.getTime()}`,
        params: {
          transactionHash: '0xtxhash123',
        },
      })
    })
  })

  describe('when original transaction is already mined', () => {
    it('skips retry if original transaction is already mined', async () => {
      const message = {
        transactionHash: '0xoriginalhash123',
      }
      const mockEnv = createMockEnv(env)
      const mockChainClient = createMockChainClient(env)

      mockChainClient.publicClient.getTransactionReceipt.mockResolvedValue({
        status: 'success',
        blockNumber: BigInt(123456),
        transactionHash: '0xoriginalhash123',
      })
      mockChainClient.walletClient.sendTransaction = vi.fn()

      await handleTransactionRetryQueueMessage(message, mockEnv, {
        getChainClient: () => mockChainClient,
        getRecentSendMessage: vi.fn(),
      })

      expect(
        mockChainClient.walletClient.sendTransaction,
      ).not.toHaveBeenCalled()
      expect(mockEnv.TRANSACTION_MONITOR_WORKFLOW.create).not.toHaveBeenCalled()
    })
  })

  describe('error handling', () => {
    it('handles transaction retrieval failure', async () => {
      const message = {
        transactionHash: '0xoriginalhash123',
      }
      const mockEnv = createMockEnv(env)
      const mockChainClient = createMockChainClient(env)

      mockChainClient.publicClient.getTransactionReceipt.mockRejectedValue(
        new Error('Transaction not found'),
      )
      const error = new Error('Failed to get original transaction')
      mockChainClient.publicClient.getTransaction.mockRejectedValue(error)
      mockChainClient.walletClient.sendTransaction = vi.fn()

      await expect(
        handleTransactionRetryQueueMessage(message, mockEnv, {
          getChainClient: () => mockChainClient,
          getRecentSendMessage: vi.fn(),
        }),
      ).rejects.toThrow('Failed to get original transaction')
      expect(
        mockChainClient.walletClient.sendTransaction,
      ).not.toHaveBeenCalled()
    })

    it('handles wallet client failure', async () => {
      const message = {
        transactionHash: '0xoriginalhash123',
      }
      const mockEnv = createMockEnv(env)
      const mockChainClient = createMockChainClient(env)

      mockChainClient.publicClient.getTransactionReceipt.mockRejectedValue(
        new Error('Transaction not found'),
      )
      mockChainClient.publicClient.getTransaction.mockResolvedValue({
        to: '0xcontract',
        nonce: 42,
        gasPrice: 1000000000n,
        gas: 1n,
        maxFeePerGas: 2000000000n,
        maxPriorityFeePerGas: 10n,
        value: 0n,
        input: '0x',
      })
      const error = new Error('Failed to send retry transaction')
      mockChainClient.walletClient.sendTransaction = vi
        .fn()
        .mockRejectedValue(error)

      await expect(
        handleTransactionRetryQueueMessage(message, mockEnv, {
          getChainClient: () => mockChainClient,
          getRecentSendMessage: vi.fn().mockResolvedValue({
            cid: 'bafy2bzacedx',
            gasLimit: 250000,
            gasFeeCap: 3000000000n,
          }),
        }),
      ).rejects.toThrow('Failed to send retry transaction')
      expect(mockEnv.TRANSACTION_MONITOR_WORKFLOW.create).not.toHaveBeenCalled()
    })

    it('handles workflow creation failure', async () => {
      const message = {
        transactionHash: '0xoriginalhash123',
      }
      const mockEnv = createMockEnv(env)
      const mockChainClient = createMockChainClient(env)

      mockChainClient.publicClient.getTransactionReceipt.mockRejectedValue(
        new Error('Transaction not found'),
      )
      mockChainClient.publicClient.getTransaction.mockResolvedValue({
        to: '0xcontract',
        nonce: 42,
        gasPrice: 1000000000n,
        gas: 1n,
        maxFeePerGas: 2000000000n,
        maxPriorityFeePerGas: 10n,
        value: 0n,
        input: '0x',
      })
      mockChainClient.walletClient.sendTransaction = vi
        .fn()
        .mockResolvedValue('0xtxhash123')
      const error = new Error('Workflow creation failed')
      mockEnv.TRANSACTION_MONITOR_WORKFLOW.create.mockRejectedValue(error)

      await expect(
        handleTransactionRetryQueueMessage(message, mockEnv, {
          getChainClient: () => mockChainClient,
          getRecentSendMessage: vi.fn().mockResolvedValue({
            cid: 'bafy2bzacedx',
            gasLimit: 250000,
            gasFeeCap: 3000000000n,
          }),
        }),
      ).rejects.toThrow('Workflow creation failed')
    })
  })
})
