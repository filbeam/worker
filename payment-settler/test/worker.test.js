import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { env } from 'cloudflare:test'
import { withDataSet, createNextId, getDaysAgo } from './test-helpers.js'
import worker from '../bin/payment-settler.js'

const nextId = createNextId()

describe('payment settler scheduled handler', () => {
  let simulateContractCalls
  let writeContractCalls

  const mockAccount = { address: '0xMockAccountAddress' }

  const mockPublicClient = {
    simulateContract: vi.fn().mockImplementation((params) => {
      simulateContractCalls.push(params)
      return Promise.resolve({
        request: { ...params, mockedRequest: true },
      })
    }),
  }

  const mockWalletClient = {
    writeContract: vi.fn().mockImplementation((request) => {
      writeContractCalls.push(request)
      return Promise.resolve('0xMockTransactionHash')
    }),
  }

  const mockGetChainClient = vi.fn().mockReturnValue({
    publicClient: mockPublicClient,
    walletClient: mockWalletClient,
    account: mockAccount,
  })

  const mockWorkflow = {
    create: vi.fn().mockResolvedValue(undefined),
  }

  const mockEnv = {
    ...env,
    FILBEAM_OPERATOR_CONTRACT_ADDRESS: '0xTestContractAddress',
    FILBEAM_OPERATOR_PAYMENT_SETTLER_PRIVATE_KEY:
      '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
    TRANSACTION_MONITOR_WORKFLOW: mockWorkflow,
  }

  beforeEach(async () => {
    simulateContractCalls = []
    writeContractCalls = []

    await env.DB.prepare('DELETE FROM data_sets').run()
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  it('should successfully settle active data sets', async () => {
    const id1 = nextId()
    const id2 = nextId()

    // Seed active data sets with recent usage
    await withDataSet(env, {
      id: id1,
      withCDN: true,
      usageReportedUntil: getDaysAgo(5),
    })
    await withDataSet(env, {
      id: id2,
      withCDN: true,
      usageReportedUntil: getDaysAgo(10),
    })

    await worker.scheduled(
      undefined,
      mockEnv,
      {},
      { getChainClient: mockGetChainClient },
    )

    expect(mockGetChainClient).toHaveBeenCalledWith(mockEnv)

    expect(simulateContractCalls).toStrictEqual([
      {
        account: mockAccount,
        abi: expect.any(Array),
        address: '0xTestContractAddress',
        functionName: 'settleCDNPaymentRails',
        args: [[expect.any(BigInt), expect.any(BigInt)]],
      },
    ])

    expect(writeContractCalls).toStrictEqual([
      {
        account: mockAccount,
        abi: expect.any(Array),
        address: '0xTestContractAddress',
        functionName: 'settleCDNPaymentRails',
        args: [[expect.any(BigInt), expect.any(BigInt)]],
        mockedRequest: true,
      },
    ])

    expect(mockWorkflow.create).toHaveBeenCalledWith({
      id: expect.stringMatching(/^payment-settler-0xMockTransactionHash-\d+$/),
      params: {
        transactionHash: '0xMockTransactionHash',
        metadata: {
          retryData: {},
        },
      },
    })
  })

  it('should handle no active data sets gracefully', async () => {
    const id1 = nextId()
    await withDataSet(env, {
      id: id1,
      withCDN: false,
      usageReportedUntil: getDaysAgo(5),
    })

    await worker.scheduled(
      undefined,
      mockEnv,
      {},
      { getChainClient: mockGetChainClient },
    )

    expect(simulateContractCalls).toStrictEqual([])
    expect(writeContractCalls).toStrictEqual([])
  })

  it('should handle terminated data sets within settlement window', async () => {
    const id1 = nextId()
    const id2 = nextId()

    await withDataSet(env, {
      id: id1,
      withCDN: true,
      usageReportedUntil: getDaysAgo(7),
    })
    await withDataSet(env, {
      id: id2,
      withCDN: false,
      lockupUnlocksAt: getDaysAgo(-10),
      usageReportedUntil: getDaysAgo(3),
    })

    await worker.scheduled(
      undefined,
      mockEnv,
      {},
      { getChainClient: mockGetChainClient },
    )

    expect(simulateContractCalls).toStrictEqual([
      {
        account: mockAccount,
        abi: expect.any(Array),
        address: '0xTestContractAddress',
        functionName: 'settleCDNPaymentRails',
        args: [[expect.any(BigInt), expect.any(BigInt)]],
      },
    ])
    expect(writeContractCalls).toStrictEqual([
      {
        account: mockAccount,
        abi: expect.any(Array),
        address: '0xTestContractAddress',
        functionName: 'settleCDNPaymentRails',
        args: [[expect.any(BigInt), expect.any(BigInt)]],
        mockedRequest: true,
      },
    ])
  })

  it('should handle contract simulation errors', async () => {
    const id1 = nextId()
    await withDataSet(env, {
      id: id1,
      withCDN: true,
      usageReportedUntil: getDaysAgo(5),
    })

    const simulationError = new Error('Contract simulation failed')
    simulationError.cause = { reason: 'Insufficient balance' }

    mockPublicClient.simulateContract.mockRejectedValue(simulationError)

    await expect(
      worker.scheduled(
        undefined,
        mockEnv,
        {},
        { getChainClient: mockGetChainClient },
      ),
    ).rejects.toThrow('Contract simulation failed')
  })

  it('should handle write contract errors', async () => {
    const id1 = nextId()
    await withDataSet(env, {
      id: id1,
      withCDN: true,
      usageReportedUntil: getDaysAgo(5),
    })

    const writeError = new Error('Transaction failed')

    mockPublicClient.simulateContract.mockImplementation((params) => {
      simulateContractCalls.push(params)
      return Promise.resolve({
        request: { ...params, mockedRequest: true },
      })
    })
    mockWalletClient.writeContract.mockRejectedValue(writeError)

    await expect(
      worker.scheduled(
        undefined,
        mockEnv,
        {},
        { getChainClient: mockGetChainClient },
      ),
    ).rejects.toThrow('Transaction failed')
  })

  it('should handle mainnet environment correctly', async () => {
    const id1 = nextId()
    await withDataSet(env, {
      id: id1,
      withCDN: true,
      usageReportedUntil: getDaysAgo(5),
    })

    mockPublicClient.simulateContract.mockImplementation((params) => {
      simulateContractCalls.push(params)
      return Promise.resolve({
        request: { ...params, mockedRequest: true },
      })
    })
    mockWalletClient.writeContract.mockImplementation((request) => {
      writeContractCalls.push(request)
      return Promise.resolve(
        '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
      )
    })

    await worker.scheduled(
      undefined,
      mockEnv,
      {},
      { getChainClient: mockGetChainClient },
    )
  })

  it('should handle empty database correctly', async () => {
    await worker.scheduled(
      undefined,
      mockEnv,
      {},
      { getChainClient: mockGetChainClient },
    )

    expect(simulateContractCalls).toStrictEqual([])
    expect(writeContractCalls).toStrictEqual([])
  })
})
