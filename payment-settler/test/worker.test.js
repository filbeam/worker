import { describe, it, expect, beforeEach, vi } from 'vitest'
import { env } from 'cloudflare:test'
import { withDataSet, createNextId, getDaysAgo } from './test-helpers.js'
import worker from '../bin/payment-settler.js'

const nextId = createNextId()

describe('payment settler scheduled handler', () => {
  let simulateContractCalls
  let writeContractCalls

  beforeEach(() => {
    vi.resetAllMocks()
  })

  const mockAccount = { address: '0xMockAccountAddress' }

  const mockPublicClient = {
    simulateContract: vi.fn(),
    estimateContractGas: vi.fn(),
  }
  beforeEach(() => {
    mockPublicClient.simulateContract.mockImplementation((params) => {
      simulateContractCalls.push(params)
      return Promise.resolve({
        request: { ...params, mockedRequest: true },
      })
    })
  })
  beforeEach(() => {
    mockPublicClient.estimateContractGas.mockResolvedValue(1_000_000n)
  })

  const mockWalletClient = {
    writeContract: vi.fn(),
  }
  beforeEach(() => {
    mockWalletClient.writeContract.mockImplementation((request) => {
      writeContractCalls.push(request)
      return Promise.resolve('0xMockTransactionHash')
    })
  })

  const mockGetChainClient = vi.fn()
  beforeEach(() => {
    mockGetChainClient.mockReturnValue({
      publicClient: mockPublicClient,
      walletClient: mockWalletClient,
      account: mockAccount,
    })
  })

  const mockWorkflow = {
    create: vi.fn(),
    createBatch: vi.fn(),
  }
  beforeEach(() => {
    mockWorkflow.create.mockResolvedValue(undefined)
    mockWorkflow.createBatch.mockResolvedValue(undefined)
  })

  const mockEnv = {
    ...env,
    SETTLEMENT_BATCH_SIZE: 1,
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
        args: [[expect.any(BigInt)]],
        gas: expect.any(BigInt),
      },
      {
        account: mockAccount,
        abi: expect.any(Array),
        address: '0xTestContractAddress',
        functionName: 'settleCDNPaymentRails',
        args: [[expect.any(BigInt)]],
        gas: expect.any(BigInt),
      },
    ])

    expect(writeContractCalls).toStrictEqual([
      {
        account: mockAccount,
        abi: expect.any(Array),
        address: '0xTestContractAddress',
        functionName: 'settleCDNPaymentRails',
        args: [[expect.any(BigInt)]],
        mockedRequest: true,
        gas: expect.any(BigInt),
      },
      {
        account: mockAccount,
        abi: expect.any(Array),
        address: '0xTestContractAddress',
        functionName: 'settleCDNPaymentRails',
        args: [[expect.any(BigInt)]],
        mockedRequest: true,
        gas: expect.any(BigInt),
      },
    ])

    expect(mockWorkflow.createBatch).toHaveBeenCalledWith([
      {
        id: expect.stringMatching(
          /^payment-settler-0xMockTransactionHash-\d+$/,
        ),
        params: {
          transactionHash: '0xMockTransactionHash',
          metadata: {},
        },
      },
      {
        id: expect.stringMatching(
          /^payment-settler-0xMockTransactionHash-\d+$/,
        ),
        params: {
          transactionHash: '0xMockTransactionHash',
          metadata: {},
        },
      },
    ])
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
        args: [[expect.any(BigInt)]],
        gas: expect.any(BigInt),
      },
      {
        account: mockAccount,
        abi: expect.any(Array),
        address: '0xTestContractAddress',
        functionName: 'settleCDNPaymentRails',
        args: [[expect.any(BigInt)]],
        gas: expect.any(BigInt),
      },
    ])
    expect(writeContractCalls).toStrictEqual([
      {
        account: mockAccount,
        abi: expect.any(Array),
        address: '0xTestContractAddress',
        functionName: 'settleCDNPaymentRails',
        args: [[expect.any(BigInt)]],
        mockedRequest: true,
        gas: expect.any(BigInt),
      },
      {
        account: mockAccount,
        abi: expect.any(Array),
        address: '0xTestContractAddress',
        functionName: 'settleCDNPaymentRails',
        args: [[expect.any(BigInt)]],
        mockedRequest: true,
        gas: expect.any(BigInt),
      },
    ])
  })

  it('should log error and continue when contract simulation fails', async () => {
    const id1 = nextId()
    await withDataSet(env, {
      id: id1,
      withCDN: true,
      usageReportedUntil: getDaysAgo(5),
    })

    const simulationError = new Error('Contract simulation failed')
    simulationError.cause = { reason: 'Insufficient balance' }

    mockPublicClient.simulateContract.mockRejectedValue(simulationError)

    // Should not throw - errors are logged
    await worker.scheduled(
      undefined,
      mockEnv,
      {},
      { getChainClient: mockGetChainClient },
    )

    // No transactions should have been written
    expect(writeContractCalls).toStrictEqual([])
    // No workflow should have been created
    expect(mockWorkflow.createBatch).not.toHaveBeenCalled()
  })

  it('should log error and continue when write contract fails', async () => {
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

    // Should not throw - errors are logged
    await worker.scheduled(
      undefined,
      mockEnv,
      {},
      { getChainClient: mockGetChainClient },
    )

    // Simulation was attempted
    expect(simulateContractCalls).toHaveLength(1)
    // No workflow should have been created since write failed
    expect(mockWorkflow.createBatch).not.toHaveBeenCalled()
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

  it('should set gas limit to 120% of estimated gas', async () => {
    const id1 = nextId()
    await withDataSet(env, {
      id: id1,
      withCDN: true,
      usageReportedUntil: getDaysAgo(5),
    })

    mockPublicClient.estimateContractGas.mockResolvedValue(1_000_000n)

    await worker.scheduled(
      undefined,
      mockEnv,
      {},
      { getChainClient: mockGetChainClient },
    )

    expect(simulateContractCalls[0].gas).toBe(1_200_000n) // 120% of 1,000,000
    expect(writeContractCalls[0].gas).toBe(1_200_000n)
  })

  it('should continue settling other batches when one batch fails', async () => {
    const id1 = nextId()
    const id2 = nextId()
    const id3 = nextId()

    // Seed three data sets - with SETTLEMENT_BATCH_SIZE=1, each gets its own batch
    await withDataSet(env, {
      id: id1,
      withCDN: true,
      usageReportedUntil: getDaysAgo(5),
    })
    await withDataSet(env, {
      id: id2,
      withCDN: true,
      usageReportedUntil: getDaysAgo(6),
    })
    await withDataSet(env, {
      id: id3,
      withCDN: true,
      usageReportedUntil: getDaysAgo(7),
    })

    // Make the second call fail
    let callCount = 0
    mockPublicClient.simulateContract.mockImplementation((params) => {
      simulateContractCalls.push(params)
      callCount++
      if (callCount === 2) {
        return Promise.reject(new Error('out of gas'))
      }
      return Promise.resolve({
        request: { ...params, mockedRequest: true },
      })
    })

    // Should not throw - errors are logged and other batches are still processed
    await worker.scheduled(
      undefined,
      mockEnv,
      {},
      { getChainClient: mockGetChainClient },
    )

    // All three simulations were attempted
    expect(simulateContractCalls).toHaveLength(3)

    // Only two successful batches were written
    expect(writeContractCalls).toHaveLength(2)

    // Only two successful transactions were monitored
    expect(mockWorkflow.createBatch).toHaveBeenCalledWith([
      {
        id: expect.stringMatching(
          /^payment-settler-0xMockTransactionHash-\d+$/,
        ),
        params: {
          transactionHash: '0xMockTransactionHash',
          metadata: {},
        },
      },
      {
        id: expect.stringMatching(
          /^payment-settler-0xMockTransactionHash-\d+$/,
        ),
        params: {
          transactionHash: '0xMockTransactionHash',
          metadata: {},
        },
      },
    ])
  })
})
