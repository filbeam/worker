import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { env } from 'cloudflare:test'
import { withDataSet, randomId } from './test-helpers.js'
import worker from '../bin/payment-settler.js'

describe('payment settler scheduled handler', () => {
  let mockPublicClient
  let mockWalletClient
  let mockAccount
  let mockGetChainClient
  let simulateContractCalls
  let writeContractCalls

  beforeEach(async () => {
    // Clear data_sets table before each test
    await env.DB.prepare('DELETE FROM data_sets').run()

    // Reset tracking arrays
    simulateContractCalls = []
    writeContractCalls = []

    // Setup mock account
    mockAccount = { address: '0xMockAccountAddress' }

    // Setup mock public client
    mockPublicClient = {
      getBlockNumber: vi.fn().mockResolvedValue(1000000n),
      simulateContract: vi.fn().mockImplementation((params) => {
        simulateContractCalls.push(params)
        return Promise.resolve({
          request: { ...params, mockedRequest: true },
        })
      }),
    }

    // Setup mock wallet client
    mockWalletClient = {
      writeContract: vi.fn().mockImplementation((request) => {
        writeContractCalls.push(request)
        return Promise.resolve('0xMockTransactionHash')
      }),
    }

    // Setup mock getChainClient function
    mockGetChainClient = vi.fn().mockReturnValue({
      publicClient: mockPublicClient,
      walletClient: mockWalletClient,
      account: mockAccount,
    })
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  it('should successfully settle active data sets', async () => {
    const id1 = randomId()
    const id2 = randomId()

    // Seed active data sets
    await withDataSet(env, { id: id1, withCDN: true })
    await withDataSet(env, { id: id2, withCDN: true })

    // Mock the transaction monitor workflow
    const mockWorkflow = {
      create: vi.fn().mockResolvedValue(undefined),
    }

    // Create a test environment
    const testEnv = {
      ...env,
      FILBEAM_CONTRACT_ADDRESS: '0xTestContractAddress',
      FILBEAM_CONTROLLER_ADDRESS_PRIVATE_KEY:
        '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
      RPC_URL: 'https://test.rpc.url',
      ENVIRONMENT: 'dev',
      TRANSACTION_MONITOR_WORKFLOW: mockWorkflow,
    }

    // Run the scheduled handler with mock
    await worker.scheduled(
      undefined,
      testEnv,
      {},
      { getChainClient: mockGetChainClient },
    )

    // Verify chain client was called
    expect(mockGetChainClient).toHaveBeenCalledWith(testEnv)

    // Verify block number was fetched
    expect(mockPublicClient.getBlockNumber).toHaveBeenCalled()

    // Verify contract simulation was called
    expect(simulateContractCalls).toHaveLength(1)
    expect(simulateContractCalls[0]).toMatchObject({
      account: mockAccount,
      abi: expect.any(Array),
      address: '0xTestContractAddress',
      functionName: 'settleCDNPaymentRailBatch',
      args: [expect.arrayContaining([expect.any(BigInt), expect.any(BigInt)])],
    })

    // Verify transaction was sent
    expect(writeContractCalls).toHaveLength(1)
    expect(writeContractCalls[0]).toMatchObject({
      mockedRequest: true,
    })

    // Verify workflow was started
    expect(mockWorkflow.create).toHaveBeenCalledWith({
      id: expect.stringMatching(
        /^settlement-tx-monitor-0xMockTransactionHash-\d+$/,
      ),
      params: {
        transactionHash: '0xMockTransactionHash',
        metadata: {
          retryData: {},
        },
      },
    })
  })

  it('should handle no active data sets gracefully', async () => {
    // Seed only inactive data set
    const id1 = randomId()
    await withDataSet(env, { id: id1, withCDN: false })

    // Create a test environment
    const testEnv = {
      ...env,
      FILBEAM_CONTRACT_ADDRESS: '0xTestContractAddress',
      FILBEAM_CONTROLLER_ADDRESS_PRIVATE_KEY:
        '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
      RPC_URL: 'https://test.rpc.url',
      ENVIRONMENT: 'dev',
    }

    // Run the scheduled handler with mock
    await worker.scheduled(
      undefined,
      testEnv,
      {},
      { getChainClient: mockGetChainClient },
    )

    // Verify block number was fetched
    expect(mockPublicClient.getBlockNumber).toHaveBeenCalled()

    // Verify no contract simulation was called
    expect(simulateContractCalls).toHaveLength(0)
    expect(writeContractCalls).toHaveLength(0)
  })

  it('should handle terminated data sets within settlement window', async () => {
    const id1 = randomId()
    const id2 = randomId()
    const currentEpoch = 1000000n

    // Seed data sets
    await withDataSet(env, { id: id1, withCDN: true })
    await withDataSet(env, {
      id: id2,
      withCDN: false,
      settleUpToEpoch: currentEpoch + 100n,
    })

    // Mock the transaction monitor workflow
    const mockWorkflow = {
      create: vi.fn().mockResolvedValue(undefined),
    }

    // Create a test environment
    const testEnv = {
      ...env,
      FILBEAM_CONTRACT_ADDRESS: '0xTestContractAddress',
      FILBEAM_CONTROLLER_ADDRESS_PRIVATE_KEY:
        '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
      RPC_URL: 'https://test.rpc.url',
      ENVIRONMENT: 'dev',
      TRANSACTION_MONITOR_WORKFLOW: mockWorkflow,
    }

    // Run the scheduled handler with mock
    await worker.scheduled(
      undefined,
      testEnv,
      {},
      { getChainClient: mockGetChainClient },
    )

    // Verify contract simulation and transaction
    expect(simulateContractCalls).toHaveLength(1)
    expect(writeContractCalls).toHaveLength(1)
  })

  it('should handle contract simulation errors', async () => {
    const id1 = randomId()
    await withDataSet(env, { id: id1, withCDN: true })

    // Mock console.error to capture output
    const consoleErrorSpy = vi.spyOn(console, 'error')

    const simulationError = new Error('Contract simulation failed')
    simulationError.cause = { reason: 'Insufficient balance' }

    // Override mock to throw error
    mockPublicClient.simulateContract.mockRejectedValue(simulationError)

    // Create a test environment
    const testEnv = {
      ...env,
      FILBEAM_CONTRACT_ADDRESS: '0xTestContractAddress',
      FILBEAM_CONTROLLER_ADDRESS_PRIVATE_KEY:
        '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
      RPC_URL: 'https://test.rpc.url',
      ENVIRONMENT: 'dev',
    }

    // Run the scheduled handler and expect it to throw
    await expect(
      worker.scheduled(
        undefined,
        testEnv,
        {},
        { getChainClient: mockGetChainClient },
      ),
    ).rejects.toThrow('Contract simulation failed')

    // Verify error logging
    expect(consoleErrorSpy).toHaveBeenCalledWith(
      'Settlement process failed:',
      simulationError,
    )
    expect(consoleErrorSpy).toHaveBeenCalledWith(
      'Contract revert reason:',
      'Insufficient balance',
    )
  })

  it('should handle write contract errors', async () => {
    const id1 = randomId()
    await withDataSet(env, { id: id1, withCDN: true })

    // Mock console.error to capture output
    const consoleErrorSpy = vi.spyOn(console, 'error')

    const writeError = new Error('Transaction failed')

    // Reset simulateContract to default behavior and override writeContract to throw
    mockPublicClient.simulateContract.mockImplementation((params) => {
      simulateContractCalls.push(params)
      return Promise.resolve({
        request: { ...params, mockedRequest: true },
      })
    })
    mockWalletClient.writeContract.mockRejectedValue(writeError)

    // Create a test environment
    const testEnv = {
      ...env,
      FILBEAM_CONTRACT_ADDRESS: '0xTestContractAddress',
      FILBEAM_CONTROLLER_ADDRESS_PRIVATE_KEY:
        '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
      RPC_URL: 'https://test.rpc.url',
      ENVIRONMENT: 'dev',
    }

    // Run the scheduled handler and expect it to throw
    await expect(
      worker.scheduled(
        undefined,
        testEnv,
        {},
        { getChainClient: mockGetChainClient },
      ),
    ).rejects.toThrow('Transaction failed')

    // Verify error logging
    expect(consoleErrorSpy).toHaveBeenCalledWith(
      'Settlement process failed:',
      writeError,
    )
  })

  it('should handle mainnet environment correctly', async () => {
    const id1 = randomId()
    await withDataSet(env, { id: id1, withCDN: true })

    // Override block number for mainnet
    mockPublicClient.getBlockNumber.mockResolvedValue(2000000n)

    // Reset mocks to default behavior
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

    // Mock the transaction monitor workflow
    const mockWorkflow = {
      create: vi.fn().mockResolvedValue(undefined),
    }

    // Create a mainnet test environment
    const testEnv = {
      ...env,
      FILBEAM_CONTRACT_ADDRESS: '0x1234567890abcdef1234567890abcdef12345678',
      FILBEAM_CONTROLLER_ADDRESS_PRIVATE_KEY:
        '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
      RPC_URL: 'https://api.node.glif.io/',
      ENVIRONMENT: 'mainnet',
      TRANSACTION_MONITOR_WORKFLOW: mockWorkflow,
    }

    // Run the scheduled handler with mock
    await worker.scheduled(
      undefined,
      testEnv,
      {},
      { getChainClient: mockGetChainClient },
    )
  })

  it('should handle empty database correctly', async () => {
    // No data sets seeded - database is empty

    // Override block number for calibration
    mockPublicClient.getBlockNumber.mockResolvedValue(1500000n)

    // Create a test environment
    const testEnv = {
      ...env,
      FILBEAM_CONTRACT_ADDRESS: '0xTestContractAddress',
      FILBEAM_CONTROLLER_ADDRESS_PRIVATE_KEY:
        '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
      RPC_URL: 'https://test.rpc.url',
      ENVIRONMENT: 'calibration',
    }

    // Run the scheduled handler with mock
    await worker.scheduled(
      undefined,
      testEnv,
      {},
      { getChainClient: mockGetChainClient },
    )

    // Verify no contract interactions
    expect(simulateContractCalls).toHaveLength(0)
    expect(writeContractCalls).toHaveLength(0)
  })
})
