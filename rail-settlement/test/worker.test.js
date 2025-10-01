import { describe, it, expect, beforeEach, vi } from 'vitest'
import { env } from 'cloudflare:test'
import { withDataSet, randomId } from './test-helpers.js'
import worker from '../bin/rail-settlement.js'

describe('rail settlement scheduled handler', () => {
  beforeEach(async () => {
    // Clear data_sets table before each test
    await env.DB.prepare('DELETE FROM data_sets').run()
    // Reset all mocks
    vi.clearAllMocks()
    // Reset console mocks
    vi.restoreAllMocks()
  })

  it('should successfully settle active data sets', async () => {
    const id1 = randomId()
    const id2 = randomId()

    // Seed active data sets
    await withDataSet(env, { id: id1, withCDN: true })
    await withDataSet(env, { id: id2, withCDN: true })

    // Mock console.log to capture output
    const consoleLogSpy = vi.spyOn(console, 'log')

    // Mock the chain client
    const mockHash = '0xTransactionHash123'
    const mockPublicClient = {
      getBlockNumber: vi.fn().mockResolvedValue(1000000n),
      simulateContract: vi.fn().mockResolvedValue({
        request: { functionName: 'settleCDNPaymentRailBatch' },
      }),
    }
    const mockWalletClient = {
      writeContract: vi.fn().mockResolvedValue(mockHash),
    }
    const mockAccount = { address: '0xTestAccount' }

    const mockGetChainClient = vi.fn().mockReturnValue({
      publicClient: mockPublicClient,
      walletClient: mockWalletClient,
      account: mockAccount,
    })

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

    // Verify chain client was called
    expect(mockGetChainClient).toHaveBeenCalledWith(testEnv)

    // Verify block number was fetched
    expect(mockPublicClient.getBlockNumber).toHaveBeenCalled()

    // Verify contract simulation
    expect(mockPublicClient.simulateContract).toHaveBeenCalledWith({
      account: mockAccount,
      abi: expect.any(Array),
      address: '0xTestContractAddress',
      functionName: 'settleCDNPaymentRailBatch',
      args: [expect.arrayContaining([expect.any(BigInt), expect.any(BigInt)])],
    })

    // Verify transaction was sent
    expect(mockWalletClient.writeContract).toHaveBeenCalled()

    // Verify the logs
    expect(consoleLogSpy).toHaveBeenCalledWith(
      'Starting rail settlement worker',
    )
    expect(consoleLogSpy).toHaveBeenCalledWith('Current epoch: 1000000')
    expect(consoleLogSpy).toHaveBeenCalledWith(
      'Found 2 data sets for settlement:',
      expect.arrayContaining([id1, id2]),
    )
    expect(consoleLogSpy).toHaveBeenCalledWith(
      `Settlement transaction sent: ${mockHash}`,
    )
    expect(consoleLogSpy).toHaveBeenCalledWith('Settled 2 data sets')
  })

  it('should handle no active data sets gracefully', async () => {
    // Seed only inactive data set
    const id1 = randomId()
    await withDataSet(env, { id: id1, withCDN: false })

    // Mock console.log to capture output
    const consoleLogSpy = vi.spyOn(console, 'log')

    // Mock the chain client
    const mockPublicClient = {
      getBlockNumber: vi.fn().mockResolvedValue(1000000n),
    }
    const mockWalletClient = {}
    const mockAccount = { address: '0xTestAccount' }

    const mockGetChainClient = vi.fn().mockReturnValue({
      publicClient: mockPublicClient,
      walletClient: mockWalletClient,
      account: mockAccount,
    })

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

    // Verify the logs
    expect(consoleLogSpy).toHaveBeenCalledWith(
      'Starting rail settlement worker',
    )
    expect(consoleLogSpy).toHaveBeenCalledWith('Current epoch: 1000000')
    expect(consoleLogSpy).toHaveBeenCalledWith(
      'No active data sets found for settlement',
    )
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

    // Mock console.log to capture output
    const consoleLogSpy = vi.spyOn(console, 'log')

    // Mock the chain client
    const mockHash = '0xTransactionHash456'
    const mockPublicClient = {
      getBlockNumber: vi.fn().mockResolvedValue(currentEpoch),
      simulateContract: vi.fn().mockResolvedValue({
        request: { functionName: 'settleCDNPaymentRailBatch' },
      }),
    }
    const mockWalletClient = {
      writeContract: vi.fn().mockResolvedValue(mockHash),
    }
    const mockAccount = { address: '0xTestAccount' }

    const mockGetChainClient = vi.fn().mockReturnValue({
      publicClient: mockPublicClient,
      walletClient: mockWalletClient,
      account: mockAccount,
    })

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

    // Verify both data sets are settled
    expect(consoleLogSpy).toHaveBeenCalledWith(
      'Found 2 data sets for settlement:',
      expect.arrayContaining([id1, id2]),
    )
    expect(consoleLogSpy).toHaveBeenCalledWith('Settled 2 data sets')
  })

  it('should handle contract simulation errors', async () => {
    const id1 = randomId()
    await withDataSet(env, { id: id1, withCDN: true })

    // Mock console.error to capture output
    const consoleErrorSpy = vi.spyOn(console, 'error')

    const simulationError = new Error('Contract simulation failed')
    simulationError.cause = { reason: 'Insufficient balance' }

    // Mock the chain client with error
    const mockPublicClient = {
      getBlockNumber: vi.fn().mockResolvedValue(1000000n),
      simulateContract: vi.fn().mockRejectedValue(simulationError),
    }
    const mockWalletClient = {}
    const mockAccount = { address: '0xTestAccount' }

    const mockGetChainClient = vi.fn().mockReturnValue({
      publicClient: mockPublicClient,
      walletClient: mockWalletClient,
      account: mockAccount,
    })

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

    // Mock the chain client with write error
    const mockPublicClient = {
      getBlockNumber: vi.fn().mockResolvedValue(1000000n),
      simulateContract: vi.fn().mockResolvedValue({
        request: { functionName: 'settleCDNPaymentRailBatch' },
      }),
    }
    const mockWalletClient = {
      writeContract: vi.fn().mockRejectedValue(writeError),
    }
    const mockAccount = { address: '0xTestAccount' }

    const mockGetChainClient = vi.fn().mockReturnValue({
      publicClient: mockPublicClient,
      walletClient: mockWalletClient,
      account: mockAccount,
    })

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

    // Mock console.log to capture output
    const consoleLogSpy = vi.spyOn(console, 'log')

    // Mock the chain client for mainnet
    const mockHash =
      '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
    const mockPublicClient = {
      getBlockNumber: vi.fn().mockResolvedValue(2000000n),
      simulateContract: vi.fn().mockResolvedValue({
        request: { functionName: 'settleCDNPaymentRailBatch' },
      }),
    }
    const mockWalletClient = {
      writeContract: vi.fn().mockResolvedValue(mockHash),
    }
    const mockAccount = {
      address: '0x1234567890abcdef1234567890abcdef12345678',
    }

    const mockGetChainClient = vi.fn().mockReturnValue({
      publicClient: mockPublicClient,
      walletClient: mockWalletClient,
      account: mockAccount,
    })

    // Create a mainnet test environment
    const testEnv = {
      ...env,
      FILBEAM_CONTRACT_ADDRESS: '0x1234567890abcdef1234567890abcdef12345678',
      FILBEAM_CONTROLLER_ADDRESS_PRIVATE_KEY:
        '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
      RPC_URL: 'https://api.node.glif.io/',
      ENVIRONMENT: 'mainnet',
    }

    // Run the scheduled handler with mock
    await worker.scheduled(
      undefined,
      testEnv,
      {},
      { getChainClient: mockGetChainClient },
    )

    // Verify mainnet-specific behavior
    expect(consoleLogSpy).toHaveBeenCalledWith('Current epoch: 2000000')
    expect(consoleLogSpy).toHaveBeenCalledWith(
      `Settlement transaction sent: ${mockHash}`,
    )
  })

  it('should handle empty database correctly', async () => {
    // No data sets seeded - database is empty

    // Mock console.log to capture output
    const consoleLogSpy = vi.spyOn(console, 'log')

    // Mock the chain client
    const mockPublicClient = {
      getBlockNumber: vi.fn().mockResolvedValue(1500000n),
    }
    const mockWalletClient = {}
    const mockAccount = { address: '0xTestAccount' }

    const mockGetChainClient = vi.fn().mockReturnValue({
      publicClient: mockPublicClient,
      walletClient: mockWalletClient,
      account: mockAccount,
    })

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

    // Verify the logs
    expect(consoleLogSpy).toHaveBeenCalledWith(
      'Starting rail settlement worker',
    )
    expect(consoleLogSpy).toHaveBeenCalledWith('Current epoch: 1500000')
    expect(consoleLogSpy).toHaveBeenCalledWith(
      'No active data sets found for settlement',
    )
  })
})
