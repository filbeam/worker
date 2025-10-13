import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { applyD1Migrations, env as testEnv } from 'cloudflare:test'
import {
  withDataSet,
  withRetrievalLog,
  filecoinEpochToTimestamp,
  FILECOIN_GENESIS_UNIX_TIMESTAMP,
} from './test-helpers.js'
import { epochToTimestamp } from '../lib/rollup.js'
import worker from '../bin/usage-reporter.js'

describe('rollup worker scheduled entrypoint', () => {
  let env
  let mockGetChainClient
  let mockPublicClient
  let mockWalletClient
  let mockAccount
  let simulateContractCalls
  let writeContractCalls
  let mockWorkflow

  beforeEach(async () => {
    simulateContractCalls = []
    writeContractCalls = []

    mockWorkflow = {
      create: vi.fn().mockResolvedValue(undefined),
    }

    env = {
      ...testEnv,
      ENVIRONMENT: 'dev',
      RPC_URL: 'https://mock-rpc.example.com',
      FILBEAM_CONTRACT_ADDRESS: '0xMockFilBeamAddress',
      FILBEAM_CONTROLLER_PRIVATE_KEY: '0xMockPrivateKey',
      GENESIS_BLOCK_TIMESTAMP: '1598306400',
      TRANSACTION_MONITOR_WORKFLOW: mockWorkflow,
    }

    await applyD1Migrations(env.DB, env.TEST_MIGRATIONS)

    // Create mock chain client
    mockAccount = { address: '0xMockAccountAddress' }

    mockPublicClient = {
      getBlockNumber: vi.fn().mockResolvedValue(101n),
      simulateContract: vi.fn().mockImplementation((params) => {
        simulateContractCalls.push(params)
        return Promise.resolve({
          request: { ...params, mockedRequest: true },
        })
      }),
    }

    mockWalletClient = {
      writeContract: vi.fn().mockImplementation((request) => {
        writeContractCalls.push(request)
        return Promise.resolve('0xMockTransactionHash')
      }),
    }

    mockGetChainClient = vi.fn().mockReturnValue({
      publicClient: mockPublicClient,
      walletClient: mockWalletClient,
      account: mockAccount,
    })
  })

  afterEach(async () => {
    await env.DB.exec('DELETE FROM retrieval_logs;')
    await env.DB.exec('DELETE FROM data_sets;')
    vi.clearAllMocks()
  })

  it('should report usage data for multiple datasets', async () => {
    // Setup: Create datasets with usage data
    const epoch99Timestamp = epochToTimestamp(
      99n,
      BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
    )
    const epoch98Timestamp = epochToTimestamp(
      98n,
      BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
    )
    await withDataSet(env, { id: '1', usageReportedUntil: epoch99Timestamp })
    await withDataSet(env, { id: '2', usageReportedUntil: epoch98Timestamp })

    const epoch100Timestamp = filecoinEpochToTimestamp(100)

    // Add retrieval logs for dataset 1
    await withRetrievalLog(env, {
      timestamp: epoch100Timestamp,
      dataSetId: '1',
      egressBytes: 2000,
      cacheMiss: 0,
    })
    await withRetrievalLog(env, {
      timestamp: epoch100Timestamp,
      dataSetId: '1',
      egressBytes: 500,
      cacheMiss: 1,
    })

    // Add retrieval logs for dataset 2
    await withRetrievalLog(env, {
      timestamp: epoch100Timestamp,
      dataSetId: '2',
      egressBytes: 3000,
      cacheMiss: 0,
    })
    await withRetrievalLog(env, {
      timestamp: epoch100Timestamp,
      dataSetId: '2',
      egressBytes: 1000,
      cacheMiss: 1,
    })

    // Execute scheduled function with mocked chain client
    await worker.scheduled(null, env, null, {
      getChainClient: mockGetChainClient,
    })

    // Verify chain client was created with correct environment
    expect(mockGetChainClient).toHaveBeenCalledWith(env)

    // Verify block number was fetched
    expect(mockPublicClient.getBlockNumber).toHaveBeenCalled()

    // Verify contract simulation was called with correct parameters
    expect(simulateContractCalls).toHaveLength(1)
    const simulateCall = simulateContractCalls[0]
    expect(simulateCall.address).toBe(env.FILBEAM_CONTRACT_ADDRESS)
    expect(simulateCall.functionName).toBe('recordUsageRollups')
    expect(simulateCall.args[0]).toEqual(['1', '2']) // dataSetIds
    expect(simulateCall.args[1]).toEqual([100, 100]) // epochs
    expect(simulateCall.args[2]).toEqual([2500n, 4000n]) // cdnBytesUsed (all egress)
    expect(simulateCall.args[3]).toEqual([500n, 1000n]) // cacheMissBytesUsed

    // Verify transaction was written and workflow started
    expect(writeContractCalls).toHaveLength(1)
    expect(mockWorkflow.create).toHaveBeenCalled()
  })

  it('should handle when no usage data exists', async () => {
    // Setup: Create dataset but with no retrieval logs
    const epoch99Timestamp = epochToTimestamp(
      99n,
      BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
    )
    await withDataSet(env, { id: '1', usageReportedUntil: epoch99Timestamp })

    // Execute scheduled function
    await worker.scheduled(null, env, null, {
      getChainClient: mockGetChainClient,
    })

    // Verify chain client was created and block number fetched
    expect(mockGetChainClient).toHaveBeenCalledWith(env)
    expect(mockPublicClient.getBlockNumber).toHaveBeenCalled()

    // Verify no contract calls were made
    expect(simulateContractCalls).toHaveLength(0)
    expect(writeContractCalls).toHaveLength(0)
  })

  it('should filter out datasets with zero usage', async () => {
    // Setup: Create datasets with mixed usage
    const epoch99Timestamp = epochToTimestamp(
      99n,
      BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
    )
    await withDataSet(env, { id: '1', usageReportedUntil: epoch99Timestamp })
    await withDataSet(env, { id: '2', usageReportedUntil: epoch99Timestamp })
    await withDataSet(env, { id: '3', usageReportedUntil: epoch99Timestamp })

    const epoch100Timestamp = filecoinEpochToTimestamp(100)

    // Dataset 1: Has usage
    await withRetrievalLog(env, {
      timestamp: epoch100Timestamp,
      dataSetId: '1',
      egressBytes: 1000,
      cacheMiss: 0,
    })

    // Dataset 2: No retrieval logs (zero usage)

    // Dataset 3: Has usage
    await withRetrievalLog(env, {
      timestamp: epoch100Timestamp,
      dataSetId: '3',
      egressBytes: 2000,
      cacheMiss: 1,
    })

    // Execute scheduled function
    await worker.scheduled(null, env, null, {
      getChainClient: mockGetChainClient,
    })

    // Verify only datasets with non-zero usage are reported
    expect(simulateContractCalls).toHaveLength(1)
    const simulateCall = simulateContractCalls[0]
    expect(simulateCall.args[0]).toEqual(['1', '3']) // Only datasets 1 and 3
    expect(simulateCall.args[1]).toEqual([100, 100])
    expect(simulateCall.args[2]).toEqual([1000n, 2000n]) // cdnBytesUsed (all egress)
    expect(simulateCall.args[3]).toEqual([0n, 2000n]) // cacheMissBytesUsed
  })

  it('should not report datasets that are already up to date', async () => {
    // Setup: Create datasets with different last_reported_epoch values
    // Set usage_reported_until to timestamp for epoch 99 and 100
    const epoch99Timestamp = epochToTimestamp(
      99n,
      BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
    )
    const epoch100TimestampISO = epochToTimestamp(
      100n,
      BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
    )

    await withDataSet(env, { id: '1', usageReportedUntil: epoch99Timestamp }) // Should be included
    await withDataSet(env, {
      id: '2',
      usageReportedUntil: epoch100TimestampISO,
    }) // Should NOT be included (already reported)

    const epoch100Timestamp = filecoinEpochToTimestamp(100)

    // Add retrieval logs for both datasets
    for (const id of ['1', '2']) {
      await withRetrievalLog(env, {
        timestamp: epoch100Timestamp,
        dataSetId: id,
        egressBytes: 1000,
        cacheMiss: 0,
      })
    }

    // Execute scheduled function
    await worker.scheduled(null, env, null, {
      getChainClient: mockGetChainClient,
    })

    // Verify only dataset 1 is reported (dataset 2 is already up to date)
    expect(simulateContractCalls).toHaveLength(1)
    const simulateCall = simulateContractCalls[0]
    expect(simulateCall.args[0]).toEqual(['1']) // Only dataset 1
  })

  it('should calculate correct target epoch', async () => {
    // Setup: Mock different block numbers to verify epoch calculation
    mockPublicClient.getBlockNumber.mockResolvedValue(105n)

    const epoch99Timestamp = epochToTimestamp(
      99n,
      BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
    )
    await withDataSet(env, { id: '1', usageReportedUntil: epoch99Timestamp })

    // Add logs for multiple epochs
    for (let epoch = 100; epoch <= 104; epoch++) {
      await withRetrievalLog(env, {
        timestamp: filecoinEpochToTimestamp(epoch),
        dataSetId: '1',
        egressBytes: 1000,
        cacheMiss: 0,
      })
    }

    // Execute scheduled function
    await worker.scheduled(null, env, null, {
      getChainClient: mockGetChainClient,
    })

    // Verify it reports up to epoch 104 (currentEpoch - 1)
    expect(simulateContractCalls).toHaveLength(1)
    const simulateCall = simulateContractCalls[0]
    expect(simulateCall.args[1]).toEqual([104]) // max_epoch should be 104
    expect(simulateCall.args[2]).toEqual([5000n]) // 5 epochs × 1000 bytes
  })

  it('should aggregate usage correctly across epochs', async () => {
    // Setup: Create dataset with usage across multiple epochs
    const epoch95Timestamp = epochToTimestamp(
      95n,
      BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
    )
    await withDataSet(env, { id: '1', usageReportedUntil: epoch95Timestamp })

    // Add retrieval logs across epochs 96-100
    for (let epoch = 96; epoch <= 100; epoch++) {
      await withRetrievalLog(env, {
        timestamp: filecoinEpochToTimestamp(epoch),
        dataSetId: '1',
        egressBytes: 1000,
        cacheMiss: 0,
      })
      await withRetrievalLog(env, {
        timestamp: filecoinEpochToTimestamp(epoch),
        dataSetId: '1',
        egressBytes: 500,
        cacheMiss: 1,
      })
    }

    // Execute scheduled function
    await worker.scheduled(null, env, null, {
      getChainClient: mockGetChainClient,
    })

    // Verify aggregated data
    expect(simulateContractCalls).toHaveLength(1)
    const simulateCall = simulateContractCalls[0]
    expect(simulateCall.args[0]).toEqual(['1'])
    expect(simulateCall.args[1]).toEqual([100]) // max_epoch
    expect(simulateCall.args[2]).toEqual([7500n]) // 5 epochs × (1000 + 500) bytes all egress
    expect(simulateCall.args[3]).toEqual([2500n]) // 5 epochs × 500 bytes cache miss
  })

  it('should handle datasets with null last_reported_epoch', async () => {
    // Setup: Create dataset with null last_reported_epoch (never reported)
    await withDataSet(env, { id: '1', usageReportedUntil: null })

    const epoch100Timestamp = filecoinEpochToTimestamp(100)
    await withRetrievalLog(env, {
      timestamp: epoch100Timestamp,
      dataSetId: '1',
      egressBytes: 1000,
      cacheMiss: 0,
    })

    // Execute scheduled function
    await worker.scheduled(null, env, null, {
      getChainClient: mockGetChainClient,
    })

    // Verify the dataset is included in reporting
    expect(simulateContractCalls).toHaveLength(1)
    const simulateCall = simulateContractCalls[0]
    expect(simulateCall.args[0]).toEqual(['1'])
    expect(simulateCall.args[2]).toEqual([1000n])
  })
})
