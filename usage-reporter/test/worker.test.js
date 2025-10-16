import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { applyD1Migrations, env as testEnv } from 'cloudflare:test'
import {
  withDataSet,
  withRetrievalLog,
  filecoinEpochToTimestamp,
  FILECOIN_GENESIS_UNIX_TIMESTAMP,
} from './test-helpers.js'
import { epochToTimestamp } from '../lib/usage-report.js'
import worker from '../bin/usage-reporter.js'
import filbeamAbi from '../lib/filbeam.abi.js'

describe('usage reporter worker scheduled entrypoint', () => {
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
    await env.DB.exec('DELETE FROM retrieval_logs')
    await env.DB.exec('DELETE FROM data_sets')
    vi.clearAllMocks()
  })

  it('should report usage data for multiple datasets', async () => {
    const epoch99Timestamp = epochToTimestamp(
      99n,
      BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
    )
    const epoch98Timestamp = epochToTimestamp(
      98n,
      BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
    )
    const epoch99TimestampISO = new Date(epoch99Timestamp * 1000).toISOString()
    const epoch98TimestampISO = new Date(epoch98Timestamp * 1000).toISOString()
    await withDataSet(env, { id: '1', usageReportedUntil: epoch99TimestampISO })
    await withDataSet(env, { id: '2', usageReportedUntil: epoch98TimestampISO })

    const epoch100Timestamp = filecoinEpochToTimestamp(100)

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

    await worker.scheduled(null, env, null, {
      getChainClient: mockGetChainClient,
    })

    expect(mockGetChainClient).toHaveBeenCalledWith(env)
    expect(mockPublicClient.getBlockNumber).toHaveBeenCalled()
    expect(simulateContractCalls).toStrictEqual([
      {
        account: mockAccount,
        address: env.FILBEAM_CONTRACT_ADDRESS,
        abi: filbeamAbi,
        functionName: 'recordUsageRollups',
        args: [100n, ['1', '2'], [2500n, 4000n], [500n, 1000n]],
      },
    ])

    expect(writeContractCalls).toHaveLength(1)
    expect(mockWorkflow.create).toHaveBeenCalled()
  })

  it('should handle when no usage data exists', async () => {
    const epoch99Timestamp = epochToTimestamp(
      99n,
      BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
    )
    const epoch99TimestampISO = new Date(epoch99Timestamp * 1000).toISOString()
    await withDataSet(env, { id: '1', usageReportedUntil: epoch99TimestampISO })

    await worker.scheduled(null, env, null, {
      getChainClient: mockGetChainClient,
    })

    expect(mockGetChainClient).toHaveBeenCalledWith(env)
    expect(mockPublicClient.getBlockNumber).toHaveBeenCalled()
    expect(simulateContractCalls).toHaveLength(0)
    expect(writeContractCalls).toHaveLength(0)
  })

  it('should filter out datasets with zero usage', async () => {
    const epoch99Timestamp = epochToTimestamp(
      99n,
      BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
    )
    const epoch99TimestampISO = new Date(epoch99Timestamp * 1000).toISOString()
    await withDataSet(env, { id: '1', usageReportedUntil: epoch99TimestampISO })
    await withDataSet(env, { id: '2', usageReportedUntil: epoch99TimestampISO })
    await withDataSet(env, { id: '3', usageReportedUntil: epoch99TimestampISO })

    const epoch100Timestamp = filecoinEpochToTimestamp(100)

    await withRetrievalLog(env, {
      timestamp: epoch100Timestamp,
      dataSetId: '1',
      egressBytes: 1000,
      cacheMiss: 0,
    })

    await withRetrievalLog(env, {
      timestamp: epoch100Timestamp,
      dataSetId: '3',
      egressBytes: 2000,
      cacheMiss: 1,
    })

    await worker.scheduled(null, env, null, {
      getChainClient: mockGetChainClient,
    })

    expect(simulateContractCalls).toStrictEqual([
      {
        account: mockAccount,
        address: env.FILBEAM_CONTRACT_ADDRESS,
        abi: filbeamAbi,
        functionName: 'recordUsageRollups',
        args: [100n, ['1', '3'], [1000n, 2000n], [0n, 2000n]],
      },
    ])
  })

  it('should not report datasets that are already up to date', async () => {
    const epoch99Timestamp = epochToTimestamp(
      99n,
      BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
    )
    const epoch100Timestamp = epochToTimestamp(
      100n,
      BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
    )
    const epoch99TimestampISO = new Date(epoch99Timestamp * 1000).toISOString()
    const epoch100TimestampISO = new Date(
      epoch100Timestamp * 1000,
    ).toISOString()

    await withDataSet(env, { id: '1', usageReportedUntil: epoch99TimestampISO })
    await withDataSet(env, {
      id: '2',
      usageReportedUntil: epoch100TimestampISO,
    })

    const epoch100TimestampForLogs = filecoinEpochToTimestamp(100)

    for (const id of ['1', '2']) {
      await withRetrievalLog(env, {
        timestamp: epoch100TimestampForLogs,
        dataSetId: id,
        egressBytes: 1000,
        cacheMiss: 0,
      })
    }

    await worker.scheduled(null, env, null, {
      getChainClient: mockGetChainClient,
    })

    expect(simulateContractCalls).toStrictEqual([
      {
        account: mockAccount,
        address: env.FILBEAM_CONTRACT_ADDRESS,
        abi: filbeamAbi,
        functionName: 'recordUsageRollups',
        args: [100n, ['1'], [1000n], [0n]],
      },
    ])
  })

  it('should calculate correct target epoch', async () => {
    mockPublicClient.getBlockNumber.mockResolvedValue(105n)

    const epoch99Timestamp = epochToTimestamp(
      99n,
      BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
    )
    const epoch99TimestampISO = new Date(epoch99Timestamp * 1000).toISOString()
    await withDataSet(env, { id: '1', usageReportedUntil: epoch99TimestampISO })

    for (let epoch = 100; epoch <= 104; epoch++) {
      await withRetrievalLog(env, {
        timestamp: filecoinEpochToTimestamp(epoch),
        dataSetId: '1',
        egressBytes: 1000,
        cacheMiss: 0,
      })
    }

    await worker.scheduled(null, env, null, {
      getChainClient: mockGetChainClient,
    })

    expect(simulateContractCalls).toStrictEqual([
      {
        account: mockAccount,
        address: env.FILBEAM_CONTRACT_ADDRESS,
        abi: filbeamAbi,
        functionName: 'recordUsageRollups',
        args: [104n, ['1'], [5000n], [0n]],
      },
    ])
  })

  it('should aggregate usage correctly across epochs', async () => {
    const epoch95Timestamp = epochToTimestamp(
      95n,
      BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
    )
    const epoch95TimestampISO = new Date(epoch95Timestamp * 1000).toISOString()
    await withDataSet(env, { id: '1', usageReportedUntil: epoch95TimestampISO })

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

    await worker.scheduled(null, env, null, {
      getChainClient: mockGetChainClient,
    })

    expect(simulateContractCalls).toStrictEqual([
      {
        account: mockAccount,
        address: env.FILBEAM_CONTRACT_ADDRESS,
        abi: filbeamAbi,
        functionName: 'recordUsageRollups',
        args: [100n, ['1'], [7500n], [2500n]],
      },
    ])
  })

  it('should handle datasets with null last_reported_epoch', async () => {
    await withDataSet(env, { id: '1' })

    const epoch100Timestamp = filecoinEpochToTimestamp(100)
    await withRetrievalLog(env, {
      timestamp: epoch100Timestamp,
      dataSetId: '1',
      egressBytes: 1000,
      cacheMiss: 0,
    })

    await worker.scheduled(null, env, null, {
      getChainClient: mockGetChainClient,
    })

    expect(simulateContractCalls).toStrictEqual([
      {
        account: mockAccount,
        address: env.FILBEAM_CONTRACT_ADDRESS,
        abi: filbeamAbi,
        functionName: 'recordUsageRollups',
        args: [100n, ['1'], [1000n], [0n]],
      },
    ])
  })
})
