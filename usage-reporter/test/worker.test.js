import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { applyD1Migrations, env } from 'cloudflare:test'
import { epochToUnixTimestamp } from '../lib/usage-report.js'
import worker from '../bin/usage-reporter.js'
import filbeamAbi from '../lib/filbeam.abi.js'
import {
  withDataSet,
  withRetrievalLog,
  EPOCH_95_TIMESTAMP_ISO,
  EPOCH_98_TIMESTAMP_ISO,
  EPOCH_99_TIMESTAMP_ISO,
  EPOCH_100_TIMESTAMP_ISO,
} from './test-helpers.js'

// const EPOCH_95_TIMESTAMP = epochToUnixTimestamp(
//   95n,
//   BigInt(env.GENESIS_BLOCK_TIMESTAMP),
// )
// const EPOCH_98_TIMESTAMP = epochToUnixTimestamp(
//   98n,
//   BigInt(env.GENESIS_BLOCK_TIMESTAMP),
// )
// const EPOCH_99_TIMESTAMP = epochToUnixTimestamp(
//   99n,
//   BigInt(env.GENESIS_BLOCK_TIMESTAMP),
// )
// const EPOCH_100_TIMESTAMP = epochToUnixTimestamp(
//   100n,
//   BigInt(env.GENESIS_BLOCK_TIMESTAMP),
// )
// const EPOCH_95_TIMESTAMP_ISO = new Date(EPOCH_95_TIMESTAMP * 1000).toISOString()
// const EPOCH_98_TIMESTAMP_ISO = new Date(EPOCH_98_TIMESTAMP * 1000).toISOString()
// const EPOCH_99_TIMESTAMP_ISO = new Date(EPOCH_99_TIMESTAMP * 1000).toISOString()
// const EPOCH_100_TIMESTAMP_ISO = new Date(
//   EPOCH_100_TIMESTAMP * 1000,
// ).toISOString()

describe('usage reporter worker scheduled entrypoint', () => {
  let simulateContractCalls
  let writeContractCalls

  const mockWorkflow = {
    create: vi.fn().mockResolvedValue(undefined),
  }

  const mockAccount = { address: '0xMockAccountAddress' }

  const mockPublicClient = {
    getBlockNumber: vi.fn().mockResolvedValue(101n),
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

  const mockEnv = {
    ...env,
    FILBEAM_CONTRACT_ADDRESS: '0xMockFilBeamAddress',
    FILBEAM_CONTROLLER_PRIVATE_KEY: '0xMockPrivateKey',
    TRANSACTION_MONITOR_WORKFLOW: mockWorkflow,
  }

  beforeEach(async () => {
    simulateContractCalls = []
    writeContractCalls = []

    await applyD1Migrations(mockEnv.DB, mockEnv.TEST_MIGRATIONS)
  })

  afterEach(async () => {
    await mockEnv.DB.exec('DELETE FROM retrieval_logs')
    await mockEnv.DB.exec('DELETE FROM data_sets')
    vi.clearAllMocks()
  })

  it('should report usage data for multiple datasets', async () => {
    await withDataSet(mockEnv, {
      id: '1',
      usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
    })
    await withDataSet(mockEnv, {
      id: '2',
      usageReportedUntil: EPOCH_98_TIMESTAMP_ISO,
    })

    await withRetrievalLog(mockEnv, {
      timestamp: EPOCH_100_TIMESTAMP_ISO,
      dataSetId: '1',
      egressBytes: 2000,
      cacheMiss: 0,
    })
    await withRetrievalLog(mockEnv, {
      timestamp: EPOCH_100_TIMESTAMP_ISO,
      dataSetId: '1',
      egressBytes: 500,
      cacheMiss: 1,
    })

    await withRetrievalLog(mockEnv, {
      timestamp: EPOCH_100_TIMESTAMP_ISO,
      dataSetId: '2',
      egressBytes: 3000,
      cacheMiss: 0,
    })
    await withRetrievalLog(mockEnv, {
      timestamp: EPOCH_100_TIMESTAMP_ISO,
      dataSetId: '2',
      egressBytes: 1000,
      cacheMiss: 1,
    })

    await worker.scheduled(null, mockEnv, null, {
      getChainClient: mockGetChainClient,
    })

    expect(mockGetChainClient).toHaveBeenCalledWith(mockEnv)
    expect(mockPublicClient.getBlockNumber).toHaveBeenCalled()
    expect(simulateContractCalls).toStrictEqual([
      {
        account: mockAccount,
        address: mockEnv.FILBEAM_CONTRACT_ADDRESS,
        abi: filbeamAbi,
        functionName: 'recordUsageRollups',
        args: [100n, ['1', '2'], [2500n, 4000n], [500n, 1000n]],
      },
    ])

    expect(writeContractCalls).toHaveLength(1)
    expect(mockWorkflow.create).toHaveBeenCalled()
  })

  it('should handle when no usage data exists', async () => {
    await withDataSet(mockEnv, {
      id: '1',
      usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
    })

    await worker.scheduled(null, mockEnv, null, {
      getChainClient: mockGetChainClient,
    })

    expect(mockGetChainClient).toHaveBeenCalledWith(mockEnv)
    expect(mockPublicClient.getBlockNumber).toHaveBeenCalled()
    expect(simulateContractCalls).toHaveLength(0)
    expect(writeContractCalls).toHaveLength(0)
  })

  it('should filter out datasets with zero usage', async () => {
    await withDataSet(mockEnv, {
      id: '1',
      usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
    })
    await withDataSet(mockEnv, {
      id: '2',
      usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
    })
    await withDataSet(mockEnv, {
      id: '3',
      usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
    })

    await withRetrievalLog(mockEnv, {
      timestamp: EPOCH_100_TIMESTAMP_ISO,
      dataSetId: '1',
      egressBytes: 1000,
      cacheMiss: 0,
    })

    await withRetrievalLog(mockEnv, {
      timestamp: EPOCH_100_TIMESTAMP_ISO,
      dataSetId: '3',
      egressBytes: 2000,
      cacheMiss: 1,
    })

    await worker.scheduled(null, mockEnv, null, {
      getChainClient: mockGetChainClient,
    })

    expect(simulateContractCalls).toStrictEqual([
      {
        account: mockAccount,
        address: mockEnv.FILBEAM_CONTRACT_ADDRESS,
        abi: filbeamAbi,
        functionName: 'recordUsageRollups',
        args: [100n, ['1', '3'], [1000n, 2000n], [0n, 2000n]],
      },
    ])
  })

  it('should not report datasets that are already up to date', async () => {
    await withDataSet(mockEnv, {
      id: '1',
      usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
    })
    await withDataSet(mockEnv, {
      id: '2',
      usageReportedUntil: EPOCH_100_TIMESTAMP_ISO,
    })

    for (const id of ['1', '2']) {
      await withRetrievalLog(mockEnv, {
        timestamp: EPOCH_100_TIMESTAMP_ISO,
        dataSetId: id,
        egressBytes: 1000,
        cacheMiss: 0,
      })
    }

    await worker.scheduled(null, mockEnv, null, {
      getChainClient: mockGetChainClient,
    })

    expect(simulateContractCalls).toStrictEqual([
      {
        account: mockAccount,
        address: mockEnv.FILBEAM_CONTRACT_ADDRESS,
        abi: filbeamAbi,
        functionName: 'recordUsageRollups',
        args: [100n, ['1'], [1000n], [0n]],
      },
    ])
  })

  it('should calculate correct target epoch', async () => {
    mockPublicClient.getBlockNumber.mockResolvedValueOnce(105n)

    await withDataSet(mockEnv, {
      id: '1',
      usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
    })

    for (let epoch = 100; epoch <= 104; epoch++) {
      const timestampIso = new Date(
        epochToUnixTimestamp(epoch, mockEnv.GENESIS_BLOCK_TIMESTAMP) * 1000,
      ).toISOString()
      await withRetrievalLog(mockEnv, {
        timestamp: timestampIso,
        dataSetId: '1',
        egressBytes: 1000,
        cacheMiss: 0,
      })
    }

    await worker.scheduled(null, mockEnv, null, {
      getChainClient: mockGetChainClient,
    })

    expect(simulateContractCalls).toStrictEqual([
      {
        account: mockAccount,
        address: mockEnv.FILBEAM_CONTRACT_ADDRESS,
        abi: filbeamAbi,
        functionName: 'recordUsageRollups',
        args: [104n, ['1'], [5000n], [0n]],
      },
    ])
  })

  it('should aggregate usage correctly across epochs', async () => {
    await withDataSet(mockEnv, {
      id: '1',
      usageReportedUntil: EPOCH_95_TIMESTAMP_ISO,
    })

    for (let epoch = 96; epoch <= 100; epoch++) {
      const timestampIso = new Date(
        epochToUnixTimestamp(epoch, mockEnv.GENESIS_BLOCK_TIMESTAMP) * 1000,
      ).toISOString()
      await withRetrievalLog(mockEnv, {
        timestamp: timestampIso,
        dataSetId: '1',
        egressBytes: 1000,
        cacheMiss: 0,
      })
      await withRetrievalLog(mockEnv, {
        timestamp: timestampIso,
        dataSetId: '1',
        egressBytes: 500,
        cacheMiss: 1,
      })
    }

    await worker.scheduled(null, mockEnv, null, {
      getChainClient: mockGetChainClient,
    })

    expect(simulateContractCalls).toStrictEqual([
      {
        account: mockAccount,
        address: mockEnv.FILBEAM_CONTRACT_ADDRESS,
        abi: filbeamAbi,
        functionName: 'recordUsageRollups',
        args: [100n, ['1'], [7500n], [2500n]],
      },
    ])
  })

  it('should handle datasets with null usage_reported_until', async () => {
    await withDataSet(mockEnv, { id: '1' })

    await withRetrievalLog(mockEnv, {
      timestamp: EPOCH_100_TIMESTAMP_ISO,
      dataSetId: '1',
      egressBytes: 1000,
      cacheMiss: 0,
    })

    await worker.scheduled(null, mockEnv, null, {
      getChainClient: mockGetChainClient,
    })

    expect(simulateContractCalls).toStrictEqual([
      {
        account: mockAccount,
        address: mockEnv.FILBEAM_CONTRACT_ADDRESS,
        abi: filbeamAbi,
        functionName: 'recordUsageRollups',
        args: [100n, ['1'], [1000n], [0n]],
      },
    ])
  })
})
