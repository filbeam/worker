import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { handleTransactionConfirmedQueueMessage } from '../lib/queue-handlers.js'
import { env } from 'cloudflare:test'
import { randomId, withDataSet } from './test-helpers.js'

describe('handleTransactionConfirmedQueueMessage', () => {
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

  it('processes transaction confirmation successfully for single dataset', async () => {
    const dataSetId = randomId()
    const transactionHash = '0xTest'
    const upToTimestamp = '2024-11-09T12:00:00.000Z'
    const message = {
      type: 'transaction-confirmed',
      transactionHash,
      upToTimestamp,
    }

    // Create a dataset with pending transaction hash
    await withDataSet(env, {
      id: dataSetId.toString(),
      pendingUsageReportTxHash: transactionHash,
    })

    await handleTransactionConfirmedQueueMessage(message, env)

    const { results: dataSets } = await env.DB.prepare(
      'SELECT usage_reported_until, pending_usage_report_tx_hash FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .all()

    expect(dataSets).toStrictEqual([
      {
        usage_reported_until: upToTimestamp,
        pending_usage_report_tx_hash: null,
      },
    ])
  })

  it('processes transaction confirmation successfully for multiple datasets', async () => {
    const transactionHash = '0xTest'
    const upToTimestamp = '2024-11-09T14:30:00.000Z'
    const dataSetIds = [randomId(), randomId(), randomId()]
    const message = {
      type: 'transaction-confirmed',
      transactionHash,
      upToTimestamp,
    }

    // Create three datasets with the same pending transaction hash
    for (const dataSetId of dataSetIds) {
      await withDataSet(env, {
        id: dataSetId.toString(),
        pendingUsageReportTxHash: transactionHash,
      })
    }

    await handleTransactionConfirmedQueueMessage(message, env)

    // Verify all three datasets were updated
    for (const dataSetId of dataSetIds) {
      const { results: dataSets } = await env.DB.prepare(
        'SELECT usage_reported_until, pending_usage_report_tx_hash FROM data_sets WHERE id = ?',
      )
        .bind(dataSetId)
        .all()

      expect(dataSets).toStrictEqual([
        {
          usage_reported_until: upToTimestamp,
          pending_usage_report_tx_hash: null,
        },
      ])
    }

    // Verify the total number of updated records
    const { results: allDataSets } = await env.DB.prepare(
      'SELECT COUNT(*) as count FROM data_sets WHERE usage_reported_until = ?',
    )
      .bind(upToTimestamp)
      .all()

    expect(allDataSets[0].count).toBe(3)
  })

  it('handles no matching datasets gracefully', async () => {
    const dataSetId = randomId()
    const differentTxHash = '0xTest1'
    const transactionHash = '0xTest2'
    const upToTimestamp = '2024-11-09T16:00:00.000Z'
    const message = {
      type: 'transaction-confirmed',
      transactionHash,
      upToTimestamp,
    }

    // Create a dataset with a different pending transaction hash
    await withDataSet(env, {
      id: dataSetId.toString(),
      pendingUsageReportTxHash: differentTxHash,
    })

    // Should not throw an error
    await expect(
      handleTransactionConfirmedQueueMessage(message, env),
    ).resolves.not.toThrow()

    // Verify the dataset was not modified
    const { results: dataSets } = await env.DB.prepare(
      'SELECT pending_usage_report_tx_hash FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .all()

    expect(dataSets).toStrictEqual([
      {
        pending_usage_report_tx_hash: differentTxHash,
      },
    ])
  })

  it('throws error when transactionHash is missing', async () => {
    const message = {
      type: 'transaction-confirmed',
      upToTimestamp: '2024-11-09T12:00:00.000Z',
      // transactionHash is missing
    }

    await expect(
      handleTransactionConfirmedQueueMessage(message, env),
    ).rejects.toThrow()
  })

  it('throws error when upToTimestamp is missing', async () => {
    const message = {
      type: 'transaction-confirmed',
      transactionHash: '0xTest',
      // upToTimestamp is missing
    }

    await expect(
      handleTransactionConfirmedQueueMessage(message, env),
    ).rejects.toThrow()
  })

  it('handles database errors correctly', async () => {
    const transactionHash = '0xTest'
    const upToTimestamp = '2024-11-09T12:00:00.000Z'
    const message = {
      type: 'transaction-confirmed',
      transactionHash,
      upToTimestamp,
    }

    // Create a mock environment with a failing DB
    const dbError = new Error('Database connection failed')
    const mockEnv = {
      ...env,
      DB: {
        prepare: vi.fn().mockImplementation(() => ({
          bind: vi.fn().mockReturnThis(),
          run: vi.fn().mockRejectedValue(dbError),
        })),
      },
    }

    await expect(
      handleTransactionConfirmedQueueMessage(message, mockEnv),
    ).rejects.toThrow('Database connection failed')
  })
})
