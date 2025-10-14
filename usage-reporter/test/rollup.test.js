import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { applyD1Migrations, env as testEnv } from 'cloudflare:test'
import {
  withDataSet,
  withRetrievalLog,
  filecoinEpochToTimestamp,
  FILECOIN_GENESIS_UNIX_TIMESTAMP,
} from './test-helpers.js'
import {
  aggregateUsageData,
  prepareUsageRollupData,
  epochToTimestamp,
} from '../lib/rollup.js'

describe('rollup', () => {
  describe('database operations', () => {
    let env

    beforeEach(async () => {
      env = testEnv
      await applyD1Migrations(env.DB, env.TEST_MIGRATIONS)
    })

    afterEach(async () => {
      await env.DB.exec('DELETE FROM retrieval_logs;')
      await env.DB.exec('DELETE FROM data_sets;')
    })

    describe('aggregateUsageData', () => {
      it('should aggregate usage data by cache miss status', async () => {
        // Set usage_reported_until to timestamp for epoch 99 so data for epoch 100 will be included
        const epoch99Timestamp = epochToTimestamp(
          99n,
          BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
        )
        const epoch99TimestampISO = new Date(
          epoch99Timestamp * 1000,
        ).toISOString()
        await withDataSet(env, {
          id: '1',
          usageReportedUntil: epoch99TimestampISO,
        })
        await withDataSet(env, {
          id: '2',
          usageReportedUntil: epoch99TimestampISO,
        })

        // Create timestamps for specific epochs
        const epoch99UnixTimestamp = filecoinEpochToTimestamp(99)
        const epoch100Timestamp = filecoinEpochToTimestamp(100)
        const epoch101Timestamp = filecoinEpochToTimestamp(101)

        // Outside the range (should not be included)
        await withRetrievalLog(env, {
          timestamp: epoch99UnixTimestamp,
          dataSetId: '1',
          egressBytes: 1000,
          cacheMiss: 0,
        })

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
          cacheMiss: 1,
        })

        // Add logs outside the range (should not be included)
        await withRetrievalLog(env, {
          timestamp: epoch101Timestamp,
          dataSetId: '1',
          egressBytes: 9999,
          cacheMiss: 0,
        })

        await withRetrievalLog(env, {
          timestamp: epoch101Timestamp,
          dataSetId: '2',
          egressBytes: 9999,
          cacheMiss: 0,
        })

        // Call with targetEpoch = 100 to get data up to epoch 100
        const upToTimestamp = epochToTimestamp(
          100n,
          BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
        )
        const usageData = await aggregateUsageData(env.DB, upToTimestamp)

        const usage1 = usageData.find((u) => u.data_set_id === '1')
        expect(usage1.data_set_id).toBe('1')
        expect(usage1.cdn_bytes).toBe(2500) // 2000 from cache hits + 500 from cache misses
        expect(usage1.cache_miss_bytes).toBe(500) // 500 from cache miss
        expect(usage1.max_timestamp).toBe(
          FILECOIN_GENESIS_UNIX_TIMESTAMP + 100 * 30,
        )

        const usage2 = usageData.find((u) => u.data_set_id === '2')
        expect(usage2.data_set_id).toBe('2')
        expect(usage2.cdn_bytes).toBe(3000) // All cache misses
        expect(usage2.cache_miss_bytes).toBe(3000)
        expect(usage2.max_timestamp).toBe(
          FILECOIN_GENESIS_UNIX_TIMESTAMP + 100 * 30,
        )
      })

      it('should include non-200 responses but filter out null egress_bytes', async () => {
        // Set usage_reported_until to timestamp for epoch 99 so data for epoch 100 will be included
        const epoch99Timestamp = epochToTimestamp(
          99n,
          BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
        )
        const epoch99TimestampISO = new Date(
          epoch99Timestamp * 1000,
        ).toISOString()
        await withDataSet(env, {
          id: '1',
          usageReportedUntil: epoch99TimestampISO,
        })

        const epochTimestamp = filecoinEpochToTimestamp(100)

        // Add various types of logs WITHIN epoch 100 (not at the boundary)
        await withRetrievalLog(env, {
          timestamp: epochTimestamp,
          dataSetId: '1',
          responseStatus: 404,
          egressBytes: 1000,
          cacheMiss: 0,
        })

        await withRetrievalLog(env, {
          timestamp: epochTimestamp,
          dataSetId: '1',
          responseStatus: 200,
          egressBytes: null,
          cacheMiss: 0,
        })

        await withRetrievalLog(env, {
          timestamp: epochTimestamp,
          dataSetId: '1',
          responseStatus: 200,
          egressBytes: 500,
          cacheMiss: 0,
        })

        await withRetrievalLog(env, {
          timestamp: epochTimestamp,
          dataSetId: '1',
          responseStatus: 500,
          egressBytes: 300,
          cacheMiss: 1,
        })

        // Call with targetEpoch = 100 to get data up to epoch 100
        const upToTimestamp = epochToTimestamp(
          100n,
          BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
        )
        const usageData = await aggregateUsageData(env.DB, upToTimestamp)

        const usage = usageData.find((u) => u.data_set_id === '1')
        expect(usage.data_set_id).toBe('1')
        expect(usage.cdn_bytes).toBe(1800) // All egress: 404 (1000) + 200 (500) + 500 (300)
        expect(usage.cache_miss_bytes).toBe(300) // 500 response with cache_miss=1
        expect(usage.max_timestamp).toBe(
          FILECOIN_GENESIS_UNIX_TIMESTAMP + 100 * 30,
        )
      })

      it('should only aggregate data for datasets with usage_reported_until < upToTimestamp', async () => {
        // Set different usage_reported_until values
        const epoch98Timestamp = epochToTimestamp(
          98n,
          BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
        )
        const epoch99Timestamp = epochToTimestamp(
          99n,
          BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
        )
        const epoch100Timestamp = epochToTimestamp(
          100n,
          BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
        )
        const epoch98TimestampISO = new Date(
          epoch98Timestamp * 1000,
        ).toISOString()
        const epoch99TimestampISO = new Date(
          epoch99Timestamp * 1000,
        ).toISOString()
        const epoch100TimestampISO = new Date(
          epoch100Timestamp * 1000,
        ).toISOString()

        await withDataSet(env, { id: '1' }) // Should be included (uses default 1970 epoch)
        await withDataSet(env, {
          id: '2',
          usageReportedUntil: epoch98TimestampISO,
        }) // Should be included
        await withDataSet(env, {
          id: '3',
          usageReportedUntil: epoch99TimestampISO,
        }) // Should be included
        await withDataSet(env, {
          id: '4',
          usageReportedUntil: epoch100TimestampISO,
        }) // Should NOT be included

        const epoch100UnixTimestamp = filecoinEpochToTimestamp(100)

        // Add logs for all datasets in epoch 100
        for (const id of ['1', '2', '3', '4']) {
          await withRetrievalLog(env, {
            timestamp: epoch100UnixTimestamp,
            dataSetId: id,
            egressBytes: 1000,
            cacheMiss: 0,
          })
        }

        // Call with upToTimestamp for epoch 100
        const upToTimestamp = epochToTimestamp(
          100n,
          BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
        )
        const usageData = await aggregateUsageData(env.DB, upToTimestamp)

        // Should have data for 1, 2, 3 but NOT 4
        expect(usageData.find((u) => u.data_set_id === '1')).toBeTruthy()
        expect(usageData.find((u) => u.data_set_id === '2')).toBeTruthy()
        expect(usageData.find((u) => u.data_set_id === '3')).toBeTruthy()
        expect(usageData.find((u) => u.data_set_id === '4')).toBeFalsy()

        // Verify the data for included datasets
        const usage1 = usageData.find((u) => u.data_set_id === '1')
        expect(usage1.data_set_id).toBe('1')
        expect(usage1.cdn_bytes).toBe(1000)
        expect(usage1.cache_miss_bytes).toBe(0)
        expect(usage1.max_timestamp).toBe(
          FILECOIN_GENESIS_UNIX_TIMESTAMP + 100 * 30,
        )
      })

      it('should filter out datasets with zero cdn and cache-miss bytes', async () => {
        // Set usage_reported_until to epoch 99 timestamp
        const epoch99Timestamp = epochToTimestamp(
          99n,
          BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
        )
        const epoch99TimestampISO = new Date(
          epoch99Timestamp * 1000,
        ).toISOString()
        await withDataSet(env, {
          id: '1',
          usageReportedUntil: epoch99TimestampISO,
        })
        await withDataSet(env, {
          id: '2',
          usageReportedUntil: epoch99TimestampISO,
        })
        await withDataSet(env, {
          id: '3',
          usageReportedUntil: epoch99TimestampISO,
        })

        const epoch100UnixTimestamp = filecoinEpochToTimestamp(100)

        // Dataset 1: Has usage
        await withRetrievalLog(env, {
          timestamp: epoch100UnixTimestamp,
          dataSetId: '1',
          egressBytes: 1000,
          cacheMiss: 0,
        })

        // Dataset 2: Zero usage (egress_bytes = null)
        await withRetrievalLog(env, {
          timestamp: epoch100UnixTimestamp,
          dataSetId: '2',
          egressBytes: null,
          cacheMiss: 0,
        })

        // Dataset 3: Has usage
        await withRetrievalLog(env, {
          timestamp: epoch100UnixTimestamp,
          dataSetId: '3',
          egressBytes: 500,
          cacheMiss: 1,
        })

        const upToTimestamp = epochToTimestamp(
          100n,
          BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
        )
        const usageData = await aggregateUsageData(env.DB, upToTimestamp)

        // Should only have datasets 1 and 3 (dataset 2 has null egress_bytes and is filtered out)
        expect(usageData.length).toBe(2)

        const usage1 = usageData.find((u) => u.data_set_id === '1')
        expect(usage1.data_set_id).toBe('1')
        expect(usage1.cdn_bytes).toBe(1000)
        expect(usage1.cache_miss_bytes).toBe(0)
        expect(usage1.max_timestamp).toBe(
          FILECOIN_GENESIS_UNIX_TIMESTAMP + 100 * 30,
        )

        const usage3 = usageData.find((u) => u.data_set_id === '3')
        expect(usage3.data_set_id).toBe('3')
        expect(usage3.cdn_bytes).toBe(500)
        expect(usage3.cache_miss_bytes).toBe(500)
        expect(usage3.max_timestamp).toBe(
          FILECOIN_GENESIS_UNIX_TIMESTAMP + 100 * 30,
        )
      })

      it('should exclude datasets with pending rollup transactions', async () => {
        // Set usage_reported_until to epoch 99 timestamp
        const epoch99Timestamp = epochToTimestamp(
          99n,
          BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
        )
        const epoch99TimestampISO = new Date(
          epoch99Timestamp * 1000,
        ).toISOString()

        // Dataset 1: No pending transaction - should be included
        await withDataSet(env, {
          id: '1',
          usageReportedUntil: epoch99TimestampISO,
          pendingUsageReportingTxHash: null,
        })

        // Dataset 2: Has pending transaction - should be excluded
        await withDataSet(env, {
          id: '2',
          usageReportedUntil: epoch99TimestampISO,
          pendingUsageReportingTxHash: '0x123abc',
        })

        const epoch100UnixTimestamp = filecoinEpochToTimestamp(100)

        // Add logs for both datasets
        await withRetrievalLog(env, {
          timestamp: epoch100UnixTimestamp,
          dataSetId: '1',
          egressBytes: 1000,
          cacheMiss: 0,
        })

        await withRetrievalLog(env, {
          timestamp: epoch100UnixTimestamp,
          dataSetId: '2',
          egressBytes: 2000,
          cacheMiss: 0,
        })

        const upToTimestamp = epochToTimestamp(
          100n,
          BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
        )
        const usageData = await aggregateUsageData(env.DB, upToTimestamp)

        // Should only have dataset 1 (dataset 2 has pending transaction)
        expect(usageData.length).toBe(1)

        const usage1 = usageData.find((u) => u.data_set_id === '1')
        expect(usage1.data_set_id).toBe('1')
        expect(usage1.cdn_bytes).toBe(1000)
        expect(usage1.cache_miss_bytes).toBe(0)
        expect(usage1.max_timestamp).toBe(
          FILECOIN_GENESIS_UNIX_TIMESTAMP + 100 * 30,
        )

        // Dataset 2 should not be present
        const usage2 = usageData.find((u) => u.data_set_id === '2')
        expect(usage2).toBeUndefined()
      })
    })
  })

  describe('prepareUsageRollupData', () => {
    it('should prepare batch data for contract call', () => {
      // Epoch 100 timestamp = GENESIS + (100 * 30)
      const epoch100Timestamp = FILECOIN_GENESIS_UNIX_TIMESTAMP + 100 * 30
      const usageData = [
        {
          data_set_id: '1',
          cdn_bytes: 1000,
          cache_miss_bytes: 500,
          max_timestamp: epoch100Timestamp,
        },
        {
          data_set_id: '2',
          cdn_bytes: 2000,
          cache_miss_bytes: 0,
          max_timestamp: epoch100Timestamp,
        },
        {
          data_set_id: '3',
          cdn_bytes: 0,
          cache_miss_bytes: 3000,
          max_timestamp: epoch100Timestamp,
        },
      ]

      const batchData = prepareUsageRollupData(
        usageData,
        BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
      )

      expect(batchData).toEqual({
        dataSetIds: ['1', '2', '3'],
        cdnBytesUsed: [1000n, 2000n, 0n],
        cacheMissBytesUsed: [500n, 0n, 3000n],
        maxEpochs: [100, 100, 100],
      })
    })

    it('should process all datasets', () => {
      const epoch98Timestamp = FILECOIN_GENESIS_UNIX_TIMESTAMP + 98 * 30
      const epoch99Timestamp = FILECOIN_GENESIS_UNIX_TIMESTAMP + 99 * 30
      const epoch100Timestamp = FILECOIN_GENESIS_UNIX_TIMESTAMP + 100 * 30

      const usageData = [
        {
          data_set_id: '1',
          cdn_bytes: 1000,
          cache_miss_bytes: 500,
          max_timestamp: epoch98Timestamp,
        },
        {
          data_set_id: '2',
          cdn_bytes: 2000,
          cache_miss_bytes: 0,
          max_timestamp: epoch99Timestamp,
        },
        {
          data_set_id: '3',
          cdn_bytes: 0,
          cache_miss_bytes: 3000,
          max_timestamp: epoch100Timestamp,
        },
      ]

      const batchData = prepareUsageRollupData(
        usageData,
        BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
      )

      expect(batchData.dataSetIds).toEqual(['1', '2', '3'])
      expect(batchData.cdnBytesUsed).toEqual([1000n, 2000n, 0n])
      expect(batchData.cacheMissBytesUsed).toEqual([500n, 0n, 3000n])
      expect(batchData.maxEpochs).toEqual([98, 99, 100])
    })

    it('should handle empty usage data', () => {
      const usageData = []
      const batchData = prepareUsageRollupData(
        usageData,
        BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
      )

      expect(batchData).toEqual({
        dataSetIds: [],
        cdnBytesUsed: [],
        cacheMissBytesUsed: [],
        maxEpochs: [],
      })
    })
  })
})
