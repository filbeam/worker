import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { applyD1Migrations, env as testEnv } from 'cloudflare:test'
import {
  withDataSet,
  withRetrievalLog,
  filecoinEpochToTimestamp,
  FILECOIN_GENESIS_UNIX_TIMESTAMP,
} from './test-helpers.js'
import { aggregateUsageData, prepareUsageRollupData } from '../lib/rollup.js'

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
        // Set last_rollup_reported_at_epoch to 99 so data for epoch 100 will be included
        await withDataSet(env, { id: '1', lastRollupReportedAtEpoch: 99 })
        await withDataSet(env, { id: '2', lastRollupReportedAtEpoch: 99 })

        // Create timestamps for specific epochs
        const epoch99imestamp = filecoinEpochToTimestamp(99)
        const epoch100Timestamp = filecoinEpochToTimestamp(100)
        const epoch101Timestamp = filecoinEpochToTimestamp(101)

        // Outside the range (should not be included)
        await withRetrievalLog(env, {
          timestamp: epoch99imestamp,
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
        const usageData = await aggregateUsageData(
          env.DB,
          BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
          100n,
        )

        const usage1 = usageData.find((u) => u.data_set_id === '1')
        expect(usage1).toEqual({
          data_set_id: '1',
          cdn_bytes: 2500, // 2000 from cache hits + 500 from cache misses
          cache_miss_bytes: 500, // 500 from cache miss
          max_epoch: 100,
        })

        const usage2 = usageData.find((u) => u.data_set_id === '2')
        expect(usage2).toEqual({
          data_set_id: '2',
          cdn_bytes: 3000, // All cache misses
          cache_miss_bytes: 3000,
          max_epoch: 100,
        })
      })

      it('should include non-200 responses but filter out null egress_bytes', async () => {
        // Set last_rollup_reported_at_epoch to 99 so data for epoch 100 will be included
        await withDataSet(env, { id: '1', lastRollupReportedAtEpoch: 99 })

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
        const usageData = await aggregateUsageData(
          env.DB,
          BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
          100n,
        )

        const usage = usageData.find((u) => u.data_set_id === '1')
        expect(usage).toEqual({
          data_set_id: '1',
          cdn_bytes: 1800, // All egress: 404 (1000) + 200 (500) + 500 (300)
          cache_miss_bytes: 300, // 500 response with cache_miss=1
          max_epoch: 100,
        })
      })

      it('should only aggregate data for datasets where last_rollup_reported_at_epoch < currentEpoch - 1', async () => {
        // Set different last_rollup_reported_at_epoch values
        await withDataSet(env, { id: '1', lastRollupReportedAtEpoch: null }) // Should be included
        await withDataSet(env, { id: '2', lastRollupReportedAtEpoch: 98 }) // Should be included
        await withDataSet(env, { id: '3', lastRollupReportedAtEpoch: 99 }) // Should be included (99 < 100)
        await withDataSet(env, { id: '4', lastRollupReportedAtEpoch: 100 }) // Should NOT be included (100 >= 100)

        const epoch100Timestamp = filecoinEpochToTimestamp(100)

        // Add logs for all datasets in epoch 100
        for (const id of ['1', '2', '3', '4']) {
          await withRetrievalLog(env, {
            timestamp: epoch100Timestamp,
            dataSetId: id,
            egressBytes: 1000,
            cacheMiss: 0,
          })
        }

        // Call with targetEpoch = 100 to get data up to epoch 100
        const usageData = await aggregateUsageData(
          env.DB,
          BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
          100n,
        )

        // Should have data for 1, 2, 3 but NOT 4
        expect(usageData.find((u) => u.data_set_id === '1')).toBeTruthy()
        expect(usageData.find((u) => u.data_set_id === '2')).toBeTruthy()
        expect(usageData.find((u) => u.data_set_id === '3')).toBeTruthy()
        expect(usageData.find((u) => u.data_set_id === '4')).toBeFalsy()

        // Verify the data for included datasets
        const usage1 = usageData.find((u) => u.data_set_id === '1')
        expect(usage1).toEqual({
          data_set_id: '1',
          cdn_bytes: 1000,
          cache_miss_bytes: 0,
          max_epoch: 100,
        })
      })

      it('should filter out datasets with zero cdn and cache-miss bytes', async () => {
        // Set last_rollup_reported_at_epoch to 99
        await withDataSet(env, { id: '1', lastRollupReportedAtEpoch: 99 })
        await withDataSet(env, { id: '2', lastRollupReportedAtEpoch: 99 })
        await withDataSet(env, { id: '3', lastRollupReportedAtEpoch: 99 })

        const epoch100Timestamp = filecoinEpochToTimestamp(100)

        // Dataset 1: Has usage
        await withRetrievalLog(env, {
          timestamp: epoch100Timestamp,
          dataSetId: '1',
          egressBytes: 1000,
          cacheMiss: 0,
        })

        // Dataset 2: Zero usage (egress_bytes = null)
        await withRetrievalLog(env, {
          timestamp: epoch100Timestamp,
          dataSetId: '2',
          egressBytes: null,
          cacheMiss: 0,
        })

        // Dataset 3: Has usage
        await withRetrievalLog(env, {
          timestamp: epoch100Timestamp,
          dataSetId: '3',
          egressBytes: 500,
          cacheMiss: 1,
        })

        const usageData = await aggregateUsageData(
          env.DB,
          BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
          100n,
        )

        // Should only have datasets 1 and 3 (dataset 2 has null egress_bytes and is filtered out)
        expect(usageData).toEqual([
          {
            data_set_id: '1',
            cdn_bytes: 1000,
            cache_miss_bytes: 0,
            max_epoch: 100,
          },
          {
            data_set_id: '3',
            cdn_bytes: 500,
            cache_miss_bytes: 500,
            max_epoch: 100,
          },
        ])
      })
    })
  })

  describe('prepareUsageRollupData', () => {
    it('should prepare batch data for contract call', () => {
      const usageData = [
        {
          data_set_id: '1',
          cdn_bytes: 1000,
          cache_miss_bytes: 500,
          max_epoch: 100,
        },
        {
          data_set_id: '2',
          cdn_bytes: 2000,
          cache_miss_bytes: 0,
          max_epoch: 100,
        },
        {
          data_set_id: '3',
          cdn_bytes: 0,
          cache_miss_bytes: 3000,
          max_epoch: 100,
        },
      ]

      const batchData = prepareUsageRollupData(usageData)

      expect(batchData).toEqual({
        dataSetIds: ['1', '2', '3'],
        epochs: [100, 100, 100],
        cdnBytesUsed: [1000n, 2000n, 0n],
        cacheMissBytesUsed: [500n, 0n, 3000n],
      })
    })

    it('should process all datasets', () => {
      const usageData = [
        {
          data_set_id: '1',
          cdn_bytes: 1000,
          cache_miss_bytes: 500,
          max_epoch: 100,
        },
        {
          data_set_id: '2',
          cdn_bytes: 2000,
          cache_miss_bytes: 0,
          max_epoch: 100,
        },
        {
          data_set_id: '3',
          cdn_bytes: 0,
          cache_miss_bytes: 3000,
          max_epoch: 100,
        },
      ]

      const batchData = prepareUsageRollupData(usageData)

      expect(batchData.dataSetIds).toEqual(['1', '2', '3'])
      expect(batchData.epochs).toEqual([100, 100, 100])
      expect(batchData.cdnBytesUsed).toEqual([1000n, 2000n, 0n])
      expect(batchData.cacheMissBytesUsed).toEqual([500n, 0n, 3000n])
    })

    it('should handle empty usage data', () => {
      const usageData = []
      const batchData = prepareUsageRollupData(usageData)

      expect(batchData).toEqual({
        dataSetIds: [],
        epochs: [],
        cdnBytesUsed: [],
        cacheMissBytesUsed: [],
      })
    })
  })
})
