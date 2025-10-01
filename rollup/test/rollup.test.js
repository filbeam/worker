import { describe, it, expect, beforeEach } from 'vitest'
import { applyD1Migrations, env as testEnv } from 'cloudflare:test'
import {
  withDataSet,
  withRetrievalLog,
  filecoinEpochToTimestamp,
} from './test-helpers.js'
import { aggregateUsageData, prepareBatchData } from '../lib/rollup.js'

describe('rollup', () => {
  describe('database operations', () => {
    let env

    beforeEach(async () => {
      env = testEnv
      await applyD1Migrations(env.DB, env.TEST_MIGRATIONS)
    })

    describe('aggregateUsageData', () => {
      it('should aggregate usage data by cache miss status', async () => {
        const id1 = `agg-1-${Date.now()}`
        const id2 = `agg-2-${Date.now() + 1}`
        // Set last_reported_epoch to 99 so data for epoch 100 will be included
        await withDataSet(env, { id: id1, lastReportedEpoch: 99 })
        await withDataSet(env, { id: id2, lastReportedEpoch: 99 })

        // Create timestamps for specific epochs
        const epoch99imestamp = filecoinEpochToTimestamp(99)
        const epoch100Timestamp = filecoinEpochToTimestamp(100)
        const epoch101Timestamp = filecoinEpochToTimestamp(101)

        // Outside the range (should not be included)
        await withRetrievalLog(env, {
          timestamp: epoch99imestamp,
          dataSetId: id1,
          egressBytes: 1000,
          cacheMiss: 0,
        })

        // Add retrieval logs for dataset 1
        await withRetrievalLog(env, {
          timestamp: epoch100Timestamp,
          dataSetId: id1,
          egressBytes: 2000,
          cacheMiss: 0,
        })

        await withRetrievalLog(env, {
          timestamp: epoch100Timestamp,
          dataSetId: id1,
          egressBytes: 500,
          cacheMiss: 1,
        })

        // Add retrieval logs for dataset 2
        await withRetrievalLog(env, {
          timestamp: epoch100Timestamp,
          dataSetId: id2,
          egressBytes: 3000,
          cacheMiss: 1,
        })

        // Add logs outside the range (should not be included)
        await withRetrievalLog(env, {
          timestamp: epoch101Timestamp,
          dataSetId: id1,
          egressBytes: 9999,
          cacheMiss: 0,
        })

        // Call with targetEpoch = 100 to get data up to epoch 100
        const usageData = await aggregateUsageData(env.DB, 100)

        const usage1 = usageData.find((u) => u.data_set_id === id1)
        expect(usage1).toEqual({
          data_set_id: id1,
          cdn_bytes: 2000, // 2000 from cache hits
          cache_miss_bytes: 500, // 500 from cache miss
          max_epoch: 100,
        })

        const usage2 = usageData.find((u) => u.data_set_id === id2)
        expect(usage2).toEqual({
          data_set_id: id2,
          cdn_bytes: 0,
          cache_miss_bytes: 3000,
          max_epoch: 100,
        })
      })

      it('should include non-200 responses but filter out null egress_bytes', async () => {
        const id = `filter-${Date.now()}`
        // Set last_reported_epoch to 99 so data for epoch 100 will be included
        await withDataSet(env, { id, lastReportedEpoch: 99 })

        const epochTimestamp = filecoinEpochToTimestamp(100)

        // Add various types of logs WITHIN epoch 100 (not at the boundary)
        await withRetrievalLog(env, {
          timestamp: epochTimestamp,
          dataSetId: id,
          responseStatus: 404,
          egressBytes: 1000,
          cacheMiss: 0,
        })

        await withRetrievalLog(env, {
          timestamp: epochTimestamp,
          dataSetId: id,
          responseStatus: 200,
          egressBytes: null,
          cacheMiss: 0,
        })

        await withRetrievalLog(env, {
          timestamp: epochTimestamp,
          dataSetId: id,
          responseStatus: 200,
          egressBytes: 500,
          cacheMiss: 0,
        })

        await withRetrievalLog(env, {
          timestamp: epochTimestamp,
          dataSetId: id,
          responseStatus: 500,
          egressBytes: 300,
          cacheMiss: 1,
        })

        // Call with targetEpoch = 100 to get data up to epoch 100
        const usageData = await aggregateUsageData(env.DB, 100)

        const usage = usageData.find((u) => u.data_set_id === id)
        expect(usage).toEqual({
          data_set_id: id,
          cdn_bytes: 1500, // 404 (1000) + 200 (500) responses with cache_miss=0
          cache_miss_bytes: 300, // 500 response with cache_miss=1
          max_epoch: 100,
        })
      })

      it('should only aggregate data for datasets where last_reported_epoch < currentEpoch - 1', async () => {
        const timestamp = Date.now()
        const id1 = `check-1-${timestamp}`
        const id2 = `check-2-${timestamp + 1}`
        const id3 = `check-3-${timestamp + 2}`
        const id4 = `check-4-${timestamp + 3}`

        // Set different last_reported_epoch values
        await withDataSet(env, { id: id1, lastReportedEpoch: null }) // Should be included
        await withDataSet(env, { id: id2, lastReportedEpoch: 98 }) // Should be included
        await withDataSet(env, { id: id3, lastReportedEpoch: 99 }) // Should be included (99 < 100)
        await withDataSet(env, { id: id4, lastReportedEpoch: 100 }) // Should NOT be included (100 >= 100)

        const epoch100Timestamp = filecoinEpochToTimestamp(100)

        // Add logs for all datasets in epoch 100
        for (const id of [id1, id2, id3, id4]) {
          await withRetrievalLog(env, {
            timestamp: epoch100Timestamp + 10,
            dataSetId: id,
            egressBytes: 1000,
            cacheMiss: 0,
          })
        }

        // Call with targetEpoch = 100 to get data up to epoch 100
        const usageData = await aggregateUsageData(env.DB, 100)

        // Should have data for id1, id2, id3 but NOT id4
        expect(usageData.find((u) => u.data_set_id === id1)).toBeTruthy()
        expect(usageData.find((u) => u.data_set_id === id2)).toBeTruthy()
        expect(usageData.find((u) => u.data_set_id === id3)).toBeTruthy()
        expect(usageData.find((u) => u.data_set_id === id4)).toBeFalsy()

        // Verify the data for included datasets
        const usage1 = usageData.find((u) => u.data_set_id === id1)
        expect(usage1).toEqual({
          data_set_id: id1,
          cdn_bytes: 1000,
          cache_miss_bytes: 0,
          max_epoch: 100,
        })
      })
    })
  })

  describe('prepareBatchData', () => {
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

      const batchData = prepareBatchData(usageData)

      expect(batchData).toEqual({
        dataSetIds: ['1', '2', '3'],
        epochs: [100, 100, 100],
        cdnBytesUsed: [1000n, 2000n, 0n],
        cacheMissBytesUsed: [500n, 0n, 3000n],
      })
    })

    it('should filter out datasets with zero usage', () => {
      const usageData = [
        {
          data_set_id: '1',
          cdn_bytes: 1000,
          cache_miss_bytes: 500,
          max_epoch: 100,
        },
        { data_set_id: '2', cdn_bytes: 0, cache_miss_bytes: 0, max_epoch: 100 }, // Zero usage
        {
          data_set_id: '3',
          cdn_bytes: 0,
          cache_miss_bytes: 3000,
          max_epoch: 100,
        },
      ]

      const batchData = prepareBatchData(usageData)

      expect(batchData.dataSetIds).toEqual(['1', '3'])
      expect(batchData.epochs).toEqual([100, 100])
      expect(batchData.cdnBytesUsed).toEqual([1000n, 0n])
      expect(batchData.cacheMissBytesUsed).toEqual([500n, 3000n])
    })

    it('should handle empty usage data', () => {
      const usageData = []
      const batchData = prepareBatchData(usageData)

      expect(batchData).toEqual({
        dataSetIds: [],
        epochs: [],
        cdnBytesUsed: [],
        cacheMissBytesUsed: [],
      })
    })
  })
})
