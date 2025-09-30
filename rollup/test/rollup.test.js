import { describe, it, expect, beforeEach } from 'vitest'
import { applyD1Migrations, env as testEnv } from 'cloudflare:test'
import { withDataSet } from './test-helpers.js'
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
        const epoch100Timestamp = 100 * 30 + 1598306400
        const epoch101Timestamp = 101 * 30 + 1598306400

        // Add retrieval logs for dataset 1
        await env.DB.prepare(
          `INSERT INTO retrieval_logs (timestamp, data_set_id, response_status, egress_bytes, cache_miss)
           VALUES (datetime(?, 'unixepoch'), ?, 200, 1000, 0)`,
        )
          .bind(epoch100Timestamp + 10, id1)
          .run()

        await env.DB.prepare(
          `INSERT INTO retrieval_logs (timestamp, data_set_id, response_status, egress_bytes, cache_miss)
           VALUES (datetime(?, 'unixepoch'), ?, 200, 2000, 0)`,
        )
          .bind(epoch100Timestamp + 20, id1)
          .run()

        await env.DB.prepare(
          `INSERT INTO retrieval_logs (timestamp, data_set_id, response_status, egress_bytes, cache_miss)
           VALUES (datetime(?, 'unixepoch'), ?, 200, 500, 1)`,
        )
          .bind(epoch100Timestamp + 15, id1)
          .run()

        // Add retrieval logs for dataset 2
        await env.DB.prepare(
          `INSERT INTO retrieval_logs (timestamp, data_set_id, response_status, egress_bytes, cache_miss)
           VALUES (datetime(?, 'unixepoch'), ?, 200, 3000, 1)`,
        )
          .bind(epoch100Timestamp + 25, id2)
          .run()

        // Add logs outside the range (should not be included)
        await env.DB.prepare(
          `INSERT INTO retrieval_logs (timestamp, data_set_id, response_status, egress_bytes, cache_miss)
           VALUES (datetime(?, 'unixepoch'), ?, 200, 9999, 0)`,
        )
          .bind(epoch101Timestamp + 10, id1)
          .run()

        // Call with targetEpoch = 100 to get data up to epoch 100
        const usageData = await aggregateUsageData(env.DB, 100)

        expect(usageData.get(id1)).toEqual({
          cdnBytes: 3000, // 1000 + 2000 from cache hits
          cacheMissBytes: 500, // 500 from cache miss
          epoch: 100,
        })

        expect(usageData.get(id2)).toEqual({
          cdnBytes: 0,
          cacheMissBytes: 3000,
          epoch: 100,
        })
      })

      it('should filter out non-200 responses and null egress_bytes', async () => {
        const id = `filter-${Date.now()}`
        // Set last_reported_epoch to 99 so data for epoch 100 will be included
        await withDataSet(env, { id, lastReportedEpoch: 99 })

        const epochTimestamp = 100 * 30 + 1598306400

        // Add various types of logs WITHIN epoch 100 (not at the boundary)
        await env.DB.prepare(
          `INSERT INTO retrieval_logs (timestamp, data_set_id, response_status, egress_bytes, cache_miss)
           VALUES (datetime(?, 'unixepoch'), ?, 404, 1000, 0)`,
        )
          .bind(epochTimestamp + 10, id)
          .run()

        await env.DB.prepare(
          `INSERT INTO retrieval_logs (timestamp, data_set_id, response_status, egress_bytes, cache_miss)
           VALUES (datetime(?, 'unixepoch'), ?, 200, NULL, 0)`,
        )
          .bind(epochTimestamp + 20, id)
          .run()

        await env.DB.prepare(
          `INSERT INTO retrieval_logs (timestamp, data_set_id, response_status, egress_bytes, cache_miss)
           VALUES (datetime(?, 'unixepoch'), ?, 200, 500, 0)`,
        )
          .bind(epochTimestamp + 25, id) // Changed from +30 to +25 to keep it within epoch 100
          .run()

        // Call with targetEpoch = 100 to get data up to epoch 100
        const usageData = await aggregateUsageData(env.DB, 100)

        expect(usageData.get(id)).toEqual({
          cdnBytes: 500, // Only the valid 200 response
          cacheMissBytes: 0,
          epoch: 100,
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

        const epoch100Timestamp = 100 * 30 + 1598306400

        // Add logs for all datasets in epoch 100
        for (const id of [id1, id2, id3, id4]) {
          await env.DB.prepare(
            `INSERT INTO retrieval_logs (timestamp, data_set_id, response_status, egress_bytes, cache_miss)
             VALUES (datetime(?, 'unixepoch'), ?, 200, 1000, 0)`,
          )
            .bind(epoch100Timestamp + 10, id)
            .run()
        }

        // Call with targetEpoch = 100 to get data up to epoch 100
        const usageData = await aggregateUsageData(env.DB, 100)

        // Should have data for id1, id2, id3 but NOT id4
        expect(usageData.has(id1)).toBe(true)
        expect(usageData.has(id2)).toBe(true)
        expect(usageData.has(id3)).toBe(true)
        expect(usageData.has(id4)).toBe(false)

        // Verify the data for included datasets
        expect(usageData.get(id1)).toEqual({
          cdnBytes: 1000,
          cacheMissBytes: 0,
          epoch: 100,
        })
      })
    })
  })

  describe('prepareBatchData', () => {
    it('should prepare batch data for contract call', () => {
      const usageData = new Map([
        ['1', { cdnBytes: 1000, cacheMissBytes: 500, epoch: 100 }],
        ['2', { cdnBytes: 2000, cacheMissBytes: 0, epoch: 100 }],
        ['3', { cdnBytes: 0, cacheMissBytes: 3000, epoch: 100 }],
      ])

      const batchData = prepareBatchData(usageData)

      expect(batchData).toEqual({
        dataSetIds: ['1', '2', '3'],
        epochs: [100, 100, 100],
        cdnBytesUsed: [1000n, 2000n, 0n],
        cacheMissBytesUsed: [500n, 0n, 3000n],
      })
    })

    it('should filter out datasets with zero usage', () => {
      const usageData = new Map([
        ['1', { cdnBytes: 1000, cacheMissBytes: 500, epoch: 100 }],
        ['2', { cdnBytes: 0, cacheMissBytes: 0, epoch: 100 }], // Zero usage
        ['3', { cdnBytes: 0, cacheMissBytes: 3000, epoch: 100 }],
      ])

      const batchData = prepareBatchData(usageData)

      expect(batchData.dataSetIds).toEqual(['1', '3'])
      expect(batchData.epochs).toEqual([100, 100])
      expect(batchData.cdnBytesUsed).toEqual([1000n, 0n])
      expect(batchData.cacheMissBytesUsed).toEqual([500n, 3000n])
    })

    it('should handle empty usage data', () => {
      const usageData = new Map()
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
