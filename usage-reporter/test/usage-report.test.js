import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { applyD1Migrations, env } from 'cloudflare:test'
import {
  aggregateUsageData,
  prepareUsageReportData,
} from '../lib/usage-report.js'
import {
  withDataSet,
  withRetrievalLog,
  EPOCH_100_TIMESTAMP,
  EPOCH_98_TIMESTAMP_ISO,
  EPOCH_99_TIMESTAMP_ISO,
  EPOCH_100_TIMESTAMP_ISO,
  EPOCH_101_TIMESTAMP_ISO,
} from './test-helpers.js'

describe('usage report', () => {
  describe('database operations', () => {
    beforeEach(async () => {
      await applyD1Migrations(env.DB, env.TEST_MIGRATIONS)
    })

    afterEach(async () => {
      await env.DB.exec('DELETE FROM retrieval_logs;')
      await env.DB.exec('DELETE FROM data_sets;')
    })

    describe('aggregateUsageData', () => {
      it('should aggregate usage data by cache miss status', async () => {
        await withDataSet(env, {
          id: '1',
          usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
        })
        await withDataSet(env, {
          id: '2',
          usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
        })

        await withRetrievalLog(env, {
          timestamp: EPOCH_99_TIMESTAMP_ISO,
          dataSetId: '1',
          egressBytes: 1000,
          cacheMiss: 0,
        })

        await withRetrievalLog(env, {
          timestamp: EPOCH_100_TIMESTAMP_ISO,
          dataSetId: '1',
          egressBytes: 2000,
          cacheMiss: 0,
        })

        await withRetrievalLog(env, {
          timestamp: EPOCH_100_TIMESTAMP_ISO,
          dataSetId: '1',
          egressBytes: 500,
          cacheMiss: 1,
        })

        await withRetrievalLog(env, {
          timestamp: EPOCH_100_TIMESTAMP_ISO,
          dataSetId: '2',
          egressBytes: 3000,
          cacheMiss: 1,
        })

        await withRetrievalLog(env, {
          timestamp: EPOCH_101_TIMESTAMP_ISO,
          dataSetId: '1',
          egressBytes: 9999,
          cacheMiss: 0,
        })

        await withRetrievalLog(env, {
          timestamp: EPOCH_101_TIMESTAMP_ISO,
          dataSetId: '2',
          egressBytes: 9999,
          cacheMiss: 0,
        })

        const usageData = await aggregateUsageData(env.DB, EPOCH_100_TIMESTAMP)

        expect(usageData).toStrictEqual([
          {
            data_set_id: '1',
            cdn_bytes: 2500,
            cache_miss_bytes: 500,
          },
          {
            data_set_id: '2',
            cdn_bytes: 3000,
            cache_miss_bytes: 3000,
          },
        ])
      })

      it('should include non-200 responses but filter out null egress_bytes', async () => {
        await withDataSet(env, {
          id: '1',
          usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
        })

        await withRetrievalLog(env, {
          timestamp: EPOCH_100_TIMESTAMP_ISO,
          dataSetId: '1',
          responseStatus: 404,
          egressBytes: 1000,
          cacheMiss: 0,
        })

        await withRetrievalLog(env, {
          timestamp: EPOCH_100_TIMESTAMP_ISO,
          dataSetId: '1',
          responseStatus: 200,
          egressBytes: null,
          cacheMiss: 0,
        })

        await withRetrievalLog(env, {
          timestamp: EPOCH_100_TIMESTAMP_ISO,
          dataSetId: '1',
          responseStatus: 200,
          egressBytes: 500,
          cacheMiss: 0,
        })

        await withRetrievalLog(env, {
          timestamp: EPOCH_100_TIMESTAMP_ISO,
          dataSetId: '1',
          responseStatus: 500,
          egressBytes: 300,
          cacheMiss: 1,
        })

        const usageData = await aggregateUsageData(env.DB, EPOCH_100_TIMESTAMP)

        expect(usageData).toStrictEqual([
          {
            data_set_id: '1',
            cdn_bytes: 1800,
            cache_miss_bytes: 300,
          },
        ])
      })

      it('should only aggregate data for datasets with usage_reported_until < upToTimestamp', async () => {
        await withDataSet(env, { id: '1' })
        await withDataSet(env, {
          id: '2',
          usageReportedUntil: EPOCH_98_TIMESTAMP_ISO,
        })
        await withDataSet(env, {
          id: '3',
          usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
        })
        await withDataSet(env, {
          id: '4',
          usageReportedUntil: EPOCH_100_TIMESTAMP_ISO,
        })

        for (const id of ['1', '2', '3', '4']) {
          await withRetrievalLog(env, {
            timestamp: EPOCH_100_TIMESTAMP_ISO,
            dataSetId: id,
            egressBytes: 1000,
            cacheMiss: 0,
          })
        }

        const usageData = await aggregateUsageData(env.DB, EPOCH_100_TIMESTAMP)

        expect(usageData).toStrictEqual([
          {
            data_set_id: '1',
            cdn_bytes: 1000,
            cache_miss_bytes: 0,
          },
          {
            data_set_id: '2',
            cdn_bytes: 1000,
            cache_miss_bytes: 0,
          },
          {
            data_set_id: '3',
            cdn_bytes: 1000,
            cache_miss_bytes: 0,
          },
        ])
      })

      it('should filter out datasets with zero cdn and cache-miss bytes', async () => {
        await withDataSet(env, {
          id: '1',
          usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
        })
        await withDataSet(env, {
          id: '2',
          usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
        })
        await withDataSet(env, {
          id: '3',
          usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
        })

        await withRetrievalLog(env, {
          timestamp: EPOCH_100_TIMESTAMP_ISO,
          dataSetId: '1',
          egressBytes: 1000,
          cacheMiss: 0,
        })

        await withRetrievalLog(env, {
          timestamp: EPOCH_100_TIMESTAMP_ISO,
          dataSetId: '2',
          egressBytes: null,
          cacheMiss: 0,
        })

        await withRetrievalLog(env, {
          timestamp: EPOCH_100_TIMESTAMP_ISO,
          dataSetId: '3',
          egressBytes: 500,
          cacheMiss: 1,
        })

        const usageData = await aggregateUsageData(env.DB, EPOCH_100_TIMESTAMP)

        expect(usageData).toStrictEqual([
          {
            data_set_id: '1',
            cdn_bytes: 1000,
            cache_miss_bytes: 0,
          },
          {
            data_set_id: '3',
            cdn_bytes: 500,
            cache_miss_bytes: 500,
          },
        ])
      })

      it('should exclude datasets with pending usage report transactions', async () => {
        await withDataSet(env, {
          id: '1',
          usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
          pendingUsageReportTxHash: null,
        })

        await withDataSet(env, {
          id: '2',
          usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
          pendingUsageReportTxHash: '0x123abc',
        })

        await withRetrievalLog(env, {
          timestamp: EPOCH_100_TIMESTAMP_ISO,
          dataSetId: '1',
          egressBytes: 1000,
          cacheMiss: 0,
        })

        await withRetrievalLog(env, {
          timestamp: EPOCH_100_TIMESTAMP_ISO,
          dataSetId: '2',
          egressBytes: 2000,
          cacheMiss: 0,
        })

        const usageData = await aggregateUsageData(env.DB, EPOCH_100_TIMESTAMP)

        expect(usageData).toStrictEqual([
          {
            data_set_id: '1',
            cdn_bytes: 1000,
            cache_miss_bytes: 0,
          },
        ])
      })
    })
  })

  describe('prepareUsageReportData', () => {
    it('should prepare batch data for contract call', () => {
      const usageData = [
        {
          data_set_id: '1',
          cdn_bytes: 1000,
          cache_miss_bytes: 500,
        },
        {
          data_set_id: '2',
          cdn_bytes: 2000,
          cache_miss_bytes: 0,
        },
        {
          data_set_id: '3',
          cdn_bytes: 0,
          cache_miss_bytes: 3000,
        },
      ]

      const batchData = prepareUsageReportData(usageData)

      expect(batchData).toEqual({
        dataSetIds: ['1', '2', '3'],
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
        },
        {
          data_set_id: '2',
          cdn_bytes: 2000,
          cache_miss_bytes: 0,
        },
        {
          data_set_id: '3',
          cdn_bytes: 0,
          cache_miss_bytes: 3000,
        },
      ]

      const batchData = prepareUsageReportData(usageData)

      expect(batchData).toEqual({
        dataSetIds: ['1', '2', '3'],
        cdnBytesUsed: [1000n, 2000n, 0n],
        cacheMissBytesUsed: [500n, 0n, 3000n],
      })
    })

    it('should handle empty usage data', () => {
      const usageData = []
      const batchData = prepareUsageReportData(usageData)

      expect(batchData).toEqual({
        dataSetIds: [],
        cdnBytesUsed: [],
        cacheMissBytesUsed: [],
      })
    })
  })
})
