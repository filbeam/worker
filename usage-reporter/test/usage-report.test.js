import { describe, it, expect, afterEach } from 'vitest'
import { env } from 'cloudflare:test'
import {
  aggregateUsageData,
  aggregateUsageByDataSet,
  prepareUsageReportData,
} from '../lib/usage-report.js'
import {
  withDataSet,
  withRetrievalLog,
  EPOCH_100_TIMESTAMP_MS,
  EPOCH_98_TIMESTAMP_ISO,
  EPOCH_99_TIMESTAMP_ISO,
  EPOCH_100_TIMESTAMP_ISO,
  EPOCH_101_TIMESTAMP_ISO,
} from './test-helpers.js'

describe('usage report', () => {
  describe('database operations', () => {
    afterEach(async () => {
      await env.DB.exec('DELETE FROM retrieval_logs')
      await env.DB.exec('DELETE FROM data_sets')
    })

    describe('aggregateUsageData', () => {
      it('aggregates cdn and cache-miss bytes per data set', async () => {
        await withDataSet(env, {
          id: '1',
          cdnRailId: 'rail-1',
          usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
        })
        await withDataSet(env, {
          id: '2',
          cdnRailId: 'rail-2',
          usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
        })

        // Excluded: timestamp equals usage_reported_until (not strictly greater)
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
          cacheMissResponseValid: 1,
        })

        await withRetrievalLog(env, {
          timestamp: EPOCH_100_TIMESTAMP_ISO,
          dataSetId: '2',
          egressBytes: 3000,
          cacheMiss: 1,
          cacheMissResponseValid: 1,
        })

        // Excluded: timestamp after the target
        await withRetrievalLog(env, {
          timestamp: EPOCH_101_TIMESTAMP_ISO,
          dataSetId: '1',
          egressBytes: 9999,
          cacheMiss: 0,
        })

        const usageData = await aggregateUsageData(
          env.DB,
          EPOCH_100_TIMESTAMP_MS,
        )

        expect(usageData).toStrictEqual({
          usageByDataSet: [
            { data_set_id: '1', cdn_bytes: 2500, cache_miss_bytes: 500 },
            { data_set_id: '2', cdn_bytes: 3000, cache_miss_bytes: 3000 },
          ],
          dataSetIds: ['1', '2'],
        })
      })

      it('reports each member of a shared CDN group as its own data set', async () => {
        await withDataSet(env, {
          id: '1',
          cdnRailId: 'shared-rail',
          usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
        })
        await withDataSet(env, {
          id: '2',
          cdnRailId: 'shared-rail',
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
          egressBytes: 2000,
          cacheMiss: 1,
          cacheMissResponseValid: 1,
        })

        const usageData = await aggregateUsageData(
          env.DB,
          EPOCH_100_TIMESTAMP_MS,
        )

        // Bandwidth is aggregated onto the shared rail on-chain; the worker
        // reports per data set so each contributes its own egress.
        expect(usageData).toStrictEqual({
          usageByDataSet: [
            { data_set_id: '1', cdn_bytes: 1000, cache_miss_bytes: 0 },
            { data_set_id: '2', cdn_bytes: 2000, cache_miss_bytes: 2000 },
          ],
          dataSetIds: ['1', '2'],
        })
      })
    })

    describe('aggregateUsageByDataSet', () => {
      it('should include non-200 responses but filter out null egress_bytes', async () => {
        await withDataSet(env, {
          id: '1',
          cdnRailId: 'rail-1',
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
          cacheMissResponseValid: 1,
        })

        const usage = await aggregateUsageByDataSet(
          env.DB,
          EPOCH_100_TIMESTAMP_MS,
        )

        expect(usage).toStrictEqual([
          { data_set_id: '1', cdn_bytes: 1800, cache_miss_bytes: 300 },
        ])
      })

      it('should only aggregate data for datasets with usage_reported_until < upToTimestamp', async () => {
        await withDataSet(env, { id: '1', cdnRailId: 'rail-1' })
        await withDataSet(env, {
          id: '2',
          cdnRailId: 'rail-2',
          usageReportedUntil: EPOCH_98_TIMESTAMP_ISO,
        })
        await withDataSet(env, {
          id: '3',
          cdnRailId: 'rail-3',
          usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
        })
        await withDataSet(env, {
          id: '4',
          cdnRailId: 'rail-4',
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

        const usage = await aggregateUsageByDataSet(
          env.DB,
          EPOCH_100_TIMESTAMP_MS,
        )

        expect(usage).toStrictEqual([
          { data_set_id: '1', cdn_bytes: 1000, cache_miss_bytes: 0 },
          { data_set_id: '2', cdn_bytes: 1000, cache_miss_bytes: 0 },
          { data_set_id: '3', cdn_bytes: 1000, cache_miss_bytes: 0 },
        ])
      })

      it('should exclude datasets with pending usage report transactions', async () => {
        await withDataSet(env, {
          id: '1',
          cdnRailId: 'rail-1',
          usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
          pendingUsageReportTxHash: null,
        })

        await withDataSet(env, {
          id: '2',
          cdnRailId: 'rail-2',
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

        const usage = await aggregateUsageByDataSet(
          env.DB,
          EPOCH_100_TIMESTAMP_MS,
        )

        expect(usage).toStrictEqual([
          { data_set_id: '1', cdn_bytes: 1000, cache_miss_bytes: 0 },
        ])
      })

      it('should only count valid cache miss responses', async () => {
        await withDataSet(env, {
          id: '1',
          usageReportedUntil: EPOCH_99_TIMESTAMP_ISO,
          pendingUsageReportTxHash: null,
        })

        await withRetrievalLog(env, {
          timestamp: EPOCH_100_TIMESTAMP_ISO,
          dataSetId: '1',
          egressBytes: 1000,
          cacheMiss: 1,
          cacheMissResponseValid: 0,
        })

        const usage = await aggregateUsageByDataSet(
          env.DB,
          EPOCH_100_TIMESTAMP_MS,
        )

        // Egress still counts toward bandwidth, but invalid cache-miss does not
        expect(usage).toStrictEqual([
          { data_set_id: '1', cdn_bytes: 1000, cache_miss_bytes: 0 },
        ])
      })
    })
  })

  describe('prepareUsageReportData', () => {
    it('produces parallel per-data-set arrays for recordUsageRollups', () => {
      const usageData = {
        usageByDataSet: [
          { data_set_id: '1', cdn_bytes: 1000, cache_miss_bytes: 500 },
          { data_set_id: '3', cdn_bytes: 2000, cache_miss_bytes: 3000 },
        ],
        dataSetIds: ['1', '3'],
      }

      const batchData = prepareUsageReportData(usageData)

      expect(batchData).toStrictEqual({
        dataSetIds: ['1', '3'],
        cdnBytesUsed: [1000n, 2000n],
        cacheMissBytesUsed: [500n, 3000n],
      })
    })
  })
})
