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
  prepareUsageReportData,
  epochToTimestamp,
} from '../lib/usage-report.js'

describe('usage report', () => {
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

        const epoch99UnixTimestamp = filecoinEpochToTimestamp(99)
        const epoch100Timestamp = filecoinEpochToTimestamp(100)
        const epoch101Timestamp = filecoinEpochToTimestamp(101)

        await withRetrievalLog(env, {
          timestamp: epoch99UnixTimestamp,
          dataSetId: '1',
          egressBytes: 1000,
          cacheMiss: 0,
        })

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
          cacheMiss: 1,
        })

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

        const upToTimestamp = epochToTimestamp(
          100n,
          BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
        )
        const usageData = await aggregateUsageData(env.DB, upToTimestamp)

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

        const upToTimestamp = epochToTimestamp(
          100n,
          BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
        )
        const usageData = await aggregateUsageData(env.DB, upToTimestamp)

        expect(usageData).toStrictEqual([
          {
            data_set_id: '1',
            cdn_bytes: 1800,
            cache_miss_bytes: 300,
          },
        ])
      })

      it('should only aggregate data for datasets with usage_reported_until < upToTimestamp', async () => {
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

        await withDataSet(env, { id: '1' })
        await withDataSet(env, {
          id: '2',
          usageReportedUntil: epoch98TimestampISO,
        })
        await withDataSet(env, {
          id: '3',
          usageReportedUntil: epoch99TimestampISO,
        })
        await withDataSet(env, {
          id: '4',
          usageReportedUntil: epoch100TimestampISO,
        })

        const epoch100UnixTimestamp = filecoinEpochToTimestamp(100)

        for (const id of ['1', '2', '3', '4']) {
          await withRetrievalLog(env, {
            timestamp: epoch100UnixTimestamp,
            dataSetId: id,
            egressBytes: 1000,
            cacheMiss: 0,
          })
        }

        const upToTimestamp = epochToTimestamp(
          100n,
          BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
        )
        const usageData = await aggregateUsageData(env.DB, upToTimestamp)

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

        await withRetrievalLog(env, {
          timestamp: epoch100UnixTimestamp,
          dataSetId: '1',
          egressBytes: 1000,
          cacheMiss: 0,
        })

        await withRetrievalLog(env, {
          timestamp: epoch100UnixTimestamp,
          dataSetId: '2',
          egressBytes: null,
          cacheMiss: 0,
        })

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
          pendingUsageReportTxHash: null,
        })

        await withDataSet(env, {
          id: '2',
          usageReportedUntil: epoch99TimestampISO,
          pendingUsageReportTxHash: '0x123abc',
        })

        const epoch100UnixTimestamp = filecoinEpochToTimestamp(100)

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

      const batchData = prepareUsageReportData(
        usageData,
        BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
      )

      expect(batchData).toEqual({
        dataSetIds: ['1', '2', '3'],
        cdnBytesUsed: [1000n, 2000n, 0n],
        cacheMissBytesUsed: [500n, 0n, 3000n],
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

      const batchData = prepareUsageReportData(
        usageData,
        BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
      )

      expect(batchData).toEqual({
        dataSetIds: ['1', '2', '3'],
        cdnBytesUsed: [1000n, 2000n, 0n],
        cacheMissBytesUsed: [500n, 0n, 3000n],
      })
    })

    it('should handle empty usage data', () => {
      const usageData = []
      const batchData = prepareUsageReportData(
        usageData,
        BigInt(FILECOIN_GENESIS_UNIX_TIMESTAMP),
      )

      expect(batchData).toEqual({
        dataSetIds: [],
        cdnBytesUsed: [],
        cacheMissBytesUsed: [],
      })
    })
  })
})
