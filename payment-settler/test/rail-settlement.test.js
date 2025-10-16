import { describe, it, expect, beforeEach } from 'vitest'
import { env } from 'cloudflare:test'
import { getDataSetsForSettlement } from '../lib/rail-settlement.js'
import { withDataSet, createNextId, getDaysAgo } from './test-helpers.js'

const nextId = createNextId()

describe('rail settlement', () => {
  describe('getDataSetsForSettlement', () => {
    beforeEach(async () => {
      // Clear data_sets table before each test
      await env.DB.prepare('DELETE FROM data_sets').run()
    })

    it('should return data sets with with_cdn = true', async () => {
      const id1 = nextId()
      const id2 = nextId()
      const id3 = nextId()

      await withDataSet(env, {
        id: id1,
        withCDN: true,
        usageReportedUntil: getDaysAgo(5),
      })
      await withDataSet(env, {
        id: id2,
        withCDN: true,
        usageReportedUntil: getDaysAgo(10),
      })
      await withDataSet(env, {
        id: id3,
        withCDN: false,
        usageReportedUntil: getDaysAgo(5),
      })

      const dataSetIds = await getDataSetsForSettlement(env.DB)

      expect(dataSetIds).toStrictEqual([id1, id2])
    })

    it('should return terminated data sets within lockup_unlocks_at window', async () => {
      const id1 = nextId()
      const id2 = nextId()
      const id3 = nextId()

      // Terminated but still within settlement window
      await withDataSet(env, {
        id: id1,
        withCDN: false,
        lockupUnlocksAt: getDaysAgo(-10),
        usageReportedUntil: getDaysAgo(7),
      })

      // Terminated and past settlement window
      await withDataSet(env, {
        id: id2,
        withCDN: false,
        lockupUnlocksAt: getDaysAgo(1),
        usageReportedUntil: getDaysAgo(7),
      })

      // Active with CDN
      await withDataSet(env, {
        id: id3,
        withCDN: true,
        usageReportedUntil: getDaysAgo(15),
      })

      const dataSetIds = await getDataSetsForSettlement(env.DB)

      expect(dataSetIds).toStrictEqual([id1, id3])
    })

    it('should handle mixed scenarios correctly', async () => {
      const id1 = nextId()
      const id2 = nextId()
      const id3 = nextId()
      const id4 = nextId()

      // Active with CDN
      await withDataSet(env, {
        id: id1,
        withCDN: true,
        usageReportedUntil: getDaysAgo(20),
      })

      // Terminated but within window
      await withDataSet(env, {
        id: id2,
        withCDN: false,
        lockupUnlocksAt: getDaysAgo(-30),
        usageReportedUntil: getDaysAgo(10),
      })

      // Terminated and past window
      await withDataSet(env, {
        id: id3,
        withCDN: false,
        lockupUnlocksAt: getDaysAgo(1),
        usageReportedUntil: getDaysAgo(5),
      })

      // Inactive without CDN and no lockup_unlocks_at
      await withDataSet(env, {
        id: id4,
        withCDN: false,
        usageReportedUntil: getDaysAgo(5),
      })

      const dataSetIds = await getDataSetsForSettlement(env.DB)

      expect(dataSetIds).toStrictEqual([id1, id2])
    })

    it('should return empty array when no data sets match', async () => {
      const id1 = nextId()

      // Only inactive data set with recent usage
      await withDataSet(env, {
        id: id1,
        withCDN: false,
        usageReportedUntil: getDaysAgo(5),
      })

      const dataSetIds = await getDataSetsForSettlement(env.DB)

      expect(dataSetIds).toStrictEqual([])
    })

    it('should exclude data sets with terminate_service_tx_hash set even if with_cdn is true', async () => {
      const id1 = nextId()
      const id2 = nextId()
      const id3 = nextId()

      // Active with CDN and no terminate_service_tx_hash
      await withDataSet(env, {
        id: id1,
        withCDN: true,
        usageReportedUntil: getDaysAgo(12),
      })

      // Active with CDN but has terminate_service_tx_hash
      await withDataSet(env, {
        id: id2,
        withCDN: true,
        terminateServiceTxHash: '0xabc123',
        usageReportedUntil: getDaysAgo(8),
      })

      // Another one with CDN but terminated
      await withDataSet(env, {
        id: id3,
        withCDN: true,
        terminateServiceTxHash: '0xdef456',
        usageReportedUntil: getDaysAgo(3),
      })

      const dataSetIds = await getDataSetsForSettlement(env.DB)

      expect(dataSetIds).toStrictEqual([id1])
    })

    it('should exclude data sets with terminate_service_tx_hash set even within lockup_unlocks_at window', async () => {
      const id1 = nextId()
      const id2 = nextId()
      const id3 = nextId()

      // Within settlement window and no terminate_service_tx_hash
      await withDataSet(env, {
        id: id1,
        withCDN: false,
        lockupUnlocksAt: getDaysAgo(-10),
        usageReportedUntil: getDaysAgo(7),
      })

      // Within settlement window but has terminate_service_tx_hash
      await withDataSet(env, {
        id: id2,
        withCDN: false,
        lockupUnlocksAt: getDaysAgo(-10),
        terminateServiceTxHash: '0xabc123',
        usageReportedUntil: getDaysAgo(2),
      })

      // Active with CDN and no terminate_service_tx_hash
      await withDataSet(env, {
        id: id3,
        withCDN: true,
        usageReportedUntil: getDaysAgo(25),
      })

      const dataSetIds = await getDataSetsForSettlement(env.DB)

      expect(dataSetIds).toStrictEqual([id1, id3])
    })

    it('should handle mixed scenarios with terminate_service_tx_hash correctly', async () => {
      const id1 = nextId()
      const id2 = nextId()
      const id3 = nextId()
      const id4 = nextId()
      const id5 = nextId()

      // Active with CDN, no terminate_service_tx_hash, recent usage
      await withDataSet(env, {
        id: id1,
        withCDN: true,
        usageReportedUntil: getDaysAgo(5),
      })

      // Active with CDN but terminated
      await withDataSet(env, {
        id: id2,
        withCDN: true,
        terminateServiceTxHash: '0xabc123',
        usageReportedUntil: getDaysAgo(5),
      })

      // Within settlement window, no terminate_service_tx_hash, recent usage
      await withDataSet(env, {
        id: id3,
        withCDN: false,
        lockupUnlocksAt: getDaysAgo(-10),
        usageReportedUntil: getDaysAgo(10),
      })

      // Within settlement window but terminated
      await withDataSet(env, {
        id: id4,
        withCDN: false,
        lockupUnlocksAt: getDaysAgo(-10),
        terminateServiceTxHash: '0xdef456',
        usageReportedUntil: getDaysAgo(5),
      })

      // Inactive without CDN, no settlement window, no terminate_service_tx_hash
      await withDataSet(env, { id: id5, withCDN: false })

      const dataSetIds = await getDataSetsForSettlement(env.DB)

      expect(dataSetIds).toStrictEqual([id1, id3])
    })

    it('should exclude data sets with no recent usage (older than 30 days)', async () => {
      const id1 = nextId()
      const id2 = nextId()
      const id3 = nextId()

      // Active with CDN, recent usage (5 days ago)
      await withDataSet(env, {
        id: id1,
        withCDN: true,
        usageReportedUntil: getDaysAgo(5),
      })

      // Active with CDN, old usage (45 days ago)
      await withDataSet(env, {
        id: id2,
        withCDN: true,
        usageReportedUntil: getDaysAgo(45),
      })

      // Active with CDN, borderline usage (exactly 30 days ago)
      await withDataSet(env, {
        id: id3,
        withCDN: true,
        usageReportedUntil: getDaysAgo(30),
      })

      const dataSetIds = await getDataSetsForSettlement(env.DB)

      expect(dataSetIds).toStrictEqual([id1, id3])
    })

    it('should exclude data sets with zero usage (never reported)', async () => {
      const id1 = nextId()
      const id2 = nextId()
      const id3 = nextId()

      // Active with CDN, recent usage
      await withDataSet(env, {
        id: id1,
        withCDN: true,
        usageReportedUntil: getDaysAgo(5),
      })

      // Active with CDN, default (1970) usage_reported_until (never reported)
      await withDataSet(env, {
        id: id2,
        withCDN: true,
        usageReportedUntil: '1970-01-01T00:00:00.000Z',
      })

      // Active with CDN, usage_reported_until not set (defaults to 1970)
      await withDataSet(env, { id: id3, withCDN: true })

      const dataSetIds = await getDataSetsForSettlement(env.DB)

      expect(dataSetIds).toStrictEqual([id1])
    })

    it('should handle lockup_unlocks_at with usage_reported_until correctly', async () => {
      const id1 = nextId()
      const id2 = nextId()
      const id3 = nextId()
      const id4 = nextId()

      // Within settlement window AND recent usage
      await withDataSet(env, {
        id: id1,
        withCDN: false,
        lockupUnlocksAt: getDaysAgo(-10),
        usageReportedUntil: getDaysAgo(10),
      })

      // Within settlement window BUT old usage
      await withDataSet(env, {
        id: id2,
        withCDN: false,
        lockupUnlocksAt: getDaysAgo(-10),
        usageReportedUntil: getDaysAgo(45),
      })

      // Past settlement window BUT recent usage (shouldn't be included)
      await withDataSet(env, {
        id: id3,
        withCDN: false,
        lockupUnlocksAt: getDaysAgo(1),
        usageReportedUntil: getDaysAgo(5),
      })

      // Active with CDN AND recent usage
      await withDataSet(env, {
        id: id4,
        withCDN: true,
        usageReportedUntil: getDaysAgo(15),
      })

      const dataSetIds = await getDataSetsForSettlement(env.DB)

      expect(dataSetIds).toStrictEqual([id1, id4])
    })
  })
})
