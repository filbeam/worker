import { describe, it, expect, beforeEach } from 'vitest'
import { env } from 'cloudflare:test'
import { getDataSetsForSettlement } from '../lib/rail-settlement.js'
import { withDataSet, randomId } from './test-helpers.js'

describe('rail settlement', () => {
  describe('getDataSetsForSettlement', () => {
    beforeEach(async () => {
      // Clear data_sets table before each test
      await env.DB.prepare('DELETE FROM data_sets').run()
    })

    it('should return data sets with with_cdn = true', async () => {
      const id1 = randomId()
      const id2 = randomId()
      const id3 = randomId()

      await withDataSet(env, { id: id1, withCDN: true })
      await withDataSet(env, { id: id2, withCDN: true })
      await withDataSet(env, { id: id3, withCDN: false })

      const currentEpoch = 1000000n
      const dataSetIds = await getDataSetsForSettlement(env.DB, currentEpoch)

      expect(dataSetIds).toHaveLength(2)
      expect(dataSetIds).toContain(id1)
      expect(dataSetIds).toContain(id2)
      expect(dataSetIds).not.toContain(id3)
    })

    it('should return terminated data sets within lockup_unlocks_at_epoch window', async () => {
      const id1 = randomId()
      const id2 = randomId()
      const id3 = randomId()

      const currentEpoch = 1000000n

      // Terminated but still within settlement window
      await withDataSet(env, {
        id: id1,
        withCDN: false,
        settleUpToEpoch: currentEpoch + 100n,
      })

      // Terminated and past settlement window
      await withDataSet(env, {
        id: id2,
        withCDN: false,
        settleUpToEpoch: currentEpoch - 100n,
      })

      // Active with CDN
      await withDataSet(env, { id: id3, withCDN: true })

      const dataSetIds = await getDataSetsForSettlement(env.DB, currentEpoch)

      expect(dataSetIds).toHaveLength(2)
      expect(dataSetIds).toContain(id1) // Still within window
      expect(dataSetIds).toContain(id3) // Active
      expect(dataSetIds).not.toContain(id2) // Past window
    })

    it('should handle mixed scenarios correctly', async () => {
      const id1 = randomId()
      const id2 = randomId()
      const id3 = randomId()
      const id4 = randomId()

      const currentEpoch = 1000000n

      // Active with CDN
      await withDataSet(env, { id: id1, withCDN: true })

      // Terminated but within window
      await withDataSet(env, {
        id: id2,
        withCDN: false,
        settleUpToEpoch: currentEpoch + 86400n,
      })

      // Terminated and past window
      await withDataSet(env, {
        id: id3,
        withCDN: false,
        settleUpToEpoch: currentEpoch - 1n,
      })

      // Inactive without CDN and no lockup_unlocks_at_epoch
      await withDataSet(env, { id: id4, withCDN: false })

      const dataSetIds = await getDataSetsForSettlement(env.DB, currentEpoch)

      expect(dataSetIds).toHaveLength(2)
      expect(dataSetIds).toContain(id1)
      expect(dataSetIds).toContain(id2)
      expect(dataSetIds).not.toContain(id3)
      expect(dataSetIds).not.toContain(id4)
    })

    it('should return empty array when no data sets match', async () => {
      const id1 = randomId()
      const currentEpoch = 1000000n

      // Only inactive data set
      await withDataSet(env, { id: id1, withCDN: false })

      const dataSetIds = await getDataSetsForSettlement(env.DB, currentEpoch)

      expect(dataSetIds).toHaveLength(0)
    })

    it('should exclude data sets with terminate_service_tx_hash set even if with_cdn is true', async () => {
      const id1 = randomId()
      const id2 = randomId()
      const id3 = randomId()

      // Active with CDN and no terminate_service_tx_hash
      await withDataSet(env, { id: id1, withCDN: true })

      // Active with CDN but has terminate_service_tx_hash
      await withDataSet(env, {
        id: id2,
        withCDN: true,
        terminateServiceTxHash: '0xabc123',
      })

      // Another one with CDN but terminated
      await withDataSet(env, {
        id: id3,
        withCDN: true,
        terminateServiceTxHash: '0xdef456',
      })

      const currentEpoch = 1000000n
      const dataSetIds = await getDataSetsForSettlement(env.DB, currentEpoch)

      expect(dataSetIds).toHaveLength(1)
      expect(dataSetIds).toContain(id1)
      expect(dataSetIds).not.toContain(id2)
      expect(dataSetIds).not.toContain(id3)
    })

    it('should exclude data sets with terminate_service_tx_hash set even within lockup_unlocks_at_epoch window', async () => {
      const id1 = randomId()
      const id2 = randomId()
      const id3 = randomId()

      const currentEpoch = 1000000n

      // Within settlement window and no terminate_service_tx_hash
      await withDataSet(env, {
        id: id1,
        withCDN: false,
        settleUpToEpoch: currentEpoch + 100n,
      })

      // Within settlement window but has terminate_service_tx_hash
      await withDataSet(env, {
        id: id2,
        withCDN: false,
        settleUpToEpoch: currentEpoch + 100n,
        terminateServiceTxHash: '0xabc123',
      })

      // Active with CDN and no terminate_service_tx_hash
      await withDataSet(env, { id: id3, withCDN: true })

      const dataSetIds = await getDataSetsForSettlement(env.DB, currentEpoch)

      expect(dataSetIds).toHaveLength(2)
      expect(dataSetIds).toContain(id1)
      expect(dataSetIds).toContain(id3)
      expect(dataSetIds).not.toContain(id2)
    })

    it('should handle mixed scenarios with terminate_service_tx_hash correctly', async () => {
      const id1 = randomId()
      const id2 = randomId()
      const id3 = randomId()
      const id4 = randomId()
      const id5 = randomId()

      const currentEpoch = 1000000n

      // Active with CDN, no terminate_service_tx_hash
      await withDataSet(env, { id: id1, withCDN: true })

      // Active with CDN but terminated
      await withDataSet(env, {
        id: id2,
        withCDN: true,
        terminateServiceTxHash: '0xabc123',
      })

      // Within settlement window, no terminate_service_tx_hash
      await withDataSet(env, {
        id: id3,
        withCDN: false,
        settleUpToEpoch: currentEpoch + 100n,
      })

      // Within settlement window but terminated
      await withDataSet(env, {
        id: id4,
        withCDN: false,
        settleUpToEpoch: currentEpoch + 100n,
        terminateServiceTxHash: '0xdef456',
      })

      // Inactive without CDN, no settlement window, no terminate_service_tx_hash
      await withDataSet(env, { id: id5, withCDN: false })

      const dataSetIds = await getDataSetsForSettlement(env.DB, currentEpoch)

      expect(dataSetIds).toHaveLength(2)
      expect(dataSetIds).toContain(id1)
      expect(dataSetIds).toContain(id3)
      expect(dataSetIds).not.toContain(id2)
      expect(dataSetIds).not.toContain(id4)
      expect(dataSetIds).not.toContain(id5)
    })
  })
})
