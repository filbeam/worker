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

    it('should return terminated data sets within settle_up_to_epoch window', async () => {
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

      // Inactive without CDN and no settle_up_to_epoch
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
  })
})
