import { describe, it, expect, beforeEach } from 'vitest'
import { handleGetDataSetStats, handleGetPayerStats } from '../lib/handlers.js'
import { env } from 'cloudflare:test'
import { withDataSet, withRetrievalLog } from './test-helpers.js'

describe('stats-handlers', () => {
  beforeEach(async () => {
    await env.DB.exec('DELETE FROM data_sets')
  })

  describe('handleGetDataSetStats', () => {
    it('returns quotas for valid data set', async () => {
      const dataSetId = '1'
      await withDataSet(env, {
        dataSetId,
        serviceProviderId: '1',
        payerAddress: '0xPayerAddress',
        withCDN: true,
        cdnEgressQuota: 3000,
        cacheMissEgressQuota: 6000,
      })

      const res = await handleGetDataSetStats(env, dataSetId)

      expect(res.status).toBe(200)
      expect(res.headers.get('Content-Type')).toBe('application/json')

      const data = await res.json()
      expect(data).toStrictEqual({
        cdnEgressQuota: '3000',
        cacheMissEgressQuota: '6000',
      })
    })

    it('returns 404 for non-existent data set', async () => {
      const res = await handleGetDataSetStats(env, 'non-existent')

      expect(res.status).toBe(404)
      const text = await res.text()
      expect(text).toBe('Not Found')
    })

    it('handles null quota values', async () => {
      const dataSetId = '2'
      await withDataSet(env, {
        dataSetId,
        serviceProviderId: '2',
        payerAddress: '0xPayerAddress2',
        withCDN: false,
        cdnEgressQuota: null,
        cacheMissEgressQuota: null,
      })

      const res = await handleGetDataSetStats(env, dataSetId)

      expect(res.status).toBe(200)

      const data = await res.json()
      expect(data).toStrictEqual({
        cdnEgressQuota: '0',
        cacheMissEgressQuota: '0',
      })
    })
  })

  describe('handleGetPayerStats', () => {
    it('returns quotas for valid payer', async () => {
      const payerAddress = '0xpayeraddress'
      const dataSetId = '1'
      const egressBytes = 100
      await withDataSet(env, {
        dataSetId: '1',
        serviceProviderId: '1',
        payerAddress,
        withCDN: true,
        cdnEgressQuota: 3000,
        cacheMissEgressQuota: 6000,
      })
      await withRetrievalLog(env, {
        timestamp: new Date().toISOString(),
        dataSetId,
        egressBytes,
        cacheMiss: true,
      })

      const res = await handleGetPayerStats(env, payerAddress)

      expect(res.status).toBe(200)
      expect(res.headers.get('Content-Type')).toBe('application/json')

      const data = await res.json()
      expect(data).toStrictEqual({
        cacheMissEgressBytes: String(egressBytes),
        cacheMissRequests: '1',
        remainingCDNEgressBytes: '3000',
        remainingCacheMissEgressBytes: '6000',
        totalEgressBytes: String(egressBytes),
        totalRequests: '1',
      })
    })

    it('returns 404 for non-existent payer', async () => {
      const res = await handleGetPayerStats(env, 'non-existent')

      expect(res.status).toBe(404)
      const text = await res.text()
      expect(text).toBe('Not Found')
    })
  })
})
