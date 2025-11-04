import { describe, it, expect, beforeEach } from 'vitest'
import { handleGetDataSetStats } from '../lib/handlers.js'
import { env } from 'cloudflare:test'
import { withDataSet } from './test-helpers.js'

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
        cdn_egress_quota: '3000',
        cache_miss_egress_quota: '6000',
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
        cdn_egress_quota: '0',
        cache_miss_egress_quota: '0',
      })
    })
  })
})
