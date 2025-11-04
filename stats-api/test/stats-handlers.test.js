import { describe, it, expect, beforeEach } from 'vitest'
import { handleGetDataSetStats } from '../lib/stats-handlers.js'
import { env } from 'cloudflare:test'

describe('stats-handlers', () => {
  beforeEach(async () => {
    // Clean up database before each test
    await env.DB.batch([env.DB.prepare('DELETE FROM data_sets')])
  })

  describe('handleGetDataSetStats', () => {
    it('returns quotas for valid data set', async () => {
      // Setup test data
      const dataSetId = '1'
      await env.DB.prepare(
        'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
      )
        .bind(dataSetId, '1', '0xPayerAddress', true, 3000, 6000)
        .run()

      // Get stats
      const res = await handleGetDataSetStats(env, dataSetId)

      // Assert response
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
      // Setup test data with null quotas
      const dataSetId = '2'
      await env.DB.prepare(
        'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
      )
        .bind(dataSetId, '2', '0xPayerAddress2', false, null, null)
        .run()

      // Get stats
      const res = await handleGetDataSetStats(env, dataSetId)

      // Assert response
      expect(res.status).toBe(200)

      const data = await res.json()
      expect(data).toStrictEqual({
        cdn_egress_quota: '0',
        cache_miss_egress_quota: '0',
      })
    })
  })
})
