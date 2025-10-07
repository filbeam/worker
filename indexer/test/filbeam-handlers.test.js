import { describe, it, expect } from 'vitest'
import { env } from 'cloudflare:test'
import { handleFilBeamUsageReported } from '../lib/filbeam-handlers.js'

// Helper function to create a dataset with optional last_rollup_reported_at_epoch
async function withDataSet(
  env,
  {
    id,
    lastReportedEpoch = null,
    withCDN = true,
    serviceProviderId = '1',
    payerAddress = '0xPayer',
  },
) {
  // First ensure service provider exists
  await env.DB.prepare(
    `INSERT OR IGNORE INTO service_providers (id, service_url) VALUES (?, ?)`,
  )
    .bind(String(serviceProviderId), 'https://example.com')
    .run()

  await env.DB.prepare(
    `INSERT INTO data_sets (
      id,
      service_provider_id,
      payer_address,
      with_cdn,
      last_rollup_reported_at_epoch
    )
    VALUES (?, ?, ?, ?, ?)`,
  )
    .bind(
      String(id),
      String(serviceProviderId),
      payerAddress,
      withCDN,
      lastReportedEpoch,
    )
    .run()
}

describe('filbeam-handlers', () => {
  describe('handleFilBeamUsageReported', () => {
    it('should update last_rollup_reported_at_epoch when new_epoch is greater', async () => {
      // Setup: Create a dataset with last_rollup_reported_at_epoch = 100
      await withDataSet(env, {
        id: '1',
        lastReportedEpoch: 100,
      })

      const payload = {
        data_set_id: '1',
        new_epoch: 150,
        cdn_bytes_used: '1000',
        cache_miss_bytes_used: '500',
      }

      const response = await handleFilBeamUsageReported(env, payload)

      expect(response.status).toBe(200)
      expect(await response.text()).toBe('OK')

      // Verify the epoch was updated
      const result = await env.DB.prepare(
        'SELECT last_rollup_reported_at_epoch FROM data_sets WHERE id = ?',
      )
        .bind('1')
        .first()

      expect(result.last_rollup_reported_at_epoch).toBe(150)
    })

    it('should update last_rollup_reported_at_epoch when current value is NULL', async () => {
      // Setup: Create a dataset with no last_rollup_reported_at_epoch
      await withDataSet(env, {
        id: '2',
        lastReportedEpoch: null,
      })

      const payload = {
        data_set_id: '2',
        new_epoch: 50,
        cdn_bytes_used: '2000',
        cache_miss_bytes_used: '1000',
      }

      const response = await handleFilBeamUsageReported(env, payload)

      expect(response.status).toBe(200)

      // Verify the epoch was updated
      const result = await env.DB.prepare(
        'SELECT last_rollup_reported_at_epoch FROM data_sets WHERE id = ?',
      )
        .bind('2')
        .first()

      expect(result.last_rollup_reported_at_epoch).toBe(50)
    })

    it('should return 400 when new_epoch is less than last_rollup_reported_at_epoch', async () => {
      // Setup: Create a dataset with last_rollup_reported_at_epoch = 100
      await withDataSet(env, {
        id: '3',
        lastReportedEpoch: 100,
      })

      const payload = {
        data_set_id: '3',
        new_epoch: 90, // Less than current
        cdn_bytes_used: '1000',
        cache_miss_bytes_used: '500',
      }

      const response = await handleFilBeamUsageReported(env, payload)

      expect(response.status).toBe(400)
      expect(await response.text()).toContain(
        'must be greater than last_rollup_reported_at_epoch',
      )

      // Verify the epoch was NOT updated
      const result = await env.DB.prepare(
        'SELECT last_rollup_reported_at_epoch FROM data_sets WHERE id = ?',
      )
        .bind('3')
        .first()

      expect(result.last_rollup_reported_at_epoch).toBe(100) // Should remain unchanged
    })

    it('should return 400 when new_epoch equals last_rollup_reported_at_epoch', async () => {
      // Setup: Create a dataset with last_rollup_reported_at_epoch = 100
      await withDataSet(env, {
        id: '4',
        lastReportedEpoch: 100,
      })

      const payload = {
        data_set_id: '4',
        new_epoch: 100, // Equal to current
        cdn_bytes_used: '1000',
        cache_miss_bytes_used: '500',
      }

      const response = await handleFilBeamUsageReported(env, payload)

      expect(response.status).toBe(400)
      expect(await response.text()).toContain(
        'must be greater than last_rollup_reported_at_epoch',
      )

      // Verify the epoch was NOT updated
      const result = await env.DB.prepare(
        'SELECT last_rollup_reported_at_epoch FROM data_sets WHERE id = ?',
      )
        .bind('4')
        .first()

      expect(result.last_rollup_reported_at_epoch).toBe(100) // Should remain unchanged
    })

    it('should return 200 but not update when dataset does not exist', async () => {
      const payload = {
        data_set_id: '999', // Non-existent dataset
        new_epoch: 200,
        cdn_bytes_used: '1000',
        cache_miss_bytes_used: '500',
      }

      const response = await handleFilBeamUsageReported(env, payload)

      expect(response.status).toBe(200) // Still returns OK
      expect(await response.text()).toBe('OK')

      // Verify no dataset was created
      const result = await env.DB.prepare(
        'SELECT * FROM data_sets WHERE id = ?',
      )
        .bind('999')
        .first()

      expect(result).toBeNull()
    })

    it('should handle numeric data_set_id', async () => {
      // Setup: Create a dataset
      await withDataSet(env, {
        id: '5',
        lastReportedEpoch: 10,
      })

      const payload = {
        data_set_id: 5, // Numeric instead of string
        new_epoch: 20,
        cdn_bytes_used: '1000',
        cache_miss_bytes_used: '500',
      }

      const response = await handleFilBeamUsageReported(env, payload)

      expect(response.status).toBe(200)

      // Verify the epoch was updated
      const result = await env.DB.prepare(
        'SELECT last_rollup_reported_at_epoch FROM data_sets WHERE id = ?',
      )
        .bind('5')
        .first()

      expect(result.last_rollup_reported_at_epoch).toBe(20)
    })
  })
})
