import { describe, it, beforeEach, afterEach, expect } from 'vitest'
import { env } from 'cloudflare:test'
import { handleFWSSCDNPaymentRailsToppedUp } from '../lib/fwss-handlers.js'
import { withDataSet, withServiceProvider } from './test-helpers.js'
import { BYTES_PER_TIB } from '../lib/constants.js'

describe('handleFWSSCDNPaymentRailsToppedUp', () => {
  let testServiceProviderId
  let testDataSetId

  beforeEach(async () => {
    // Clean up test data
    await env.DB.exec('DELETE FROM data_sets')
    await env.DB.exec('DELETE FROM service_providers')

    // Create test service provider and data set using helpers
    testServiceProviderId = await withServiceProvider(env, {
      serviceProviderId: 'test-provider-1',
    })

    testDataSetId = await withDataSet(env, {
      dataSetId: 'test-data-set-1',
      serviceProviderId: testServiceProviderId,
      payerAddress: '0xtest',
      withCDN: true,
    })
  })

  afterEach(async () => {
    // Clean up test data
    await env.DB.exec('DELETE FROM data_sets')
    await env.DB.exec('DELETE FROM service_providers')
  })

  it('calculates and stores egress quotas correctly', async () => {
    const testEnv = {
      ...env,
      CDN_RATE_PER_TIB: '5000000000000000000', // 5 USDFC per TiB
      CACHE_MISS_RATE_PER_TIB: '5000000000000000000', // Same rate for cache miss
    }

    const payload = {
      data_set_id: testDataSetId,
      cdn_amount_added: '5000000000000000000', // 5 USDFC = 1 TiB quota
      cache_miss_amount_added: '10000000000000000000', // 10 USDFC = 2 TiB quota
    }

    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload)

    // Verify the quotas were incremented correctly
    const result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(testDataSetId)
      .first()

    expect(result.cdn_egress_quota).toBe(Number(BYTES_PER_TIB)) // 1 TiB in bytes
    expect(result.cache_miss_egress_quota).toBe(Number(BYTES_PER_TIB * 2n)) // 2 TiB in bytes
  })

  it('handles zero amounts added', async () => {
    const testEnv = {
      ...env,
      CDN_RATE_PER_TIB: '5000000000000000000',
      CACHE_MISS_RATE_PER_TIB: '5000000000000000000',
    }

    const payload = {
      data_set_id: testDataSetId,
      cdn_amount_added: '0',
      cache_miss_amount_added: '0',
    }

    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload)

    const result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(testDataSetId)
      .first()

    expect(result.cdn_egress_quota).toBe(0)
    expect(result.cache_miss_egress_quota).toBe(0)
  })

  it('accumulates quota values on multiple top-ups', async () => {
    const testEnv = {
      ...env,
      CDN_RATE_PER_TIB: '5000000000000000000',
      CACHE_MISS_RATE_PER_TIB: '5000000000000000000',
    }

    // First top-up - 1 USDFC = 0.2 TiB
    const payload1 = {
      data_set_id: testDataSetId,
      cdn_amount_added: '1000000000000000000', // 1 USDFC
      cache_miss_amount_added: '2000000000000000000', // 2 USDFC
    }

    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload1)

    let result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(testDataSetId)
      .first()

    const expectedQuota1 = Number(BYTES_PER_TIB / 5n) // 0.2 TiB
    const expectedQuota2 = Number((BYTES_PER_TIB * 2n) / 5n) // 0.4 TiB
    expect(result.cdn_egress_quota).toBe(expectedQuota1)
    expect(result.cache_miss_egress_quota).toBe(expectedQuota2)

    // Second top-up with different values (should add, not replace)
    const payload2 = {
      data_set_id: testDataSetId,
      cdn_amount_added: '5000000000000000000', // 5 USDFC = 1 TiB more
      cache_miss_amount_added: '10000000000000000000', // 10 USDFC = 2 TiB more
    }

    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload2)

    result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(testDataSetId)
      .first()

    // Should be accumulated: 0.2 + 1 = 1.2 TiB for CDN, 0.4 + 2 = 2.4 TiB for cache miss
    const expectedAccumulatedCdn = expectedQuota1 + Number(BYTES_PER_TIB)
    const expectedAccumulatedCacheMiss =
      expectedQuota2 + Number(BYTES_PER_TIB * 2n)
    expect(result.cdn_egress_quota).toBe(expectedAccumulatedCdn)
    expect(result.cache_miss_egress_quota).toBe(expectedAccumulatedCacheMiss)
  })

  it('handles missing data set gracefully', async () => {
    const testEnv = {
      ...env,
      CDN_RATE_PER_TIB: '5000000000000000000',
      CACHE_MISS_RATE_PER_TIB: '5000000000000000000',
    }

    const payload = {
      data_set_id: 'non-existent-data-set',
      cdn_amount_added: '5000000000000000000',
      cache_miss_amount_added: '10000000000000000000',
    }

    // Should not throw, just log warning
    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload)

    // Verify no data was created for non-existent data set
    const result = await env.DB.prepare('SELECT * FROM data_sets WHERE id = ?')
      .bind('non-existent-data-set')
      .first()

    expect(result).toBeNull()
  })
})
