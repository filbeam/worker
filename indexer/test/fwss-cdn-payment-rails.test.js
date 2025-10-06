import { describe, it, beforeEach, afterEach, expect } from 'vitest'
import { env } from 'cloudflare:test'
import { handleFWSSCDNPaymentRailsToppedUp } from '../lib/fwss-handlers.js'
import { withDataSet, withServiceProvider } from './test-helpers.js'
import { formatUsdfcAmount, BYTES_PER_TIB } from '../lib/rate-helpers.js'

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
      CDN_RATE_DOLLARS_PER_TIB: '5', // $5 per TiB
      CACHE_MISS_RATE_DOLLARS_PER_TIB: '5', // Same rate for cache miss
    }

    const payload = {
      data_set_id: testDataSetId,
      total_cdn_lockup: formatUsdfcAmount(5), // 5 USDFC = 1 TiB quota
      total_cache_miss_lockup: formatUsdfcAmount(10), // 10 USDFC = 2 TiB quota
    }

    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload)

    // Verify the quotas were set correctly
    const result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(testDataSetId)
      .first()

    expect(result.cdn_egress_quota).toBe(BYTES_PER_TIB.toString()) // 1 TiB in bytes
    expect(result.cache_miss_egress_quota).toBe((BYTES_PER_TIB * 2n).toString()) // 2 TiB in bytes
  })

  it('handles uint256 large numbers correctly', async () => {
    const testEnv = {
      ...env,
      CDN_RATE_DOLLARS_PER_TIB: '1', // $1 per TiB
      CACHE_MISS_RATE_DOLLARS_PER_TIB: '2', // $2 per TiB
    }

    const payload = {
      data_set_id: testDataSetId,
      // Very large uint256 values
      total_cdn_lockup:
        '115792089237316195423570985008687907853269984665640564039457584007913129639935', // Max uint256
      total_cache_miss_lockup: '100000000000000000000000000000000', // 1e32
    }

    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload)

    const result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(testDataSetId)
      .first()

    // Verify large number calculations
    // With $1 per TiB: max_uint256 / (1e18) * BYTES_PER_TIB
    expect(result.cdn_egress_quota).toBe(
      '127314748520905380391777855525586135065716774604121015664758778084648831',
    )
    // With $2 per TiB: 1e32 / (2e18) * BYTES_PER_TIB
    expect(result.cache_miss_egress_quota).toBe('54975581388800000000000000')
  })

  it('handles zero lockup amounts', async () => {
    const testEnv = {
      ...env,
      CDN_RATE_DOLLARS_PER_TIB: '5',
      CACHE_MISS_RATE_DOLLARS_PER_TIB: '5',
    }

    const payload = {
      data_set_id: testDataSetId,
      total_cdn_lockup: '0',
      total_cache_miss_lockup: '0',
    }

    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload)

    const result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(testDataSetId)
      .first()

    expect(result.cdn_egress_quota).toBe('0')
    expect(result.cache_miss_egress_quota).toBe('0')
  })

  it('handles division by zero when rate is zero', async () => {
    const testEnv = {
      ...env,
      CDN_RATE_DOLLARS_PER_TIB: '0', // Zero rate
      CACHE_MISS_RATE_DOLLARS_PER_TIB: '0', // Zero rate
    }

    const payload = {
      data_set_id: testDataSetId,
      total_cdn_lockup: formatUsdfcAmount(5),
      total_cache_miss_lockup: formatUsdfcAmount(10),
    }

    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload)

    const result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(testDataSetId)
      .first()

    // Should set quota to 0 when rate is 0
    expect(result.cdn_egress_quota).toBe('0')
    expect(result.cache_miss_egress_quota).toBe('0')
  })

  it('replaces existing quota values', async () => {
    const testEnv = {
      ...env,
      CDN_RATE_DOLLARS_PER_TIB: '5',
      CACHE_MISS_RATE_DOLLARS_PER_TIB: '5',
    }

    // First update - 1 USDFC = 0.2 TiB
    const payload1 = {
      data_set_id: testDataSetId,
      total_cdn_lockup: formatUsdfcAmount(1), // 1 USDFC
      total_cache_miss_lockup: formatUsdfcAmount(2), // 2 USDFC
    }

    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload1)

    let result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(testDataSetId)
      .first()

    const expectedQuota1 = (BYTES_PER_TIB / 5n).toString() // 0.2 TiB
    const expectedQuota2 = ((BYTES_PER_TIB * 2n) / 5n).toString() // 0.4 TiB
    expect(result.cdn_egress_quota).toBe(expectedQuota1)
    expect(result.cache_miss_egress_quota).toBe(expectedQuota2)

    // Second update with different values (should replace, not add)
    const payload2 = {
      data_set_id: testDataSetId,
      total_cdn_lockup: formatUsdfcAmount(5), // 5 USDFC = 1 TiB
      total_cache_miss_lockup: formatUsdfcAmount(10), // 10 USDFC = 2 TiB
    }

    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload2)

    result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(testDataSetId)
      .first()

    // Should be replaced, not accumulated
    expect(result.cdn_egress_quota).toBe(BYTES_PER_TIB.toString())
    expect(result.cache_miss_egress_quota).toBe((BYTES_PER_TIB * 2n).toString())
  })

  it('handles missing data set gracefully', async () => {
    const testEnv = {
      ...env,
      CDN_RATE_DOLLARS_PER_TIB: '5',
      CACHE_MISS_RATE_DOLLARS_PER_TIB: '5',
    }

    const payload = {
      data_set_id: 'non-existent-data-set',
      total_cdn_lockup: formatUsdfcAmount(5),
      total_cache_miss_lockup: formatUsdfcAmount(10),
    }

    // Should not throw, just log warning
    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload)

    // Verify no data was created for non-existent data set
    const result = await env.DB.prepare('SELECT * FROM data_sets WHERE id = ?')
      .bind('non-existent-data-set')
      .first()

    expect(result).toBeNull()
  })

  it('handles missing lockup fields with default values', async () => {
    const testEnv = {
      ...env,
      CDN_RATE_DOLLARS_PER_TIB: '5',
      CACHE_MISS_RATE_DOLLARS_PER_TIB: '5',
    }

    const payload = {
      data_set_id: testDataSetId,
      // Missing lockup fields
    }

    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload)

    const result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(testDataSetId)
      .first()

    // Should default to 0
    expect(result.cdn_egress_quota).toBe('0')
    expect(result.cache_miss_egress_quota).toBe('0')
  })
})
