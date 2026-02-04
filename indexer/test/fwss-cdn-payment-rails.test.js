import { describe, it, beforeEach, afterEach, expect } from 'vitest'
import { env } from 'cloudflare:test'
import {
  handleFWSSCDNPaymentRailsToppedUp,
  handleFWSSDataSetCreated,
} from '../lib/fwss-handlers.js'
import { withDataSet, withServiceProvider } from './test-helpers.js'
import { BYTES_PER_TIB } from '../lib/constants.js'

describe('handleFWSSCDNPaymentRailsToppedUp', () => {
  let testServiceProviderId
  let testDataSetId

  beforeEach(async () => {
    // Clean up test data
    await env.DB.exec('DELETE FROM data_sets')
    await env.DB.exec('DELETE FROM data_set_egress_quotas')
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
    await env.DB.exec('DELETE FROM data_set_egress_quotas')
    await env.DB.exec('DELETE FROM service_providers')
  })

  it('calculates and stores egress quotas correctly', async () => {
    const testEnv = {
      ...env,
      CDN_RATE_PER_TIB: '5000000000000000000',
      CACHE_MISS_RATE_PER_TIB: '5000000000000000000',
    }

    const payload = {
      id: '0xtest-calc-quotas-0',
      data_set_id: testDataSetId,
      cdn_amount_added: '5000000000000000000',
      cache_miss_amount_added: '10000000000000000000',
    }

    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload)

    const result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_set_egress_quotas WHERE data_set_id = ?',
    )
      .bind(testDataSetId)
      .first()

    expect(result).toStrictEqual({
      cdn_egress_quota: Number(BYTES_PER_TIB),
      cache_miss_egress_quota: Number(BYTES_PER_TIB * 2n),
    })
  })

  it('handles zero amounts added', async () => {
    const testEnv = {
      ...env,
      CDN_RATE_PER_TIB: '5000000000000000000',
      CACHE_MISS_RATE_PER_TIB: '5000000000000000000',
    }

    const payload = {
      id: '0xtest-zero-amounts-0',
      data_set_id: testDataSetId,
      cdn_amount_added: '0',
      cache_miss_amount_added: '0',
    }

    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload)

    const result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_set_egress_quotas WHERE data_set_id = ?',
    )
      .bind(testDataSetId)
      .first()

    expect(result).toStrictEqual({
      cdn_egress_quota: 0,
      cache_miss_egress_quota: 0,
    })
  })

  it('accumulates quota values on multiple top-ups', async () => {
    const testEnv = {
      ...env,
      CDN_RATE_PER_TIB: '5000000000000000000',
      CACHE_MISS_RATE_PER_TIB: '5000000000000000000',
    }

    const payload1 = {
      id: '0xtest-accum-0',
      data_set_id: testDataSetId,
      cdn_amount_added: '5000000000000000000',
      cache_miss_amount_added: '10000000000000000000',
    }

    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload1)

    let result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_set_egress_quotas WHERE data_set_id = ?',
    )
      .bind(testDataSetId)
      .first()

    expect(result).toStrictEqual({
      cdn_egress_quota: Number(BYTES_PER_TIB),
      cache_miss_egress_quota: 2 * Number(BYTES_PER_TIB),
    })

    // Second top-up should increment, not replace (different entity id)
    const payload2 = {
      id: '0xtest-accum-1',
      data_set_id: testDataSetId,
      cdn_amount_added: '5000000000000000000',
      cache_miss_amount_added: '10000000000000000000',
    }

    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload2)

    result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_set_egress_quotas WHERE data_set_id = ?',
    )
      .bind(testDataSetId)
      .first()

    expect(result).toStrictEqual({
      cdn_egress_quota: 2 * Number(BYTES_PER_TIB),
      cache_miss_egress_quota: 4 * Number(BYTES_PER_TIB),
    })
  })

  it('is idempotent when called with identical payload (same entity id)', async () => {
    const CDN_RATE_PER_TIB = 5_000_000_000_000_000_000n
    const CACHE_MISS_RATE_PER_TIB = 5_000_000_000_000_000_000n

    const testEnv = {
      ...env,
      CDN_RATE_PER_TIB: CDN_RATE_PER_TIB.toString(),
      CACHE_MISS_RATE_PER_TIB: CACHE_MISS_RATE_PER_TIB.toString(),
    }

    const payload = {
      id: '0xabc123-0', // entity id from subgraph
      data_set_id: testDataSetId,
      cdn_amount_added: (1n * CDN_RATE_PER_TIB).toString(),
      cache_miss_amount_added: (2n * CACHE_MISS_RATE_PER_TIB).toString(),
    }

    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload)

    const resultAfterFirst = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_set_egress_quotas WHERE data_set_id = ?',
    )
      .bind(testDataSetId)
      .first()

    expect(resultAfterFirst).toStrictEqual({
      cdn_egress_quota: Number(BYTES_PER_TIB),
      cache_miss_egress_quota: Number(2n * BYTES_PER_TIB),
    })

    // Call with identical payload (duplicate webhook delivery)
    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload)

    const resultAfterSecond = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_set_egress_quotas WHERE data_set_id = ?',
    )
      .bind(testDataSetId)
      .first()

    // Quotas should NOT be doubled - the same event should be idempotent
    expect(resultAfterSecond).toStrictEqual(resultAfterFirst)
  })

  it('processes distinct events with identical payloads but different entity ids', async () => {
    const CDN_RATE_PER_TIB = 5_000_000_000_000_000_000n
    const CACHE_MISS_RATE_PER_TIB = 5_000_000_000_000_000_000n

    const testEnv = {
      ...env,
      CDN_RATE_PER_TIB: CDN_RATE_PER_TIB.toString(),
      CACHE_MISS_RATE_PER_TIB: CACHE_MISS_RATE_PER_TIB.toString(),
    }

    const payload1 = {
      id: '0xabc123-0', // first entity id
      data_set_id: testDataSetId,
      cdn_amount_added: (1n * CDN_RATE_PER_TIB).toString(),
      cache_miss_amount_added: (2n * CACHE_MISS_RATE_PER_TIB).toString(),
    }

    const payload2 = {
      id: '0xdef456-0', // different entity id
      data_set_id: testDataSetId,
      cdn_amount_added: (1n * CDN_RATE_PER_TIB).toString(), // same amounts
      cache_miss_amount_added: (2n * CACHE_MISS_RATE_PER_TIB).toString(),
    }

    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload1)

    const resultAfterFirst = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_set_egress_quotas WHERE data_set_id = ?',
    )
      .bind(testDataSetId)
      .first()

    expect(resultAfterFirst).toStrictEqual({
      cdn_egress_quota: Number(BYTES_PER_TIB),
      cache_miss_egress_quota: Number(2n * BYTES_PER_TIB),
    })

    // Call with different entity id - should be processed as new event
    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload2)

    const resultAfterSecond = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_set_egress_quotas WHERE data_set_id = ?',
    )
      .bind(testDataSetId)
      .first()

    // Quotas should be doubled - both events are distinct and should be processed
    expect(resultAfterSecond).toStrictEqual({
      cdn_egress_quota: Number(2n * BYTES_PER_TIB),
      cache_miss_egress_quota: Number(4n * BYTES_PER_TIB),
    })
  })

  it('creates quotas in egress table when data set does not exist', async () => {
    const testEnv = {
      ...env,
      CDN_RATE_PER_TIB: '5000000000000000000',
      CACHE_MISS_RATE_PER_TIB: '5000000000000000000',
    }

    const payload = {
      id: '0xtest-non-existent-0',
      data_set_id: 'non-existent-data-set',
      cdn_amount_added: '5000000000000000000',
      cache_miss_amount_added: '5000000000000000000',
    }

    await handleFWSSCDNPaymentRailsToppedUp(testEnv, payload)

    // Verify quotas were created in the egress quotas table
    const quotaResult = await env.DB.prepare(
      'SELECT * FROM data_set_egress_quotas WHERE data_set_id = ?',
    )
      .bind('non-existent-data-set')
      .first()

    expect(quotaResult).toStrictEqual({
      data_set_id: 'non-existent-data-set',
      cdn_egress_quota: Number(BYTES_PER_TIB),
      cache_miss_egress_quota: Number(BYTES_PER_TIB),
    })

    const dataSetResult = await env.DB.prepare(
      'SELECT * FROM data_sets WHERE id = ?',
    )
      .bind('non-existent-data-set')
      .first()

    expect(dataSetResult).toBeNull()
  })
})

describe('webhook ordering scenarios', () => {
  beforeEach(async () => {
    await env.DB.prepare('DELETE FROM data_sets').run()
    await env.DB.prepare('DELETE FROM data_set_egress_quotas').run()
    await env.DB.prepare('DELETE FROM service_providers').run()
    await env.DB.prepare('DELETE FROM wallet_details').run()
  })

  it('handles CDN top-up before data set creation', async () => {
    const testServiceProviderId = '5000000000000000000'
    const testDataSetId = '5000000000000000000'
    const payerAddress = '0x0000000000000000000000000000000000000000'
    await withServiceProvider(env, testServiceProviderId, 'test-service')

    const topUpEnv = {
      ...env,
      CDN_RATE_PER_TIB: '5000000000000000000',
      CACHE_MISS_RATE_PER_TIB: '5000000000000000000',
    }

    // Step 1: CDN top-up webhook arrives first
    const topUpPayload = {
      id: '0xtest-topup-before-create-0',
      data_set_id: testDataSetId,
      cdn_amount_added: '5000000000000000000',
      cache_miss_amount_added: '5000000000000000000',
    }

    await handleFWSSCDNPaymentRailsToppedUp(topUpEnv, topUpPayload)

    // Verify quotas were created in the egress quotas table
    let quotaResult = await env.DB.prepare(
      'SELECT * FROM data_set_egress_quotas WHERE data_set_id = ?',
    )
      .bind(testDataSetId)
      .first()

    expect(quotaResult).toStrictEqual({
      data_set_id: testDataSetId,
      cdn_egress_quota: Number(BYTES_PER_TIB),
      cache_miss_egress_quota: Number(BYTES_PER_TIB),
    })

    // Verify data set was NOT created yet
    let dataSetResult = await env.DB.prepare(
      'SELECT * FROM data_sets WHERE id = ?',
    )
      .bind(testDataSetId)
      .first()

    expect(dataSetResult).toBeNull()

    // Step 2: Data set creation webhook arrives later
    const createPayload = {
      data_set_id: testDataSetId,
      provider_id: testServiceProviderId,
      payer: payerAddress,
      metadata_keys: ['withCDN', 'withIPFSIndexing'],
      metadata_values: ['true', 'true'],
    }

    const createEnv = {
      ...env,
      CHAINALYSIS_API_KEY: 'test-key',
    }

    const mockCheckIfAddressIsSanctioned = async () => false

    await handleFWSSDataSetCreated(createEnv, createPayload, {
      checkIfAddressIsSanctioned: mockCheckIfAddressIsSanctioned,
    })

    // Verify data set was created with metadata
    dataSetResult = await env.DB.prepare('SELECT * FROM data_sets WHERE id = ?')
      .bind(testDataSetId)
      .first()

    expect(dataSetResult).toStrictEqual({
      id: testDataSetId,
      service_provider_id: testServiceProviderId,
      payer_address: payerAddress.toLowerCase(),
      with_cdn: 1,
      with_ipfs_indexing: 1,
      lockup_unlocks_at: null,
      total_egress_bytes_used: 0,
      terminate_service_tx_hash: null,
      usage_reported_until: '1970-01-01T00:00:00.000Z',
      cdn_payments_settled_until: '1970-01-01T00:00:00.000Z',
      pending_usage_report_tx_hash: null,
    })

    // Verify quotas remain unchanged in the egress quotas table
    quotaResult = await env.DB.prepare(
      'SELECT * FROM data_set_egress_quotas WHERE data_set_id = ?',
    )
      .bind(testDataSetId)
      .first()

    expect(quotaResult).toStrictEqual({
      data_set_id: testDataSetId,
      cdn_egress_quota: Number(BYTES_PER_TIB),
      cache_miss_egress_quota: Number(BYTES_PER_TIB),
    })
  })

  it('handles multiple CDN top-ups before data set creation', async () => {
    const testServiceProviderId = '5000000000000000000'
    const testDataSetId = '5000000000000000000'
    const payerAddress = '0x0000000000000000000000000000000000000000'
    await withServiceProvider(env, testServiceProviderId, 'test-service')

    const topUpEnv = {
      ...env,
      CDN_RATE_PER_TIB: '5000000000000000000',
      CACHE_MISS_RATE_PER_TIB: '5000000000000000000',
    }

    // Step 1: First CDN top-up
    await handleFWSSCDNPaymentRailsToppedUp(topUpEnv, {
      id: '0xtest-multi-topup-0',
      data_set_id: testDataSetId,
      cdn_amount_added: '5000000000000000000',
      cache_miss_amount_added: '5000000000000000000',
    })

    // Step 2: Second CDN top-up (different entity id)
    await handleFWSSCDNPaymentRailsToppedUp(topUpEnv, {
      id: '0xtest-multi-topup-1',
      data_set_id: testDataSetId,
      cdn_amount_added: '5000000000000000000',
      cache_miss_amount_added: '10000000000000000000',
    })

    // Verify accumulated quotas in egress quotas table
    let quotaResult = await env.DB.prepare(
      'SELECT * FROM data_set_egress_quotas WHERE data_set_id = ?',
    )
      .bind(testDataSetId)
      .first()

    expect(quotaResult).toStrictEqual({
      data_set_id: testDataSetId,
      cdn_egress_quota: 2 * Number(BYTES_PER_TIB),
      cache_miss_egress_quota: 3 * Number(BYTES_PER_TIB),
    })

    // Verify data set still doesn't exist
    let dataSetResult = await env.DB.prepare(
      'SELECT * FROM data_sets WHERE id = ?',
    )
      .bind(testDataSetId)
      .first()

    expect(dataSetResult).toBeNull()

    // Step 3: Data set creation webhook arrives
    const createPayload = {
      data_set_id: testDataSetId,
      provider_id: testServiceProviderId,
      payer: payerAddress,
      metadata_keys: ['withCDN'],
      metadata_values: ['true'],
    }

    const createEnv = {
      ...env,
      CHAINALYSIS_API_KEY: 'test-key',
    }

    const mockCheckIfAddressIsSanctioned = async () => false

    await handleFWSSDataSetCreated(createEnv, createPayload, {
      checkIfAddressIsSanctioned: mockCheckIfAddressIsSanctioned,
    })

    // Verify data set metadata was created
    dataSetResult = await env.DB.prepare('SELECT * FROM data_sets WHERE id = ?')
      .bind(testDataSetId)
      .first()

    expect(dataSetResult).toStrictEqual({
      id: testDataSetId,
      service_provider_id: testServiceProviderId,
      payer_address: payerAddress.toLowerCase(),
      with_cdn: 1,
      with_ipfs_indexing: 0,
      lockup_unlocks_at: null,
      total_egress_bytes_used: 0,
      terminate_service_tx_hash: null,
      usage_reported_until: '1970-01-01T00:00:00.000Z',
      cdn_payments_settled_until: '1970-01-01T00:00:00.000Z',
      pending_usage_report_tx_hash: null,
    })

    // Verify accumulated quotas preserved in egress quotas table
    quotaResult = await env.DB.prepare(
      'SELECT * FROM data_set_egress_quotas WHERE data_set_id = ?',
    )
      .bind(testDataSetId)
      .first()

    expect(quotaResult).toStrictEqual({
      data_set_id: testDataSetId,
      cdn_egress_quota: 2 * Number(BYTES_PER_TIB),
      cache_miss_egress_quota: 3 * Number(BYTES_PER_TIB),
    })
  })

  it('handles data set creation before CDN top-up', async () => {
    const testServiceProviderId = '5000000000000000000'
    const testDataSetId = '5000000000000000000'
    const payerAddress = '0x0000000000000000000000000000000000000000'
    await withServiceProvider(env, testServiceProviderId, 'test-service')

    // Step 1: Data set creation webhook arrives first (normal order)
    const createPayload = {
      data_set_id: testDataSetId,
      provider_id: testServiceProviderId,
      payer: payerAddress,
      metadata_keys: ['withCDN', 'withIPFSIndexing'],
      metadata_values: ['true', 'false'],
    }

    const createEnv = {
      ...env,
      CHAINALYSIS_API_KEY: 'test-key',
    }

    const mockCheckIfAddressIsSanctioned = async () => false
    await handleFWSSDataSetCreated(createEnv, createPayload, {
      checkIfAddressIsSanctioned: mockCheckIfAddressIsSanctioned,
    })

    // Verify initial data set creation
    let dataSetResult = await env.DB.prepare(
      'SELECT * FROM data_sets WHERE id = ?',
    )
      .bind(testDataSetId)
      .first()

    expect(dataSetResult).toStrictEqual({
      id: testDataSetId,
      service_provider_id: testServiceProviderId,
      payer_address: payerAddress.toLowerCase(),
      with_cdn: 1,
      with_ipfs_indexing: 1,
      lockup_unlocks_at: null,
      total_egress_bytes_used: 0,
      terminate_service_tx_hash: null,
      usage_reported_until: '1970-01-01T00:00:00.000Z',
      cdn_payments_settled_until: '1970-01-01T00:00:00.000Z',
      pending_usage_report_tx_hash: null,
    })

    // Verify no quotas exist yet
    let quotaResult = await env.DB.prepare(
      'SELECT * FROM data_set_egress_quotas WHERE data_set_id = ?',
    )
      .bind(testDataSetId)
      .first()

    expect(quotaResult).toBeNull()

    // Step 2: CDN top-up webhook arrives later
    const topUpEnv = {
      ...env,
      CDN_RATE_PER_TIB: '5000000000000000000',
      CACHE_MISS_RATE_PER_TIB: '5000000000000000000',
    }

    const topUpPayload = {
      id: '0xtest-create-before-topup-0',
      data_set_id: testDataSetId,
      cdn_amount_added: '5000000000000000000',
      cache_miss_amount_added: '5000000000000000000',
    }

    await handleFWSSCDNPaymentRailsToppedUp(topUpEnv, topUpPayload)

    // Verify quotas were created in egress quotas table
    quotaResult = await env.DB.prepare(
      'SELECT * FROM data_set_egress_quotas WHERE data_set_id = ?',
    )
      .bind(testDataSetId)
      .first()

    expect(quotaResult).toStrictEqual({
      data_set_id: testDataSetId,
      cdn_egress_quota: Number(BYTES_PER_TIB),
      cache_miss_egress_quota: Number(BYTES_PER_TIB),
    })

    // Verify data set remains unchanged
    dataSetResult = await env.DB.prepare('SELECT * FROM data_sets WHERE id = ?')
      .bind(testDataSetId)
      .first()

    expect(dataSetResult).toStrictEqual({
      id: testDataSetId,
      service_provider_id: testServiceProviderId,
      payer_address: payerAddress.toLowerCase(),
      with_cdn: 1,
      with_ipfs_indexing: 1,
      lockup_unlocks_at: null,
      total_egress_bytes_used: 0,
      terminate_service_tx_hash: null,
      usage_reported_until: '1970-01-01T00:00:00.000Z',
      cdn_payments_settled_until: '1970-01-01T00:00:00.000Z',
      pending_usage_report_tx_hash: null,
    })
  })
})
