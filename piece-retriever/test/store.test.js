import { describe, it, beforeAll, expect } from 'vitest'
import {
  logRetrievalResult,
  getStorageProviderAndValidatePayer,
  updateDataSetStats,
} from '../lib/store.js'
import { env } from 'cloudflare:test'
import {
  withDataSet,
  withPiece,
  withDataSetPieces,
  withApprovedProvider,
} from './test-helpers.js'

describe('logRetrievalResult', () => {
  it('inserts a log into local D1 via logRetrievalResult and verifies it', async () => {
    const DATA_SET_ID = '1'

    await logRetrievalResult(env, {
      dataSetId: DATA_SET_ID,
      cacheMiss: false,
      egressBytes: 1234,
      responseStatus: 200,
      timestamp: new Date().toISOString(),
      requestCountryCode: 'US',
    })

    const readOutput = await env.DB.prepare(
      `SELECT 
        data_set_id,
        response_status,
        egress_bytes,
        cache_miss,
        request_country_code
      FROM retrieval_logs 
      WHERE data_set_id = '${DATA_SET_ID}'`,
    ).all()
    const result = readOutput.results
    expect(result).toEqual([
      {
        data_set_id: DATA_SET_ID,
        response_status: 200,
        egress_bytes: 1234,
        cache_miss: 0,
        request_country_code: 'US',
      },
    ])
  })
})

describe('getStorageProviderAndValidatePayer', () => {
  const APPROVED_SERVICE_PROVIDER_ID = '20'
  beforeAll(async () => {
    await withApprovedProvider(env, {
      id: APPROVED_SERVICE_PROVIDER_ID,
      serviceUrl: 'https://approved-provider.xyz',
    })
  })

  it('returns service provider for valid pieceCid', async () => {
    const dataSetId = 'test-set-1'
    const pieceCid = 'bafk4test'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'

    await withDataSetPieces(env, {
      dataSetId,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      payerAddress,
      withCDN: true,
      cdnEgressQuota: '100',
      cacheMissEgressQuota: '100',
      pieceCid,
      pieceId: 'piece-1',
    })

    const result = await getStorageProviderAndValidatePayer(
      env,
      payerAddress,
      pieceCid,
      true,
    )
    expect(result.serviceProviderId).toBe(APPROVED_SERVICE_PROVIDER_ID)
  })

  it('throws error if pieceCid not found', async () => {
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'
    await expect(
      getStorageProviderAndValidatePayer(
        env,
        payerAddress,
        'nonexistent-cid',
        true,
      ),
    ).rejects.toThrow(/does not exist/)
  })

  it('throws error if data_set_id exists but has no associated service provider', async () => {
    const pieceCid = 'cid-no-owner'
    const dataSetId = 'data-set-no-owner'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'

    await withPiece(env, { pieceId: 'piece-no-sp', dataSetId, pieceCid })

    await expect(
      getStorageProviderAndValidatePayer(env, payerAddress, pieceCid, true),
    ).rejects.toThrow(/no associated service provider/)
  })

  it('returns error if no payment rail', async () => {
    const pieceCid = 'cid-unapproved'
    const dataSetId = 'data-set-unapproved'
    const serviceProviderId = APPROVED_SERVICE_PROVIDER_ID
    const payerAddress = '0xabcdef1234567890abcdef1234567890abcdef12'

    await withDataSetPieces(env, {
      dataSetId,
      serviceProviderId,
      payerAddress: payerAddress.replace('a', 'b'),
      withCDN: true,
      cdnEgressQuota: '100',
      cacheMissEgressQuota: '100',
      pieceCid,
      pieceId: 'piece-2',
    })

    await expect(
      getStorageProviderAndValidatePayer(env, payerAddress, pieceCid, true),
    ).rejects.toThrow(
      /There is no Filecoin Warm Storage Service deal for payer/,
    )
  })

  it('returns error if withCDN=false', async () => {
    const pieceCid = 'cid-unapproved'
    const dataSetId = 'data-set-unapproved'
    const serviceProviderId = APPROVED_SERVICE_PROVIDER_ID
    const payerAddress = '0xabcdef1234567890abcdef1234567890abcdef12'

    await withDataSetPieces(env, {
      dataSetId,
      serviceProviderId,
      payerAddress,
      withCDN: false,
      cdnEgressQuota: '100',
      cacheMissEgressQuota: '100',
      pieceCid,
      pieceId: 'piece-2',
    })

    await expect(
      getStorageProviderAndValidatePayer(env, payerAddress, pieceCid, true),
    ).rejects.toThrow(/withCDN=false/)
  })

  it('returns serviceProviderId for approved service provider', async () => {
    const pieceCid = 'cid-approved'
    const dataSetId = 'data-set-approved'
    const payerAddress = '0xabcdef1234567890abcdef1234567890abcdef12'

    await withDataSetPieces(env, {
      dataSetId,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      withCDN: true,
      cdnEgressQuota: '100',
      cacheMissEgressQuota: '100',
      payerAddress,
      pieceId: 'piece-3',
      pieceCid,
    })

    const result = await getStorageProviderAndValidatePayer(
      env,
      payerAddress,
      pieceCid,
      true,
    )
    expect(result.serviceProviderId).toBe(APPROVED_SERVICE_PROVIDER_ID)
  })

  it('returns a random service provider when multiple service providers share the same pieceCid', async () => {
    const dataSetId1 = 'data-set-a'
    const dataSetId2 = 'data-set-b'
    const pieceCid = 'shared-piece-cid'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'
    const serviceProviderId1 = 'service-provider-a'
    const serviceProviderId2 = 'service-provicer-b'

    await withApprovedProvider(env, {
      id: serviceProviderId1,
    })
    await withApprovedProvider(env, {
      id: serviceProviderId2,
    })

    // Insert both owners into separate sets with the same pieceCid
    await withDataSetPieces(env, {
      dataSetId: dataSetId1,
      serviceProviderId: serviceProviderId1,
      withCDN: true,
      cdnEgressQuota: '100',
      cacheMissEgressQuota: '100',
      pieceId: 'piece-a',
      pieceCid,
      payerAddress,
    })

    await withDataSetPieces(env, {
      dataSetId: dataSetId2,
      serviceProviderId: serviceProviderId2,
      withCDN: true,
      cdnEgressQuota: '100',
      cacheMissEgressQuota: '100',
      pieceId: 'piece-b',
      pieceCid,
      payerAddress,
    })

    const serviceProviderIdsReturned = new Set()
    for (let i = 0; i < 100; i++) {
      const result = await getStorageProviderAndValidatePayer(
        env,
        payerAddress,
        pieceCid,
        true,
      )
      serviceProviderIdsReturned.add(result.serviceProviderId)
      if (serviceProviderIdsReturned.size === 2) {
        return
      }
    }
    throw new Error('Did not return 2 different SPs')
  })

  it('ignores owners that are not approved by Filecoin Warm Storage Service', async () => {
    const dataSetId1 = '0'
    const dataSetId2 = '1'
    const pieceCid = 'shared-piece-cid'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'
    const serviceProviderId1 = '0'
    const serviceProviderId2 = '1'

    await withApprovedProvider(env, {
      id: serviceProviderId1,
      serviceUrl: 'https://pdp-provider-1.xyz',
    })

    // NOTE: the second provider is not registered as an approved provider

    // Important: we must insert the unapproved provider first!
    await withDataSetPieces(env, {
      payerAddress,
      serviceProviderId: serviceProviderId2,
      dataSetId: dataSetId2,
      withCDN: true,
      pieceCid,
      cdnEgressQuota: '100',
      cacheMissEgressQuota: '100',
    })

    await withDataSetPieces(env, {
      payerAddress,
      serviceProviderId: serviceProviderId1,
      dataSetId: dataSetId1,
      withCDN: true,
      pieceCid,
      cdnEgressQuota: '100',
      cacheMissEgressQuota: '100',
    })

    // Should return service provider 1 because service provider 2 is not approved
    const result = await getStorageProviderAndValidatePayer(
      env,
      payerAddress,
      pieceCid,
      true,
    )
    expect(result).toEqual({
      dataSetId: dataSetId1,
      serviceProviderId: serviceProviderId1.toLowerCase(),
      serviceUrl: 'https://pdp-provider-1.xyz',
      cdnEgressQuota: 100n,
      cacheMissEgressQuota: 100n,
    })
  })
})

describe('Egress Quota Management', () => {
  const APPROVED_SERVICE_PROVIDER_ID = '30'

  beforeAll(async () => {
    await withApprovedProvider(env, {
      id: APPROVED_SERVICE_PROVIDER_ID,
      serviceUrl: 'https://quota-test-provider.xyz',
    })
  })

  it('returns error when CDN quota is exhausted', async () => {
    const dataSetId = 'test-quota-exhausted'
    const pieceCid = 'test-cid-exhausted'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'

    await withDataSetPieces(env, {
      dataSetId,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      payerAddress,
      withCDN: true,
      cdnEgressQuota: 0,
      cacheMissEgressQuota: 1,
      pieceCid,
      pieceId: 'piece-exhausted',
    })

    await expect(
      getStorageProviderAndValidatePayer(env, payerAddress, pieceCid, true),
    ).rejects.toThrow(/CDN egress quota exhausted for payer/)
  })

  it('returns error when cache-miss quota is exhausted', async () => {
    const dataSetId = 'test-cache-miss-exhausted'
    const pieceCid = 'test-cid-cache-miss-exhausted'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'

    await withDataSetPieces(env, {
      dataSetId,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      payerAddress,
      withCDN: true,
      cdnEgressQuota: 1,
      cacheMissEgressQuota: 0,
      pieceCid,
      pieceId: 'piece-cache-miss-exhausted',
    })

    await expect(
      getStorageProviderAndValidatePayer(env, payerAddress, pieceCid, true),
    ).rejects.toThrow(/Cache miss egress quota exhausted for payer/)
  })

  it('allows retrieval when quota is sufficient', async () => {
    const dataSetId = 'test-quota-sufficient'
    const pieceCid = 'test-cid-sufficient'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'

    await withDataSetPieces(env, {
      dataSetId,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      payerAddress,
      withCDN: true,
      cdnEgressQuota: 1,
      cacheMissEgressQuota: 1,
      pieceCid,
      pieceId: 'piece-sufficient',
    })

    const result = await getStorageProviderAndValidatePayer(
      env,
      payerAddress,
      pieceCid,
      true,
    )
    expect(result).toStrictEqual({
      dataSetId,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      serviceUrl: 'https://quota-test-provider.xyz',
      cdnEgressQuota: 1n,
      cacheMissEgressQuota: 1n,
    })
  })

  it('correctly decrements CDN quota on cache hit', async () => {
    const dataSetId = 'test-quota-decrement-cdn'
    const initialQuota = 1000
    const egressBytes = 100

    await withDataSet(env, {
      dataSetId,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      payerAddress: '0xtest',
      withCDN: true,
      cdnEgressQuota: initialQuota,
      cacheMissEgressQuota: initialQuota,
    })

    await updateDataSetStats(env, {
      dataSetId,
      egressBytes,
      cacheMiss: false,
      enforceEgressQuota: true,
    })

    const result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .first()

    expect(result).toStrictEqual({
      cdn_egress_quota: 900,
      cache_miss_egress_quota: initialQuota,
    })
  })

  it('correctly decrements cache miss quota on cache miss', async () => {
    const dataSetId = 'test-quota-decrement-miss'
    const initialQuota = 1000
    const egressBytes = 100

    await withDataSet(env, {
      dataSetId,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      payerAddress: '0xtest',
      withCDN: true,
      cdnEgressQuota: initialQuota,
      cacheMissEgressQuota: initialQuota,
    })

    await updateDataSetStats(env, {
      dataSetId,
      egressBytes,
      cacheMiss: true,
      enforceEgressQuota: true,
    })

    const result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .first()

    expect(result).toStrictEqual({
      cdn_egress_quota: 900,
      cache_miss_egress_quota: 900,
    })
  })

  it('allows quota to go negative when egress exceeds quota', async () => {
    const dataSetId = 'test-quota-below-zero'
    const insufficientQuota = 100
    const egressBytes = 200

    await withDataSet(env, {
      dataSetId,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      payerAddress: '0xtest',
      withCDN: true,
      cdnEgressQuota: insufficientQuota,
      cacheMissEgressQuota: insufficientQuota,
    })

    // Should allow quota to go negative
    await updateDataSetStats(env, {
      dataSetId,
      egressBytes,
      cacheMiss: true,
      enforceEgressQuota: true,
    })

    const result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .first()

    // Both quotas should be negative (100 - 200 = -100)
    expect(result).toStrictEqual({
      cdn_egress_quota: -100,
      cache_miss_egress_quota: -100,
    })
  })

  it('allows CDN quota to go negative when egress exceeds quota', async () => {
    const dataSetId = 'test-cdn-quota-below-zero'
    const insufficientQuota = 50
    const sufficientQuota = 1000
    const egressBytes = 150

    await withDataSet(env, {
      dataSetId,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      payerAddress: '0xtest',
      withCDN: true,
      cdnEgressQuota: insufficientQuota,
      cacheMissEgressQuota: sufficientQuota,
    })

    // Should allow CDN quota to go negative
    await updateDataSetStats(env, {
      dataSetId,
      egressBytes,
      cacheMiss: false,
      enforceEgressQuota: true,
    })

    const result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .first()

    // CDN quota should be negative (50 - 150 = -100)
    // Cache miss quota should remain unchanged on cache hit
    expect(result).toStrictEqual({
      cdn_egress_quota: -100,
      cache_miss_egress_quota: sufficientQuota,
    })
  })

  it('returns error when quota values are null', async () => {
    const dataSetId = 'test-quota-null'
    const pieceCid = 'test-cid-null'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'

    await withDataSetPieces(env, {
      dataSetId,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      withCDN: true,
      cdnEgressQuota: null,
      cacheMissEgressQuota: null,
      payerAddress,
      pieceId: 'piece-null',
      pieceCid,
    })

    // Should return 402 error when quotas are null
    await expect(
      getStorageProviderAndValidatePayer(env, payerAddress, pieceCid, true),
    ).rejects.toThrow(/CDN egress quota exhausted for payer/)
  })

  it('handles quota exactly at piece size', async () => {
    const dataSetId = 'test-quota-exact'
    const pieceCid = 'test-cid-exact'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'
    const exactQuota = 100

    await withDataSetPieces(env, {
      dataSetId,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      payerAddress,
      withCDN: true,
      cdnEgressQuota: exactQuota,
      cacheMissEgressQuota: exactQuota,
      pieceCid,
      pieceId: 'piece-exact',
    })

    // Quota of exactly 100 should be sufficient (> 0)
    const result = await getStorageProviderAndValidatePayer(
      env,
      payerAddress,
      pieceCid,
      true,
    )
    expect(result).toStrictEqual({
      dataSetId,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      serviceUrl: 'https://quota-test-provider.xyz',
      cdnEgressQuota: 100n,
      cacheMissEgressQuota: 100n,
    })

    // Decrement by exact amount should result in 0
    await updateDataSetStats(env, {
      dataSetId,
      egressBytes: 100,
      cacheMiss: false,
      enforceEgressQuota: true,
    })

    const afterDecrement = await env.DB.prepare(
      'SELECT cdn_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .first()
    expect(afterDecrement).toStrictEqual({
      cdn_egress_quota: 0,
    })
  })

  it('allows retrieval when quota enforcement is disabled and CDN quota is exhausted', async () => {
    const dataSetId = 'test-no-enforce-cdn-exhausted'
    const pieceCid = 'test-cid-no-enforce-cdn-exhausted'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'

    await withDataSetPieces(env, {
      dataSetId,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      payerAddress,
      withCDN: true,
      cdnEgressQuota: 0,
      cacheMissEgressQuota: 1,
      pieceCid,
      pieceId: 'piece-no-enforce-cdn-exhausted',
    })

    const result = await getStorageProviderAndValidatePayer(
      env,
      payerAddress,
      pieceCid,
      false,
    )
    expect(result).toStrictEqual({
      dataSetId,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      serviceUrl: 'https://quota-test-provider.xyz',
      cdnEgressQuota: 0n,
      cacheMissEgressQuota: 1n,
    })
  })

  it('allows retrieval when quota enforcement is disabled and cache-miss quota is exhausted', async () => {
    const dataSetId = 'test-no-enforce-cache-miss-exhausted'
    const pieceCid = 'test-cid-no-enforce-cache-miss-exhausted'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'

    await withDataSetPieces(env, {
      dataSetId,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      payerAddress,
      withCDN: true,
      cdnEgressQuota: 1,
      cacheMissEgressQuota: 0,
      pieceCid,
      pieceId: 'piece-no-enforce-cache-miss-exhausted',
    })

    const result = await getStorageProviderAndValidatePayer(
      env,
      payerAddress,
      pieceCid,
      false,
    )
    expect(result).toStrictEqual({
      dataSetId,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      serviceUrl: 'https://quota-test-provider.xyz',
      cdnEgressQuota: 1n,
      cacheMissEgressQuota: 0n,
    })
  })

  it('allows retrieval when quota enforcement is disabled and both quotas are exhausted', async () => {
    const dataSetId = 'test-no-enforce-both-exhausted'
    const pieceCid = 'test-cid-no-enforce-both-exhausted'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'

    await withDataSetPieces(env, {
      dataSetId,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      payerAddress,
      withCDN: true,
      cdnEgressQuota: 0,
      cacheMissEgressQuota: 0,
      pieceCid,
      pieceId: 'piece-no-enforce-both-exhausted',
    })

    const result = await getStorageProviderAndValidatePayer(
      env,
      payerAddress,
      pieceCid,
      false,
    )
    expect(result).toStrictEqual({
      dataSetId,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      serviceUrl: 'https://quota-test-provider.xyz',
      cdnEgressQuota: 0n,
      cacheMissEgressQuota: 0n,
    })
  })
})

describe('updateDataSetStats', () => {
  it('updates egress stats', async () => {
    const DATA_SET_ID = 'test-data-set-1'
    const EGRESS_BYTES = 123456

    await withDataSet(env, {
      dataSetId: DATA_SET_ID,
      cdnEgressQuota: 100,
      cacheMissEgressQuota: 100,
    })
    await updateDataSetStats(env, {
      dataSetId: DATA_SET_ID,
      egressBytes: EGRESS_BYTES,
    })

    const { results: insertResults } = await env.DB.prepare(
      `SELECT id, total_egress_bytes_used 
       FROM data_sets
       WHERE id = ?`,
    )
      .bind(DATA_SET_ID)
      .all()

    expect(insertResults).toEqual([
      {
        id: DATA_SET_ID,
        total_egress_bytes_used: EGRESS_BYTES,
      },
    ])

    // Update the egress stats
    await updateDataSetStats(env, {
      dataSetId: DATA_SET_ID,
      egressBytes: 1000,
    })

    const { results: updateResults } = await env.DB.prepare(
      `SELECT id, total_egress_bytes_used 
       FROM data_sets 
       WHERE id = ?`,
    )
      .bind(DATA_SET_ID)
      .all()

    expect(updateResults).toEqual([
      {
        id: DATA_SET_ID,
        total_egress_bytes_used: EGRESS_BYTES + 1000,
      },
    ])
  })

  it('does not decrement quotas when enforceEgressQuota is false', async () => {
    const DATA_SET_ID = 'test-data-set-no-enforce'
    const EGRESS_BYTES = 100
    const initialCdnQuota = 500
    const initialCacheMissQuota = 300

    await withDataSet(env, {
      dataSetId: DATA_SET_ID,
      cdnEgressQuota: initialCdnQuota,
      cacheMissEgressQuota: initialCacheMissQuota,
    })

    // Test with cache hit (cacheMiss = false)
    await updateDataSetStats(env, {
      dataSetId: DATA_SET_ID,
      egressBytes: EGRESS_BYTES,
      cacheMiss: false,
      enforceEgressQuota: false,
    })

    const { results: afterCacheHit } = await env.DB.prepare(
      `SELECT total_egress_bytes_used, cdn_egress_quota, cache_miss_egress_quota
       FROM data_sets
       WHERE id = ?`,
    )
      .bind(DATA_SET_ID)
      .all()

    expect(afterCacheHit).toStrictEqual([
      {
        total_egress_bytes_used: EGRESS_BYTES,
        cdn_egress_quota: initialCdnQuota,
        cache_miss_egress_quota: initialCacheMissQuota,
      },
    ])

    // Test with cache miss (cacheMiss = true)
    await updateDataSetStats(env, {
      dataSetId: DATA_SET_ID,
      egressBytes: EGRESS_BYTES,
      cacheMiss: true,
      enforceEgressQuota: false,
    })

    const { results: afterCacheMiss } = await env.DB.prepare(
      `SELECT total_egress_bytes_used, cdn_egress_quota, cache_miss_egress_quota
       FROM data_sets
       WHERE id = ?`,
    )
      .bind(DATA_SET_ID)
      .all()

    expect(afterCacheMiss).toStrictEqual([
      {
        total_egress_bytes_used: EGRESS_BYTES * 2,
        cdn_egress_quota: initialCdnQuota,
        cache_miss_egress_quota: initialCacheMissQuota,
      },
    ])
  })

  it('decrements quotas when enforceEgressQuota is true', async () => {
    const DATA_SET_ID = 'test-data-set-enforce'
    const EGRESS_BYTES = 100
    const initialCdnQuota = 500
    const initialCacheMissQuota = 300

    await withDataSet(env, {
      dataSetId: DATA_SET_ID,
      cdnEgressQuota: initialCdnQuota,
      cacheMissEgressQuota: initialCacheMissQuota,
    })

    // Test with cache hit (cacheMiss = false)
    await updateDataSetStats(env, {
      dataSetId: DATA_SET_ID,
      egressBytes: EGRESS_BYTES,
      cacheMiss: false,
      enforceEgressQuota: true,
    })

    const { results: afterCacheHit } = await env.DB.prepare(
      `SELECT total_egress_bytes_used, cdn_egress_quota, cache_miss_egress_quota
       FROM data_sets
       WHERE id = ?`,
    )
      .bind(DATA_SET_ID)
      .all()

    expect(afterCacheHit).toStrictEqual([
      {
        total_egress_bytes_used: EGRESS_BYTES,
        cdn_egress_quota: initialCdnQuota - EGRESS_BYTES,
        cache_miss_egress_quota: initialCacheMissQuota,
      },
    ])

    // Test with cache miss (cacheMiss = true)
    await updateDataSetStats(env, {
      dataSetId: DATA_SET_ID,
      egressBytes: EGRESS_BYTES,
      cacheMiss: true,
      enforceEgressQuota: true,
    })

    const { results: afterCacheMiss } = await env.DB.prepare(
      `SELECT total_egress_bytes_used, cdn_egress_quota, cache_miss_egress_quota
       FROM data_sets
       WHERE id = ?`,
    )
      .bind(DATA_SET_ID)
      .all()

    expect(afterCacheMiss).toStrictEqual([
      {
        total_egress_bytes_used: EGRESS_BYTES * 2,
        cdn_egress_quota: initialCdnQuota - EGRESS_BYTES * 2,
        cache_miss_egress_quota: initialCacheMissQuota - EGRESS_BYTES,
      },
    ])
  })
})
