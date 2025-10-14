import { describe, it, beforeAll, expect } from 'vitest'
import {
  logRetrievalResult,
  getStorageProviderAndValidatePayer,
  updateDataSetStats,
} from '../lib/store.js'
import { env } from 'cloudflare:test'
import {
  withDataSetPieces,
  withApprovedProvider,
} from './test-data-builders.js'

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
    const pieceCid = 'test-cid-1'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'

    await env.DB.prepare(
      'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
    )
      .bind(
        dataSetId,
        APPROVED_SERVICE_PROVIDER_ID,
        payerAddress,
        true,
        1099511627776,
        1099511627776,
      )
      .run()
    await env.DB.prepare(
      'INSERT INTO pieces (id, data_set_id, cid) VALUES (?, ?, ?)',
    )
      .bind('piece-1', dataSetId, pieceCid)
      .run()

    const result = await getStorageProviderAndValidatePayer(
      env,
      payerAddress,
      pieceCid,
    )
    expect(result.serviceProviderId).toBe(APPROVED_SERVICE_PROVIDER_ID)
  })

  it('throws error if pieceCid not found', async () => {
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'
    await expect(
      getStorageProviderAndValidatePayer(env, payerAddress, 'nonexistent-cid'),
    ).rejects.toThrow(/does not exist/)
  })

  it('throws error if data_set_id exists but has no associated service provider', async () => {
    const cid = 'cid-no-owner'
    const dataSetId = 'data-set-no-owner'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'

    await env.DB.prepare(
      `
      INSERT INTO pieces (id, data_set_id, cid)
      VALUES (?, ?, ?)
    `,
    )
      .bind('piece-1', dataSetId, cid)
      .run()

    await expect(
      getStorageProviderAndValidatePayer(env, payerAddress, cid),
    ).rejects.toThrow(/no associated service provider/)
  })

  it('returns error if no payment rail', async () => {
    const cid = 'cid-unapproved'
    const dataSetId = 'data-set-unapproved'
    const serviceProviderId = APPROVED_SERVICE_PROVIDER_ID
    const payerAddress = '0xabcdef1234567890abcdef1234567890abcdef12'

    await env.DB.batch([
      env.DB.prepare(
        'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
      ).bind(
        dataSetId,
        serviceProviderId,
        payerAddress.replace('a', 'b'),
        true,
        1099511627776,
        1099511627776,
      ),
      env.DB.prepare(
        'INSERT INTO pieces (id, data_set_id, cid) VALUES (?, ?, ?)',
      ).bind('piece-2', dataSetId, cid),
    ])

    await expect(
      getStorageProviderAndValidatePayer(env, payerAddress, cid),
    ).rejects.toThrow(
      /There is no Filecoin Warm Storage Service deal for payer/,
    )
  })

  it('returns error if withCDN=false', async () => {
    const cid = 'cid-unapproved'
    const dataSetId = 'data-set-unapproved'
    const serviceProviderId = APPROVED_SERVICE_PROVIDER_ID
    const payerAddress = '0xabcdef1234567890abcdef1234567890abcdef12'

    await env.DB.batch([
      env.DB.prepare(
        'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
      ).bind(
        dataSetId,
        serviceProviderId,
        payerAddress,
        false,
        1099511627776,
        1099511627776,
      ),
      env.DB.prepare(
        'INSERT INTO pieces (id, data_set_id, cid) VALUES (?, ?, ?)',
      ).bind('piece-2', dataSetId, cid),
    ])

    await expect(
      getStorageProviderAndValidatePayer(env, payerAddress, cid),
    ).rejects.toThrow(/withCDN=false/)
  })

  it('returns serviceProviderId for approved service provider', async () => {
    const cid = 'cid-approved'
    const dataSetId = 'data-set-approved'
    const payerAddress = '0xabcdef1234567890abcdef1234567890abcdef12'

    await env.DB.batch([
      env.DB.prepare(
        'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
      ).bind(
        dataSetId,
        APPROVED_SERVICE_PROVIDER_ID,
        payerAddress,
        true,
        1099511627776,
        1099511627776,
      ),
      env.DB.prepare(
        'INSERT INTO pieces (id, data_set_id, cid) VALUES (?, ?, ?)',
      ).bind('piece-3', dataSetId, cid),
    ])

    const result = await getStorageProviderAndValidatePayer(
      env,
      payerAddress,
      cid,
    )

    expect(result.serviceProviderId).toBe(APPROVED_SERVICE_PROVIDER_ID)
  })
  it('returns the service provider first in the ordering when multiple service providers share the same pieceCid', async () => {
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
    await env.DB.prepare(
      'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
    )
      .bind(
        dataSetId1,
        serviceProviderId1,
        payerAddress,
        true,
        1099511627776,
        1099511627776,
      )
      .run()

    await env.DB.prepare(
      'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
    )
      .bind(
        dataSetId2,
        serviceProviderId2,
        payerAddress,
        true,
        1099511627776,
        1099511627776,
      )
      .run()

    // Insert same pieceCid for both sets
    await env.DB.prepare(
      'INSERT INTO pieces (id, data_set_id, cid) VALUES (?, ?, ?)',
    )
      .bind('piece-a', dataSetId1, pieceCid)
      .run()

    await env.DB.prepare(
      'INSERT INTO pieces (id, data_set_id, cid) VALUES (?, ?, ?)',
    )
      .bind('piece-b', dataSetId2, pieceCid)
      .run()

    // Should return only the serviceProviderId1 which is the first in the ordering
    const result = await getStorageProviderAndValidatePayer(
      env,
      payerAddress,
      pieceCid,
    )
    expect(result.serviceProviderId).toBe(serviceProviderId1)
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
    })

    await withDataSetPieces(env, {
      payerAddress,
      serviceProviderId: serviceProviderId1,
      dataSetId: dataSetId1,
      withCDN: true,
      pieceCid,
    })

    // Should return service provider 1 because service provider 2 is not approved
    const result = await getStorageProviderAndValidatePayer(
      env,
      payerAddress,
      pieceCid,
    )
    expect(result).toEqual({
      dataSetId: dataSetId1,
      serviceProviderId: serviceProviderId1.toLowerCase(),
      serviceUrl: 'https://pdp-provider-1.xyz',
      cdnEgressQuota: 1099511627776,
      cacheMissEgressQuota: 1099511627776,
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

    await env.DB.prepare(
      'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
    )
      .bind(
        dataSetId,
        APPROVED_SERVICE_PROVIDER_ID,
        payerAddress,
        true,
        0,
        1000000,
      )
      .run()
    await env.DB.prepare(
      'INSERT INTO pieces (id, data_set_id, cid) VALUES (?, ?, ?)',
    )
      .bind('piece-exhausted', dataSetId, pieceCid)
      .run()

    await expect(
      getStorageProviderAndValidatePayer(env, payerAddress, pieceCid),
    ).rejects.toThrow(/CDN egress quota exhausted/)
  })

  it('returns error when cache-miss quota is exhausted', async () => {
    const dataSetId = 'test-cache-miss-exhausted'
    const pieceCid = 'test-cid-cache-miss-exhausted'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'

    await env.DB.prepare(
      'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
    )
      .bind(
        dataSetId,
        APPROVED_SERVICE_PROVIDER_ID,
        payerAddress,
        true,
        1000000,
        0,
      )
      .run()
    await env.DB.prepare(
      'INSERT INTO pieces (id, data_set_id, cid) VALUES (?, ?, ?)',
    )
      .bind('piece-cache-miss-exhausted', dataSetId, pieceCid)
      .run()

    await expect(
      getStorageProviderAndValidatePayer(env, payerAddress, pieceCid),
    ).rejects.toThrow(/Cache miss egress quota exhausted/)
  })

  it('allows retrieval when quota is sufficient', async () => {
    const dataSetId = 'test-quota-sufficient'
    const pieceCid = 'test-cid-sufficient'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'

    await env.DB.prepare(
      'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
    )
      .bind(
        dataSetId,
        APPROVED_SERVICE_PROVIDER_ID,
        payerAddress,
        true,
        1000000,
        1000000,
      )
      .run()
    await env.DB.prepare(
      'INSERT INTO pieces (id, data_set_id, cid) VALUES (?, ?, ?)',
    )
      .bind('piece-sufficient', dataSetId, pieceCid)
      .run()

    const result = await getStorageProviderAndValidatePayer(
      env,
      payerAddress,
      pieceCid,
    )
    expect(result.dataSetId).toBe(dataSetId)
    expect(result.cdnEgressQuota).toBe(1000000)
    expect(result.cacheMissEgressQuota).toBe(1000000)
  })

  it('correctly decrements CDN quota on cache hit', async () => {
    const dataSetId = 'test-quota-decrement-cdn'
    const initialQuota = 1000000
    const egressBytes = 100000

    await env.DB.prepare(
      'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
    )
      .bind(
        dataSetId,
        APPROVED_SERVICE_PROVIDER_ID,
        '0xtest',
        true,
        initialQuota,
        initialQuota,
      )
      .run()

    await updateDataSetStats(env, {
      dataSetId,
      egressBytes,
      cacheMiss: false,
    })

    const result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .first()

    const expectedQuota = Number(initialQuota) - egressBytes
    expect(result.cdn_egress_quota).toBe(expectedQuota)
    expect(result.cache_miss_egress_quota).toBe(Number(initialQuota))
  })

  it('correctly decrements cache miss quota on cache miss', async () => {
    const dataSetId = 'test-quota-decrement-miss'
    const initialQuota = 1000000
    const egressBytes = 100000

    await env.DB.prepare(
      'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
    )
      .bind(
        dataSetId,
        APPROVED_SERVICE_PROVIDER_ID,
        '0xtest',
        true,
        initialQuota,
        initialQuota,
      )
      .run()

    await updateDataSetStats(env, {
      dataSetId,
      egressBytes,
      cacheMiss: true,
    })

    const result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .first()

    const expectedQuota = Number(initialQuota) - egressBytes
    expect(result.cdn_egress_quota).toBe(expectedQuota)
    expect(result.cache_miss_egress_quota).toBe(expectedQuota)
  })

  it('allows quota to go negative when egress exceeds quota', async () => {
    const dataSetId = 'test-quota-below-zero'
    const insufficientQuota = 1000
    const egressBytes = 2000

    await env.DB.prepare(
      'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
    )
      .bind(
        dataSetId,
        APPROVED_SERVICE_PROVIDER_ID,
        '0xtest',
        true,
        insufficientQuota,
        insufficientQuota,
      )
      .run()

    // Should allow quota to go negative
    await updateDataSetStats(env, {
      dataSetId,
      egressBytes,
      cacheMiss: true,
    })

    const result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .first()

    // Both quotas should be negative (1000 - 2000 = -1000)
    expect(result.cdn_egress_quota).toBe(-1000)
    expect(result.cache_miss_egress_quota).toBe(-1000)
  })

  it('allows CDN quota to go negative when egress exceeds quota', async () => {
    const dataSetId = 'test-cdn-quota-below-zero'
    const insufficientQuota = 500
    const sufficientQuota = 1000000
    const egressBytes = 1500

    await env.DB.prepare(
      'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
    )
      .bind(
        dataSetId,
        APPROVED_SERVICE_PROVIDER_ID,
        '0xtest',
        true,
        insufficientQuota,
        sufficientQuota,
      )
      .run()

    // Should allow CDN quota to go negative
    await updateDataSetStats(env, {
      dataSetId,
      egressBytes,
      cacheMiss: false,
    })

    const result = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .first()

    // CDN quota should be negative (500 - 1500 = -1000)
    expect(result.cdn_egress_quota).toBe(-1000)
    // Cache miss quota should remain unchanged on cache hit
    expect(result.cache_miss_egress_quota).toBe(sufficientQuota)
  })

  it('returns error when quota values are null', async () => {
    const dataSetId = 'test-quota-null'
    const pieceCid = 'test-cid-null'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'

    await env.DB.prepare(
      'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
    )
      .bind(
        dataSetId,
        APPROVED_SERVICE_PROVIDER_ID,
        payerAddress,
        true,
        null,
        null,
      )
      .run()
    await env.DB.prepare(
      'INSERT INTO pieces (id, data_set_id, cid) VALUES (?, ?, ?)',
    )
      .bind('piece-null', dataSetId, pieceCid)
      .run()

    // Should return 402 error when quotas are null
    await expect(
      getStorageProviderAndValidatePayer(env, payerAddress, pieceCid),
    ).rejects.toThrow(/CDN egress quota exhausted/)
  })

  it('handles quota exactly at piece size', async () => {
    const dataSetId = 'test-quota-exact'
    const pieceCid = 'test-cid-exact'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'
    const exactQuota = '1000'

    await env.DB.prepare(
      'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
    )
      .bind(
        dataSetId,
        APPROVED_SERVICE_PROVIDER_ID,
        payerAddress,
        true,
        exactQuota,
        exactQuota,
      )
      .run()
    await env.DB.prepare(
      'INSERT INTO pieces (id, data_set_id, cid) VALUES (?, ?, ?)',
    )
      .bind('piece-exact', dataSetId, pieceCid)
      .run()

    // Quota of exactly 1000 should be sufficient (> 0)
    const result = await getStorageProviderAndValidatePayer(
      env,
      payerAddress,
      pieceCid,
    )
    expect(result.dataSetId).toBe(dataSetId)

    // Decrement by exact amount should result in 0
    await updateDataSetStats(env, {
      dataSetId,
      egressBytes: 1000,
      cacheMiss: false,
    })

    const afterDecrement = await env.DB.prepare(
      'SELECT cdn_egress_quota FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .first()
    expect(afterDecrement.cdn_egress_quota).toBe(0)
  })
})

describe('updateDataSetStats', () => {
  it('updates egress stats', async () => {
    const DATA_SET_ID = 'test-data-set-1'
    const EGRESS_BYTES = 123456

    await withDataSetPieces(env, {
      dataSetId: DATA_SET_ID,
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
})
