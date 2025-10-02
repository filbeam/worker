import { describe, it, beforeAll } from 'vitest'
import assert from 'node:assert/strict'
import {
  logRetrievalResult,
  getStorageProviderAndValidatePayer,
  updateDataSetStats,
  getSlugForWalletAndCid,
} from '../lib/store.js'
import { env } from 'cloudflare:test'
import { withDataSetPiece, withApprovedProvider } from './test-data-builders.js'

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
    assert.deepStrictEqual(result, [
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

  it('returns service provider for valid ipfsRootCid', async () => {
    const dataSetId = 'test-set-1'
    const ipfsRootCid = 'bafk4test'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'

    await env.DB.prepare(
      'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, with_ipfs_indexing) VALUES (?, ?, ?, ?, ?)',
    )
      .bind(dataSetId, APPROVED_SERVICE_PROVIDER_ID, payerAddress, true, true)
      .run()
    await env.DB.prepare(
      'INSERT INTO pieces (id, data_set_id, cid, ipfs_root_cid) VALUES (?, ?, ?, ?)',
    )
      .bind('piece-1', dataSetId, 'baga4piece', ipfsRootCid)
      .run()

    const result = await getStorageProviderAndValidatePayer(
      env,
      payerAddress,
      ipfsRootCid,
    )
    assert.strictEqual(result.serviceProviderId, APPROVED_SERVICE_PROVIDER_ID)
  })

  it('throws error if ipfsRootCid not found', async () => {
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'
    await assert.rejects(
      async () =>
        await getStorageProviderAndValidatePayer(
          env,
          payerAddress,
          'nonexistent-cid',
        ),
      /does not exist/,
    )
  })

  it('throws error if data_set_id exists but has no associated service provider', async () => {
    const cid = 'cid-no-owner'
    const dataSetId = 'data-set-no-owner'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'

    await env.DB.prepare(
      `
      INSERT INTO pieces (id, data_set_id, cid, ipfs_root_cid)
      VALUES (?, ?, ?, ?)
    `,
    )
      .bind('piece-1', dataSetId, `bagatestpiece`, cid)
      .run()

    await assert.rejects(
      async () =>
        await getStorageProviderAndValidatePayer(env, payerAddress, cid),
      /no associated service provider/,
    )
  })

  it('returns error if no payment rail', async () => {
    const cid = 'cid-unapproved'
    const dataSetId = 'data-set-unapproved'
    const serviceProviderId = APPROVED_SERVICE_PROVIDER_ID
    const payerAddress = '0xabcdef1234567890abcdef1234567890abcdef12'

    await env.DB.batch([
      env.DB.prepare(
        'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn) VALUES (?, ?, ?, ?)',
      ).bind(
        dataSetId,
        serviceProviderId,
        payerAddress.replace('a', 'b'),
        true,
      ),
      env.DB.prepare(
        'INSERT INTO pieces (id, data_set_id, cid, ipfs_root_cid) VALUES (?, ?, ?, ?)',
      ).bind('piece-2', dataSetId, 'bagatest', cid),
    ])

    await assert.rejects(
      async () =>
        await getStorageProviderAndValidatePayer(env, payerAddress, cid),
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
        'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn) VALUES (?, ?, ?, ?)',
      ).bind(dataSetId, serviceProviderId, payerAddress, false),
      env.DB.prepare(
        'INSERT INTO pieces (id, data_set_id, cid, ipfs_root_cid) VALUES (?, ?, ?, ?)',
      ).bind('piece-2', dataSetId, 'bagatest', cid),
    ])

    await assert.rejects(
      async () =>
        await getStorageProviderAndValidatePayer(env, payerAddress, cid),
      /withCDN=false/,
    )
  })

  it('returns serviceProviderId for approved service provider', async () => {
    const cid = 'cid-approved'
    const dataSetId = 'data-set-approved'
    const payerAddress = '0xabcdef1234567890abcdef1234567890abcdef12'

    await env.DB.batch([
      env.DB.prepare(
        'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, with_ipfs_indexing) VALUES (?, ?, ?, ?, ?)',
      ).bind(dataSetId, APPROVED_SERVICE_PROVIDER_ID, payerAddress, true, true),
      env.DB.prepare(
        'INSERT INTO pieces (id, data_set_id, cid, ipfs_root_cid) VALUES (?, ?, ?, ?)',
      ).bind('piece-3', dataSetId, 'bagatest', cid),
    ])

    const result = await getStorageProviderAndValidatePayer(
      env,
      payerAddress,
      cid,
    )

    assert.strictEqual(result.serviceProviderId, APPROVED_SERVICE_PROVIDER_ID)
  })
  it('returns the service provider first in the ordering when multiple service providers share the same ipfsRootCid', async () => {
    const dataSetId1 = 'data-set-a'
    const dataSetId2 = 'data-set-b'
    const ipfsRootCid = 'shared-ipfs-cid'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'
    const serviceProviderId1 = 'service-provider-a'
    const serviceProviderId2 = 'service-provicer-b'

    await withApprovedProvider(env, {
      id: serviceProviderId1,
    })
    await withApprovedProvider(env, {
      id: serviceProviderId2,
    })

    // Insert both owners into separate sets with the same ipfsRootCid
    await env.DB.prepare(
      'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, with_ipfs_indexing) VALUES (?, ?, ?, ?, ?)',
    )
      .bind(dataSetId1, serviceProviderId1, payerAddress, true, true)
      .run()

    await env.DB.prepare(
      'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn) VALUES (?, ?, ?, ?)',
    )
      .bind(dataSetId2, serviceProviderId2, payerAddress, true)
      .run()

    // Insert same ipfsRootCid for both sets
    await env.DB.prepare(
      'INSERT INTO pieces (id, data_set_id, cid, ipfs_root_cid) VALUES (?, ?, ?, ?)',
    )
      .bind('piece-a', dataSetId1, 'bagatest', ipfsRootCid)
      .run()

    await env.DB.prepare(
      'INSERT INTO pieces (id, data_set_id, cid, ipfs_root_cid) VALUES (?, ?, ?, ?)',
    )
      .bind('piece-b', dataSetId2, 'bagatest', ipfsRootCid)
      .run()

    // Should return only the serviceProviderId1 which is the first in the ordering
    const result = await getStorageProviderAndValidatePayer(
      env,
      payerAddress,
      ipfsRootCid,
    )
    assert.strictEqual(result.serviceProviderId, serviceProviderId1)
  })

  it('ignores owners that are not approved by Filecoin Warm Storage Service', async () => {
    const dataSetId1 = '0'
    const dataSetId2 = '1'
    const ipfsRootCid = 'shared-piece-cid'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'
    const serviceProviderId1 = '0'
    const serviceProviderId2 = '1'

    await withApprovedProvider(env, {
      id: serviceProviderId1,
      serviceUrl: 'https://pdp-provider-1.xyz',
    })

    // NOTE: the second provider is not registered as an approved provider

    // Important: we must insert the unapproved provider first!
    await withDataSetPiece(env, {
      payerAddress,
      serviceProviderId: serviceProviderId2,
      dataSetId: dataSetId2,
      withCDN: true,
      ipfsRootCid,
    })

    await withDataSetPiece(env, {
      payerAddress,
      serviceProviderId: serviceProviderId1,
      dataSetId: dataSetId1,
      withCDN: true,
      ipfsRootCid,
    })

    // Should return service provider 1 because service provider 2 is not approved
    const result = await getStorageProviderAndValidatePayer(
      env,
      payerAddress,
      ipfsRootCid,
    )
    assert.deepStrictEqual(result, {
      dataSetId: dataSetId1,
      serviceProviderId: serviceProviderId1.toLowerCase(),
      serviceUrl: 'https://pdp-provider-1.xyz',
    })
  })
})

describe('updateDataSetStats', () => {
  it('updates egress stats', async () => {
    const DATA_SET_ID = 'test-data-set-1'
    const EGRESS_BYTES = 123456

    await withDataSetPiece(env, {
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

    assert.deepStrictEqual(insertResults, [
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

    assert.deepStrictEqual(updateResults, [
      {
        id: DATA_SET_ID,
        total_egress_bytes_used: EGRESS_BYTES + 1000,
      },
    ])
  })
})

describe('getSlugForWalletAndCid', () => {
  const APPROVED_SERVICE_PROVIDER_ID = '30'
  beforeAll(async () => {
    await withApprovedProvider(env, {
      id: APPROVED_SERVICE_PROVIDER_ID,
      serviceUrl: 'https://approved-provider-slug.xyz',
    })
  })

  it('returns slug with version, dataSetId and pieceId encoded in base32', async () => {
    const dataSetId = '12345'
    const pieceId = '67890'
    const ipfsRootCid = 'bafk4slugtest1'
    const payerAddress = '0xabcdef1234567890abcdef1234567890abcdef34'

    await withDataSetPiece(env, {
      payerAddress,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      dataSetId,
      pieceId,
      withCDN: true,
      withIpfsIndexing: true,
      ipfsRootCid,
    })

    const result = await getSlugForWalletAndCid(env, payerAddress, ipfsRootCid)

    // Slug format: version-base32(dataSetId)-base32(pieceId)
    assert.strictEqual(result, '1-bga4q-baeete')
  })

  it('returns slug with zero-encoded values for dataSetId=0 and pieceId=0', async () => {
    const dataSetId = '0'
    const pieceId = '0'
    const ipfsRootCid = 'bafk4slugtest2'
    const payerAddress = '0xabcdef1234567890abcdef1234567890abcdef35'

    await withDataSetPiece(env, {
      payerAddress,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      dataSetId,
      pieceId,
      withCDN: true,
      withIpfsIndexing: true,
      ipfsRootCid,
    })

    const result = await getSlugForWalletAndCid(env, payerAddress, ipfsRootCid)

    // For dataSetId=0 and pieceId=0, bigIntToBase32 returns '0'
    assert.strictEqual(result, '1-0-0')
  })

  it('throws error for invalid payer address', async () => {
    const dataSetId = '99999'
    const pieceId = '88888'
    const ipfsRootCid = 'bafk4slugtest3'
    const validPayerAddress = '0xabcdef1234567890abcdef1234567890abcdef36'
    const invalidPayerAddress = '0x0000000000000000000000000000000000000000'

    await withDataSetPiece(env, {
      payerAddress: validPayerAddress,
      serviceProviderId: APPROVED_SERVICE_PROVIDER_ID,
      dataSetId,
      pieceId,
      withCDN: true,
      withIpfsIndexing: true,
      ipfsRootCid,
    })

    await assert.rejects(
      async () =>
        await getSlugForWalletAndCid(env, invalidPayerAddress, ipfsRootCid),
      /There is no Filecoin Warm Storage Service deal for payer/,
    )
  })
})
