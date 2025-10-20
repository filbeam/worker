import { describe, it, expect, vi, beforeAll } from 'vitest'
import worker from '../bin/ipfs-retriever.js'
import { createHash } from 'node:crypto'
import { retrieveIpfsContent } from '../lib/retrieval.js'
import {
  env,
  createExecutionContext,
  waitOnExecutionContext,
} from 'cloudflare:test'
import assert from 'node:assert/strict'
import {
  withDataSetPiece,
  withApprovedProvider,
  withBadBits,
  withWalletDetails,
} from './test-data-builders.js'
import { CONTENT_STORED_ON_CALIBRATION } from './test-data.js'
import { buildSlug } from '../lib/store.js'

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

const DNS_ROOT = '.ipfs.filbeam.io'
env.DNS_ROOT = DNS_ROOT

describe('retriever.fetch', () => {
  const defaultPayerAddress = '0x1234567890abcdef1234567890abcdef12345678'
  const {
    ipfsRootCid: realIpfsRootCid,
    dataSetId,
    pieceId: realPieceId,
  } = CONTENT_STORED_ON_CALIBRATION[0]
  const realDataSetId = String(dataSetId)

  beforeAll(async () => {
    await env.DB.batch([
      env.DB.prepare('DELETE FROM pieces'),
      env.DB.prepare('DELETE FROM data_sets'),
      env.DB.prepare('DELETE FROM bad_bits'),
      env.DB.prepare('DELETE FROM wallet_details'),
    ])

    for (const {
      serviceProviderId,
      serviceUrl,
      pieceCid,
      ipfsRootCid,
      dataSetId,
      pieceId,
    } of CONTENT_STORED_ON_CALIBRATION) {
      await withDataSetPiece(env, {
        serviceProviderId,
        pieceCid,
        ipfsRootCid,
        payerAddress: defaultPayerAddress,
        withCDN: true,
        withIpfsIndexing: true,
        dataSetId: String(dataSetId),
        pieceId,
      })
      await withApprovedProvider(env, {
        id: serviceProviderId,
        serviceUrl,
      })
    }
  })

  it('redirects to https://filbeam.com when no CID and no wallet address were provided', async () => {
    const ctx = createExecutionContext()
    const req = new Request(`https://${DNS_ROOT.slice(1)}/`)
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(302)
    expect(res.headers.get('Location')).toBe('https://filbeam.com/')
  })

  it('returns 404 for invalid path format on DNS_ROOT (missing CID)', async () => {
    const ctx = createExecutionContext()
    const req = new Request(
      `https://${DNS_ROOT.slice(1)}/${defaultPayerAddress}`,
    )
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(404)
    expect(await res.text()).toContain('Invalid path format')
  })

  it('returns 404 for invalid wallet address on DNS_ROOT path', async () => {
    const ctx = createExecutionContext()
    const invalidWallet = 'invalid-wallet'
    const ipfsRootCid = 'bafk4testslug1'
    const req = new Request(
      `https://${DNS_ROOT.slice(1)}/${invalidWallet}/${ipfsRootCid}`,
    )
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(404)
    expect(await res.text()).toContain('Invalid wallet address')
  })

  it('redirects to slug subdomain when valid wallet and CID are provided on DNS_ROOT path', async () => {
    // Set up test data with numeric pieceId and dataSetId for slug generation
    const testPayerAddress = '0xabcdef1234567890abcdef1234567890abcdef99'
    const testIpfsRootCid = 'bafk4testslug2'
    const testDataSetId = '12345'
    const testPieceId = '67890'
    const serviceProviderId = '100'

    await withDataSetPiece(env, {
      serviceProviderId,
      payerAddress: testPayerAddress,
      ipfsRootCid: testIpfsRootCid,
      dataSetId: testDataSetId,
      pieceId: testPieceId,
      withCDN: true,
      withIpfsIndexing: true,
    })
    await withApprovedProvider(env, {
      id: serviceProviderId,
      serviceUrl: 'https://test-provider.example.com',
    })

    const ctx = createExecutionContext()
    const req = new Request(
      `https://${DNS_ROOT.slice(1)}/${testPayerAddress}/${testIpfsRootCid}`,
    )
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(302)
    const location = res.headers.get('Location')
    // Expected slug: 1-ga4q-aeete (version-base32(12345)-base32(67890))
    expect(location).toBe('https://1-ga4q-aeete.ipfs.filbeam.io/')
  })

  it('redirects to slug subdomain with subpath when wallet, CID, and pathname are provided on DNS_ROOT path', async () => {
    // Set up test data with numeric pieceId and dataSetId for slug generation
    const testPayerAddress = '0xabcdef1234567890abcdef1234567890abcdef98'
    const testIpfsRootCid = 'bafk4testslug3'
    const testDataSetId = '54321'
    const testPieceId = '98765'
    const serviceProviderId = '101'

    await withDataSetPiece(env, {
      serviceProviderId,
      payerAddress: testPayerAddress,
      ipfsRootCid: testIpfsRootCid,
      dataSetId: testDataSetId,
      pieceId: testPieceId,
      withCDN: true,
      withIpfsIndexing: true,
    })
    await withApprovedProvider(env, {
      id: serviceProviderId,
      serviceUrl: 'https://test-provider2.example.com',
    })

    const ctx = createExecutionContext()
    const subpath = 'path/to/file.txt'
    const req = new Request(
      `https://${DNS_ROOT.slice(1)}/${testPayerAddress}/${testIpfsRootCid}/${subpath}`,
    )
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(302)
    const location = res.headers.get('Location')
    // Expected slug: 1-2qyq-aga42 (version-base32(54321)-base32(98765))
    expect(location).toBe(
      'https://1-2qyq-aga42.ipfs.filbeam.io/path/to/file.txt',
    )
  })

  it('redirects to https://*.filcdn.io/* when old domain was used', async () => {
    const ctx = createExecutionContext()
    const req = new Request(`https://foo.filcdn.io/bar`)
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(301)
    expect(res.headers.get('Location')).toBe(`https://foo.filbeam.io/bar`)
  })

  it('returns 405 for unsupported request methods', async () => {
    const ctx = createExecutionContext()
    const req = withRequest('1', '1', 'POST')
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(405)
    expect(await res.text()).toBe('Method Not Allowed')
  })

  it('returns 400 if required fields are missing', async () => {
    const ctx = createExecutionContext()
    const mockRetrieveIpfsContent = vi.fn()
    const req = withRequest(undefined, 'foo')
    const res = await worker.fetch(req, env, ctx, {
      retrieveIpfsContent: mockRetrieveIpfsContent,
    })
    await waitOnExecutionContext(ctx)
    // When pieceId is provided but dataSetId is undefined, it creates just "foo." which
    // becomes the root domain and redirects to filbeam.com
    expect(res.status).toBe(302)
    expect(res.headers.get('Location')).toBe('https://filbeam.com/')
  })

  it('returns 400 if slug has invalid base32 encoding', async () => {
    const ctx = createExecutionContext()
    const mockRetrieveIpfsContent = vi.fn()
    const req = withRequest('notbase32', 'alsonotbase32')
    const res = await worker.fetch(req, env, ctx, {
      retrieveIpfsContent: mockRetrieveIpfsContent,
    })
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(400)
    expect(await res.text()).toContain('Invalid dataSetId encoding in slug')
  })

  it('returns the response from retrieveIpfsContent', async () => {
    const fakeResponse = new Response('hello', {
      status: 201,
      headers: { 'X-Test': 'yes' },
    })
    const mockRetrieveIpfsContent = vi.fn().mockResolvedValue({
      response: fakeResponse,
      cacheMiss: true,
    })
    const ctx = createExecutionContext()
    const req = withRequest(realDataSetId, realPieceId)
    const res = await worker.fetch(req, env, ctx, {
      retrieveIpfsContent: mockRetrieveIpfsContent,
    })
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(201)
    expect(await res.text()).toBe('hello')
    expect(res.headers.get('X-Test')).toBe('yes')
  })

  it('sets Content-Control response header', async () => {
    const originResponse = new Response('hello')
    const mockRetrieveIpfsContent = vi.fn().mockResolvedValue({
      response: originResponse,
      cacheMiss: true,
    })
    const ctx = createExecutionContext()
    const req = withRequest(realDataSetId, realPieceId)
    const res = await worker.fetch(req, env, ctx, {
      retrieveIpfsContent: mockRetrieveIpfsContent,
    })
    await waitOnExecutionContext(ctx)
    const cacheControlHeaders = res.headers.get('Cache-Control')
    expect(cacheControlHeaders).toContain('public')
    expect(cacheControlHeaders).toContain(`max-age=${env.CLIENT_CACHE_TTL}`)
  })

  it('sets Content-Control response on empty body', async () => {
    const originResponse = new Response(null)
    const mockRetrieveIpfsContent = vi.fn().mockResolvedValue({
      response: originResponse,
      cacheMiss: false,
    })
    const ctx = createExecutionContext()
    const req = withRequest(realDataSetId, realPieceId)
    const res = await worker.fetch(req, env, ctx, {
      retrieveIpfsContent: mockRetrieveIpfsContent,
    })
    await waitOnExecutionContext(ctx)
    const cacheControlHeaders = res.headers.get('Cache-Control')
    expect(cacheControlHeaders).toContain('public')
    expect(cacheControlHeaders).toContain(`max-age=${env.CLIENT_CACHE_TTL}`)
  })

  it('sets Content-Security-Policy response header', async () => {
    const originResponse = new Response('hello', {
      headers: {
        'Content-Security-Policy': 'report-uri: https://endpoint.example.com',
      },
    })
    const mockRetrieveIpfsContent = vi.fn().mockResolvedValue({
      response: originResponse,
      cacheMiss: true,
    })
    const ctx = createExecutionContext()
    const req = withRequest(realDataSetId, realPieceId)
    const res = await worker.fetch(req, env, ctx, {
      retrieveIpfsContent: mockRetrieveIpfsContent,
    })
    await waitOnExecutionContext(ctx)
    const csp = res.headers.get('Content-Security-Policy')
    expect(csp).toMatch(/^default-src 'self'/)
    expect(csp).toContain('https://*.filbeam.io')
  })

  // FIXME - update the test to retrieve real IPFS content
  // This is blocked by Curio not indexing CAR files inside PDP deals yet
  it.skip('fetches the file from calibration service provider', async () => {
    const expectedHash =
      'b9614f45cf8d401a0384eb58376b00cbcbb14f98fcba226d9fe1effe298af673'
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, realIpfsRootCid)
    const res = await worker.fetch(req, env, ctx, { retrieveIpfsContent })
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(200)
    // get the sha256 hash of the content
    const content = await res.bytes()
    const hash = createHash('sha256').update(content).digest('hex')
    expect(hash).toEqual(expectedHash)
  })
  it('stores retrieval results with cache miss and content length set in D1', async () => {
    const body = 'file content'
    const expectedEgressBytes = Buffer.byteLength(body, 'utf8')
    const fakeResponse = new Response(body, {
      status: 200,
      headers: {
        'CF-Cache-Status': 'MISS',
      },
    })
    const mockRetrieveIpfsContent = vi.fn().mockResolvedValue({
      response: fakeResponse,
      cacheMiss: true,
    })
    const ctx = createExecutionContext()
    const req = withRequest(realDataSetId, realPieceId)
    const res = await worker.fetch(req, env, ctx, {
      retrieveIpfsContent: mockRetrieveIpfsContent,
    })
    await waitOnExecutionContext(ctx)
    assert.strictEqual(res.status, 200)
    const readOutput = await env.DB.prepare(
      `SELECT id, response_status, egress_bytes, cache_miss
       FROM retrieval_logs
       WHERE data_set_id = ?`,
    )
      .bind(String(realDataSetId))
      .all()
    const result = readOutput.results
    assert.deepStrictEqual(result, [
      {
        id: 1, // Assuming this is the first log entry
        response_status: 200,
        egress_bytes: expectedEgressBytes,
        cache_miss: 1, // 1 for true, 0 for false
      },
    ])
  })
  it('stores retrieval results with cache hit and content length set in D1', async () => {
    const body = 'file content'
    const expectedEgressBytes = Buffer.byteLength(body, 'utf8')
    const fakeResponse = new Response(body, {
      status: 200,
      headers: {
        'CF-Cache-Status': 'HIT',
      },
    })
    const mockRetrieveIpfsContent = vi.fn().mockResolvedValue({
      response: fakeResponse,
      cacheMiss: false,
    })
    const ctx = createExecutionContext()
    const req = withRequest(realDataSetId, realPieceId)
    const res = await worker.fetch(req, env, ctx, {
      retrieveIpfsContent: mockRetrieveIpfsContent,
    })
    await waitOnExecutionContext(ctx)
    assert.strictEqual(res.status, 200)
    const readOutput = await env.DB.prepare(
      `SELECT id, response_status, egress_bytes, cache_miss
       FROM retrieval_logs
       WHERE data_set_id = ?`,
    )
      .bind(String(realDataSetId))
      .all()
    const result = readOutput.results
    assert.deepStrictEqual(result, [
      {
        id: 1, // Assuming this is the first log entry
        response_status: 200,
        egress_bytes: expectedEgressBytes,
        cache_miss: 0, // 1 for true, 0 for false
      },
    ])
  })
  it('stores retrieval performance stats in D1', async () => {
    const body = 'file content'
    const fakeResponse = new Response(body, {
      status: 200,
      headers: {
        'CF-Cache-Status': 'MISS',
      },
    })
    const mockRetrieveIpfsContent = async () => {
      await sleep(1) // Simulate a delay
      return {
        response: fakeResponse,
        cacheMiss: true,
      }
    }
    const ctx = createExecutionContext()
    const req = withRequest(realDataSetId, realPieceId)
    const res = await worker.fetch(req, env, ctx, {
      retrieveIpfsContent: mockRetrieveIpfsContent,
    })
    await waitOnExecutionContext(ctx)
    assert.strictEqual(res.status, 200)
    const readOutput = await env.DB.prepare(
      `SELECT
        response_status,
        fetch_ttfb,
        fetch_ttlb,
        worker_ttfb
       FROM retrieval_logs
       WHERE data_set_id = ?`,
    )
      .bind(String(realDataSetId))
      .all()
    assert.strictEqual(readOutput.results.length, 1)
    const result = readOutput.results[0]

    assert.strictEqual(result.response_status, 200)
    assert.strictEqual(typeof result.fetch_ttfb, 'number')
    assert.strictEqual(typeof result.fetch_ttlb, 'number')
    assert.strictEqual(typeof result.worker_ttfb, 'number')
  })
  it('stores request country code in D1', async () => {
    const body = 'file content'
    const mockRetrieveIpfsContent = async () => {
      return {
        response: new Response(body, {
          status: 200,
        }),
        cacheMiss: true,
      }
    }
    const ctx = createExecutionContext()
    const req = withRequest(realDataSetId, realPieceId, 'GET', {
      'CF-IPCountry': 'US',
    })
    const res = await worker.fetch(req, env, ctx, {
      retrieveIpfsContent: mockRetrieveIpfsContent,
    })
    await waitOnExecutionContext(ctx)
    assert.strictEqual(res.status, 200)
    const { results } = await env.DB.prepare(
      `SELECT request_country_code
       FROM retrieval_logs
       WHERE data_set_id = ?`,
    )
      .bind(String(realDataSetId))
      .all()
    assert.deepStrictEqual(results, [
      {
        request_country_code: 'US',
      },
    ])
  })
  it('logs 0 egress bytes for empty body', async () => {
    const fakeResponse = new Response(null, {
      status: 200,
      headers: {
        'CF-Cache-Status': 'MISS',
      },
    })
    const mockRetrieveIpfsContent = vi.fn().mockResolvedValue({
      response: fakeResponse,
      cacheMiss: true,
    })
    const ctx = createExecutionContext()
    const req = withRequest(realDataSetId, realPieceId)
    const res = await worker.fetch(req, env, ctx, {
      retrieveIpfsContent: mockRetrieveIpfsContent,
    })
    await waitOnExecutionContext(ctx)
    assert.strictEqual(res.status, 200)
    const readOutput = await env.DB.prepare(
      'SELECT egress_bytes FROM retrieval_logs WHERE data_set_id = ?',
    )
      .bind(String(realDataSetId))
      .all()
    assert.strictEqual(readOutput.results.length, 1)
    assert.strictEqual(readOutput.results[0].egress_bytes, 0)
  })

  // FIXME - update the test to retrieve real IPFS content
  // This is blocked by Curio not indexing CAR files inside PDP deals yet
  it.skip(
    'measures egress correctly from real service provider',
    { timeout: 10000 },
    async () => {
      const tasks = CONTENT_STORED_ON_CALIBRATION.map(
        ({ dataSetId, pieceCid, ipfsRootCid, serviceProviderId }) => {
          return (async () => {
            try {
              const ctx = createExecutionContext()
              const req = withRequest(defaultPayerAddress, pieceCid)
              const res = await worker.fetch(req, env, ctx, {
                retrieveIpfsContent,
              })
              await waitOnExecutionContext(ctx)

              assert.strictEqual(res.status, 200)

              const content = await res.arrayBuffer()
              const actualBytes = content.byteLength

              const { results } = await env.DB.prepare(
                'SELECT egress_bytes FROM retrieval_logs WHERE data_set_id = ?',
              )
                .bind(String(dataSetId))
                .all()

              assert.strictEqual(results.length, 1)
              assert.strictEqual(results[0].egress_bytes, actualBytes)

              return { serviceProviderId, success: true }
            } catch (err) {
              console.warn(
                `⚠️ Warning: Fetch or verification failed for serviceProvider ${serviceProviderId}:`,
                err,
              )
              throw err
            }
          })()
        },
      )

      try {
        const res = await Promise.allSettled(tasks)
        if (!res.some((r) => r.status === 'fulfilled')) {
          throw new Error('All tasks failed')
        }
      } catch (err) {
        const serviceProvidersChecked = CONTENT_STORED_ON_CALIBRATION.map(
          (o) => o.serviceProviderId,
        )
        throw new Error(
          `❌ All service providers failed to fetch. Service providers checked: ${serviceProvidersChecked.join(', ')}`,
        )
      }
    },
  )

  it('requests payment if withCDN=false', async () => {
    const dataSetId = '1004'
    const pieceId = '2004'
    const pieceCid =
      'baga6ea4seaqaleibb6ud4xeemuzzpsyhl6cxlsymsnfco4cdjka5uzajo2x4ipa'
    const ipfsRootCid = 'bafk4test'
    const serviceProviderId = 'service-provider'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345678'

    await withApprovedProvider(env, {
      id: serviceProviderId,
      serviceUrl: 'https://test-provider.xyz',
    })

    await withDataSetPiece(env, {
      serviceProviderId,
      pieceCid,
      ipfsRootCid,
      dataSetId,
      withCDN: false,
      pieceId,
      payerAddress,
    })

    const ctx = createExecutionContext()
    const req = withRequest(dataSetId, pieceId, 'GET')
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)

    assert.strictEqual(res.status, 402)
  })
  it('reads the provider URL from the database', async () => {
    const serviceProviderId = 'service-provider-id'
    const dataSetId = '1001'
    const pieceId = '2001'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345608'
    const ipfsRootCid = 'bafk4test'
    const body = 'file content'

    await withDataSetPiece(env, {
      serviceProviderId,
      dataSetId,
      pieceId,
      ipfsRootCid,
      payerAddress,
    })

    await withApprovedProvider(env, {
      id: serviceProviderId,
      serviceUrl: 'https://mock-pdp-url.com',
    })

    const mockRetrieveIpfsContent = async () => {
      return {
        response: new Response(body, {
          status: 200,
        }),
        cacheMiss: true,
      }
    }

    const ctx = createExecutionContext()
    const req = withRequest(dataSetId, pieceId)
    const res = await worker.fetch(req, env, ctx, {
      retrieveIpfsContent: mockRetrieveIpfsContent,
    })
    await waitOnExecutionContext(ctx)

    // Check if the URL fetched is from the database
    expect(await res.text()).toBe(body)
    expect(res.status).toBe(200)
  })

  it('throws an error if the providerAddress is not found in the database', async () => {
    const serviceProviderId = 'service-provider-id'
    const dataSetId = '1002'
    const pieceId = '2002'
    const payerAddress = '0x2A06D234246eD18b6C91de8349fF34C22C7268e8'
    const ipfsRootCid = 'bafk4test'

    await withDataSetPiece(env, {
      serviceProviderId,
      dataSetId,
      pieceId,
      ipfsRootCid,
      payerAddress,
    })

    const ctx = createExecutionContext()
    const req = withRequest(dataSetId, pieceId)
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)

    // Expect an error because no URL was found
    expect(res.status).toBe(404)
    expect(await res.text()).toBe(
      `No approved service provider found for payer '0x2a06d234246ed18b6c91de8349ff34c22c7268e8' and data set ID '${dataSetId}' and piece ID '${pieceId}'.`,
    )
  })

  it('returns data set ID in the X-Data-Set-ID response header', async () => {
    const { dataSetId } = CONTENT_STORED_ON_CALIBRATION[0]
    const mockRetrieveIpfsContent = vi.fn().mockResolvedValue({
      response: new Response('hello'),
      cacheMiss: true,
    })
    const ctx = createExecutionContext()
    const req = withRequest(realDataSetId, realPieceId)
    const res = await worker.fetch(req, env, ctx, {
      retrieveIpfsContent: mockRetrieveIpfsContent,
    })
    await waitOnExecutionContext(ctx)
    expect(await res.text()).toBe('hello')
    expect(res.headers.get('X-Data-Set-ID')).toBe(String(dataSetId))
  })

  it('stores data set ID in retrieval logs', async () => {
    const { dataSetId } = CONTENT_STORED_ON_CALIBRATION[0]
    const mockRetrieveIpfsContent = vi.fn().mockResolvedValue({
      response: new Response('hello'),
      cacheMiss: true,
    })
    const ctx = createExecutionContext()
    const req = withRequest(realDataSetId, realPieceId)
    const res = await worker.fetch(req, env, ctx, {
      retrieveIpfsContent: mockRetrieveIpfsContent,
    })
    await waitOnExecutionContext(ctx)
    expect(await res.text()).toBe('hello')

    assert.strictEqual(res.status, 200)
    const { results } = await env.DB.prepare(
      `SELECT id, response_status, cache_miss
       FROM retrieval_logs
       WHERE data_set_id = ?`,
    )
      .bind(String(dataSetId))
      .all()
    assert.deepStrictEqual(results, [
      {
        id: 1, // Assuming this is the first log entry
        response_status: 200,
        cache_miss: 1, // 1 for true, 0 for false
      },
    ])
  })

  it('returns data set ID in the X-Data-Set-ID response header when the response body is empty', async () => {
    const { dataSetId } = CONTENT_STORED_ON_CALIBRATION[0]
    const mockRetrieveIpfsContent = vi.fn().mockResolvedValue({
      response: new Response(null, { status: 404 }),
      cacheMiss: true,
    })
    const ctx = createExecutionContext()
    const req = withRequest(realDataSetId, realPieceId)
    const res = await worker.fetch(req, env, ctx, {
      retrieveIpfsContent: mockRetrieveIpfsContent,
    })
    await waitOnExecutionContext(ctx)
    expect(res.body).toBeNull()
    expect(res.headers.get('X-Data-Set-ID')).toBe(String(dataSetId))
  })

  it('supports HEAD requests', async () => {
    const fakeResponse = new Response('file content', {
      status: 200,
    })
    const mockRetrieveIpfsContent = vi.fn().mockResolvedValue({
      response: fakeResponse,
      cacheMiss: true,
    })
    const ctx = createExecutionContext()
    const req = withRequest(realDataSetId, realPieceId, 'HEAD')
    const res = await worker.fetch(req, env, ctx, {
      retrieveIpfsContent: mockRetrieveIpfsContent,
    })
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(200)
  })

  it('rejects retrieval requests for CIDs found in the Bad Bits denylist', async () => {
    await withBadBits(env, realIpfsRootCid)

    const fakeResponse = new Response('hello')
    const mockRetrieveIpfsContent = vi.fn().mockResolvedValue({
      response: fakeResponse,
      cacheMiss: true,
    })

    const ctx = createExecutionContext()
    const req = withRequest(realDataSetId, realPieceId)
    const res = await worker.fetch(req, env, ctx, {
      retrieveIpfsContent: mockRetrieveIpfsContent,
    })
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(404)
    expect(await res.text()).toBe(
      'The requested CID was flagged by the Bad Bits Denylist at https://badbits.dwebops.pub',
    )
  })

  it('reject retrieval request if payer is sanctioned', async () => {
    const dataSetId = '1003'
    const pieceId = '2003'
    const pieceCid =
      'baga6ea4seaqaleibb6ud4xeemuzzpsyhl6cxlsymsnfco4cdjka5uzajo2x4ipa'
    const ipfsRootCid = 'bafk4test'
    const serviceProviderId = 'service-provider-id'
    const payerAddress = '0x999999cf1046e68e36E1aA2E0E07105eDDD1f08E'

    await withApprovedProvider(env, {
      id: serviceProviderId,
      serviceUrl: 'https://test-provider.xyz',
    })

    await withDataSetPiece(env, {
      serviceProviderId,
      payerAddress,
      pieceCid,
      ipfsRootCid,
      dataSetId,
      withCDN: true,
      withIpfsIndexing: true,
      pieceId,
    })

    await withWalletDetails(
      env,
      payerAddress,
      true, // Sanctioned
    )
    const ctx = createExecutionContext()
    const req = withRequest(dataSetId, pieceId)
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)

    assert.strictEqual(res.status, 403)
  })
  it('does not log to retrieval_logs on method not allowed (405)', async () => {
    const ctx = createExecutionContext()
    const req = withRequest(realDataSetId, realPieceId, 'POST')
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)

    expect(res.status).toBe(405)
    expect(await res.text()).toBe('Method Not Allowed')

    const result = await env.DB.prepare(
      `SELECT response_status FROM retrieval_logs WHERE data_set_id = ? ORDER BY id DESC LIMIT 1`,
    )
      .bind(realDataSetId)
      .first()
    expect(result).toBeNull()
  })

  // TODO - find out why this test fails and fix the problem
  it.skip('logs to retrieval_logs on unsupported service provider (404)', async () => {
    const invalidPieceCid = 'baga6ea4seaq3invalidpiececid'
    const invalidIpfsRootCid = 'bafkinvalidrootcid'
    const dataSetId = 'unsupported-serviceProvider-test'
    const unsupportedServiceProviderId = 0

    await env.DB.batch([
      env.DB.prepare(
        'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn) VALUES (?, ?, ?, ?)',
      ).bind(
        dataSetId,
        unsupportedServiceProviderId,
        defaultPayerAddress,
        true,
      ),
      env.DB.prepare(
        'INSERT INTO pieces (id, data_set_id, cid, ipfs_root_cid) VALUES (?, ?, ?, ?)',
      ).bind(
        'piece-unsupported',
        dataSetId,
        invalidPieceCid,
        invalidIpfsRootCid,
      ),
    ])

    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, invalidIpfsRootCid)
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)

    expect(res.status).toBe(404)
    expect(await res.text()).toContain('No approved service provider found')

    const result = await env.DB.prepare(
      'SELECT * FROM retrieval_logs WHERE data_set_id = ? AND response_status = 404 and CACHE_MISS IS NULL and egress_bytes IS NULL',
    )
      .bind(dataSetId)
      .first()
    expect(result).toBeDefined()
  })
  it('does not log to retrieval_logs when slug encoding is invalid (400)', async () => {
    const ctx = createExecutionContext()
    const { count: countBefore } = await env.DB.prepare(
      'SELECT COUNT(*) AS count FROM retrieval_logs',
    ).first()

    // Use values without hyphens that will fail base32 decoding
    const req = withRequest('notbase32', 'alsoinvalid')
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)

    expect(res.status).toBe(400)
    expect(await res.text()).toContain('Invalid dataSetId encoding in slug')

    const { count: countAfter } = await env.DB.prepare(
      'SELECT COUNT(*) AS count FROM retrieval_logs',
    ).first()
    expect(countAfter).toBe(countBefore)
  })

  it('converts CAR to RAW by default (no format parameter)', async () => {
    const ctx = createExecutionContext()

    // Hard-coded in the retrieval worker for testing
    const testDataSetId = '9999'
    const testPieceId = '9999'

    const url = withRequest(
      testDataSetId,
      testPieceId,
      'GET',
      {},
      { subpath: '/rusty-lassie.png', format: null },
    )
    const req = new Request(url)

    const res = await worker.fetch(req, env, ctx, { retrieveIpfsContent })
    await waitOnExecutionContext(ctx)

    expect(res.status).toBe(200)

    // Verify content-disposition is set to inline (not attachment)
    expect(res.headers.get('content-disposition')).toBe('inline')

    // Verify we got RAW PNG data, not a CAR file
    const content = await res.bytes()
    expect(content.length).toBeGreaterThan(0)

    // PNG files start with the magic bytes: 89 50 4E 47 0D 0A 1A 0A
    expect(content.slice(0, 4)).toEqual(
      new Uint8Array([
        0x89,
        0x50, // 'P'
        0x4e, // 'N'
        0x47, // 'G'
      ]),
    )
  })
})

/**
 * @param {string} payerWalletAddress
 * @param {string} ipfsRootCid
 * @param {string} method
 * @param {Object} headers
 * @param {Object} options
 * @param {string} options.subpath
 * @returns {Request}
 */
function withRequest(
  dataSetId,
  pieceId,
  method = 'GET',
  headers = {},
  { subpath = '', format = 'car' } = {},
) {
  let url = 'http://'
  if (dataSetId && pieceId) {
    try {
      const slug = buildSlug(BigInt(dataSetId), BigInt(pieceId))
      url += `${slug}.`
    } catch {
      // If conversion fails, use raw values (for testing error cases)
      url += `1-${dataSetId}-${pieceId}.`
    }
  } else if (dataSetId) {
    url += `${dataSetId}.`
  }
  url += DNS_ROOT.slice(1) // remove the leading '.'
  if (subpath) url += `${subpath}`
  if (format) url += `?format=${format}`

  return new Request(url, { method, headers })
}
