import { describe, it, expect, vi, beforeAll } from 'vitest'
import worker from '../bin/piece-retriever.js'
import { createHash } from 'node:crypto'
import { retrieveFile } from '../lib/retrieval.js'
import {
  env,
  createExecutionContext,
  waitOnExecutionContext,
} from 'cloudflare:test'
import assert from 'node:assert/strict'
import {
  withDataSetPieces,
  withApprovedProvider,
  withBadBits,
  withWalletDetails,
} from './test-data-builders.js'
import { CONTENT_STORED_ON_CALIBRATION } from './test-data.js'

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

const DNS_ROOT = '.filbeam.io'
env.DNS_ROOT = DNS_ROOT

describe('piece-retriever.fetch', () => {
  const defaultPayerAddress = '0x1234567890abcdef1234567890abcdef12345678'
  const { pieceCid: realPieceCid, dataSetId: realDataSetId } =
    CONTENT_STORED_ON_CALIBRATION[0]

  beforeAll(async () => {
    await env.DB.batch([
      env.DB.prepare('DELETE FROM pieces'),
      env.DB.prepare('DELETE FROM data_sets'),
      env.DB.prepare('DELETE FROM wallet_details'),
    ])

    let cursor
    while (true) {
      const list = await env.BAD_BITS_KV.list({ cursor })
      for (const key of list.keys) {
        await env.BAD_BITS_KV.delete(key)
      }
      if (list.list_complete) break
      cursor = list.cursor
    }

    let i = 1
    for (const {
      serviceProviderId,
      serviceUrl,
      pieceCid,
      dataSetId,
    } of CONTENT_STORED_ON_CALIBRATION) {
      const pieceId = `root-${i}`
      await withDataSetPieces(env, {
        serviceProviderId,
        pieceCid,
        payerAddress: defaultPayerAddress,
        withCDN: true,
        dataSetId,
        pieceId,
      })
      await withApprovedProvider(env, {
        id: serviceProviderId,
        serviceUrl,
      })
      i++
    }
  })

  it('redirects to https://filbeam.com when no CID was provided', async () => {
    const ctx = createExecutionContext()
    const req = new Request(`https://${defaultPayerAddress}${DNS_ROOT}/`)
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(302)
    expect(res.headers.get('Location')).toBe('https://filbeam.com/')
  })

  it('redirects to https://filbeam.com when no CID and no wallet address were provided', async () => {
    const ctx = createExecutionContext()
    const req = new Request(`https://${DNS_ROOT.slice(1)}/`)
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(302)
    expect(res.headers.get('Location')).toBe('https://filbeam.com/')
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
    const req = withRequest(1, 'foo', 'POST')
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(405)
    expect(await res.text()).toBe('Method Not Allowed')
  })

  it('returns 400 if required fields are missing', async () => {
    const ctx = createExecutionContext()
    const mockRetrieveFile = vi.fn()
    const req = withRequest(undefined, 'foo')
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
    })
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(400)
    expect(await res.text()).toBe(
      'Invalid hostname: filbeam.io. It must end with .filbeam.io.',
    )
  })

  it('returns 400 if provided payer address is invalid', async () => {
    const ctx = createExecutionContext()
    const mockRetrieveFile = vi.fn()
    const req = withRequest('bar', realPieceCid)
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
    })
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(400)
    expect(await res.text()).toBe(
      'Invalid address: bar. Address must be a valid ethereum address.',
    )
  })

  it('returns the response from retrieveFile', async () => {
    const fakeResponse = new Response('hello', {
      status: 201,
      headers: { 'X-Test': 'yes' },
    })
    const mockRetrieveFile = vi.fn().mockResolvedValue({
      response: fakeResponse,
      cacheMiss: true,
    })
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, realPieceCid)
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
    })
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(201)
    expect(await res.text()).toBe('hello')
    expect(res.headers.get('X-Test')).toBe('yes')
  })

  it('sets Content-Control response header', async () => {
    const originResponse = new Response('hello')
    const mockRetrieveFile = vi.fn().mockResolvedValue({
      response: originResponse,
      cacheMiss: true,
    })
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, realPieceCid)
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
    })
    await waitOnExecutionContext(ctx)
    const cacheControlHeaders = res.headers.get('Cache-Control')
    expect(cacheControlHeaders).toContain('public')
    expect(cacheControlHeaders).toContain(`max-age=${env.CLIENT_CACHE_TTL}`)
  })

  it('sets Content-Control response on empty body', async () => {
    const originResponse = new Response(null)
    const mockRetrieveFile = vi.fn().mockResolvedValue({
      response: originResponse,
      cacheMiss: false,
    })
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, realPieceCid)
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
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
    const mockRetrieveFile = vi.fn().mockResolvedValue({
      response: originResponse,
      cacheMiss: true,
    })
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, realPieceCid)
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
    })
    await waitOnExecutionContext(ctx)
    const csp = res.headers.get('Content-Security-Policy')
    expect(csp).toMatch(/^default-src 'self'/)
    expect(csp).toContain('https://*.filbeam.io')
  })

  it('fetches the file from calibration service provider', async () => {
    const expectedHash =
      'b9614f45cf8d401a0384eb58376b00cbcbb14f98fcba226d9fe1effe298af673'
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, realPieceCid)
    const res = await worker.fetch(req, env, ctx, { retrieveFile })
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
    const mockRetrieveFile = vi.fn().mockResolvedValue({
      response: fakeResponse,
      cacheMiss: true,
    })
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, realPieceCid)
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
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
    const mockRetrieveFile = vi.fn().mockResolvedValue({
      response: fakeResponse,
      cacheMiss: false,
    })
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, realPieceCid)
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
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
    const mockRetrieveFile = async () => {
      await sleep(1) // Simulate a delay
      return {
        response: fakeResponse,
        cacheMiss: true,
      }
    }
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, realPieceCid)
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
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
    const mockRetrieveFile = async () => {
      return {
        response: new Response(body, {
          status: 200,
        }),
        cacheMiss: true,
      }
    }
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, realPieceCid, 'GET', {
      'CF-IPCountry': 'US',
    })
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
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
    const mockRetrieveFile = vi.fn().mockResolvedValue({
      response: fakeResponse,
      cacheMiss: true,
    })
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, realPieceCid)
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
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
  it(
    'measures egress correctly from real service provider',
    { timeout: 10000 },
    async () => {
      const tasks = CONTENT_STORED_ON_CALIBRATION.map(
        ({ dataSetId, pieceCid, serviceProviderId }) => {
          return (async () => {
            try {
              const ctx = createExecutionContext()
              const req = withRequest(defaultPayerAddress, pieceCid)
              const res = await worker.fetch(req, env, ctx, { retrieveFile })
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
    const dataSetId = 'test-data-set-no-cdn'
    const pieceId = 'root-no-cdn'
    const pieceCid =
      'baga6ea4seaqaleibb6ud4xeemuzzpsyhl6cxlsymsnfco4cdjka5uzajo2x4ipa'
    const serviceProviderId = 'service-provider'
    await withDataSetPieces(env, {
      serviceProviderId,
      pieceCid,
      dataSetId,
      withCDN: false,
      pieceId,
    })

    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, pieceCid, 'GET')
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)

    assert.strictEqual(res.status, 402)
  })
  it('reads the provider URL from the database', async () => {
    const serviceProviderId = 'service-provider-id'
    const payerAddress = '0x1234567890abcdef1234567890abcdef12345608'
    const pieceCid = 'bagaTest'
    const body = 'file content'

    await withDataSetPieces(env, {
      serviceProviderId,
      pieceCid,
      payerAddress,
    })

    await withApprovedProvider(env, {
      id: serviceProviderId,
      serviceUrl: 'https://mock-pdp-url.com',
    })

    const mockRetrieveFile = async () => {
      return {
        response: new Response(body, {
          status: 200,
        }),
        cacheMiss: true,
      }
    }

    const ctx = createExecutionContext()
    const req = withRequest(payerAddress, pieceCid)
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
    })
    await waitOnExecutionContext(ctx)

    // Check if the URL fetched is from the database
    expect(await res.text()).toBe(body)
    expect(res.status).toBe(200)
  })

  it('throws an error if the providerAddress is not found in the database', async () => {
    const serviceProviderId = 'service-provider-id'
    const payerAddress = '0x2A06D234246eD18b6C91de8349fF34C22C7268e8'
    const pieceCid = 'bagaTest'

    await withDataSetPieces(env, {
      serviceProviderId,
      pieceCid,
      payerAddress,
    })

    const ctx = createExecutionContext()
    const req = withRequest(payerAddress, pieceCid)
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)

    // Expect an error because no URL was found
    expect(res.status).toBe(404)
    expect(await res.text()).toBe(
      `No approved service provider found for payer '0x2a06d234246ed18b6c91de8349ff34c22c7268e8' and piece_cid 'bagaTest'.`,
    )
  })

  it('returns data set ID in the X-Data-Set-ID response header', async () => {
    const { pieceCid, dataSetId } = CONTENT_STORED_ON_CALIBRATION[0]
    const mockRetrieveFile = vi.fn().mockResolvedValue({
      response: new Response('hello'),
      cacheMiss: true,
    })
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, pieceCid)
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
    })
    await waitOnExecutionContext(ctx)
    expect(await res.text()).toBe('hello')
    expect(res.headers.get('X-Data-Set-ID')).toBe(String(dataSetId))
  })

  it('stores data set ID in retrieval logs', async () => {
    const { pieceCid, dataSetId } = CONTENT_STORED_ON_CALIBRATION[0]
    const mockRetrieveFile = vi.fn().mockResolvedValue({
      response: new Response('hello'),
      cacheMiss: true,
    })
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, pieceCid)
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
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
    const { pieceCid, dataSetId } = CONTENT_STORED_ON_CALIBRATION[0]
    const mockRetrieveFile = vi.fn().mockResolvedValue({
      response: new Response(null, { status: 404 }),
      cacheMiss: true,
    })
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, pieceCid)
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
    })
    await waitOnExecutionContext(ctx)
    expect(res.body).toBeNull()
    expect(res.headers.get('X-Data-Set-ID')).toBe(String(dataSetId))
  })

  it('supports HEAD requests', async () => {
    const fakeResponse = new Response('file content', {
      status: 200,
    })
    const mockRetrieveFile = vi.fn().mockResolvedValue({
      response: fakeResponse,
      cacheMiss: true,
    })
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, realPieceCid, 'HEAD')
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
    })
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(200)
  })

  it('rejects retrieval requests for CIDs found in the Bad Bits denylist', async () => {
    await withBadBits(env, realPieceCid)

    const fakeResponse = new Response('hello')
    const mockRetrieveFile = vi.fn().mockResolvedValue({
      response: fakeResponse,
      cacheMiss: true,
    })

    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, realPieceCid)
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
    })
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(404)
    expect(await res.text()).toBe(
      'The requested CID was flagged by the Bad Bits Denylist at https://badbits.dwebops.pub',
    )
  })

  it('reject retrieval request if payer is sanctioned', async () => {
    const dataSetId = 'test-data-set-payer-sanctioned'
    const pieceId = 'root-data-set-payer-sanctioned'
    const pieceCid =
      'baga6ea4seaqaleibb6ud4xeemuzzpsyhl6cxlsymsnfco4cdjka5uzajo2x4ipa'
    const serviceProviderId = 'service-provider-id'
    const payerAddress = '0x999999cf1046e68e36E1aA2E0E07105eDDD1f08E'
    await withDataSetPieces(env, {
      serviceProviderId,
      payerAddress,
      pieceCid,
      dataSetId,
      withCDN: true,
      pieceId,
    })

    await withWalletDetails(
      env,
      payerAddress,
      true, // Sanctioned
    )
    const ctx = createExecutionContext()
    const req = withRequest(payerAddress, pieceCid, 'GET')
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)

    assert.strictEqual(res.status, 403)
  })
  it('does not log to retrieval_logs on method not allowed (405)', async () => {
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, realPieceCid, 'POST')
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
  it('logs to retrieval_logs on unsupported service provider (404)', async () => {
    const invalidPieceCid = 'baga6ea4seaq3invalidrootcidfor404loggingtest'
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
        'INSERT INTO pieces (id, data_set_id, cid) VALUES (?, ?, ?)',
      ).bind('piece-unsupported', dataSetId, invalidPieceCid),
    ])

    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, invalidPieceCid)
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
  it('does not log to retrieval_logs when payer address is invalid (400)', async () => {
    const { count: countBefore } = await env.DB.prepare(
      'SELECT COUNT(*) AS count FROM retrieval_logs',
    ).first()

    const invalidAddress = 'not-an-address'
    const ctx = createExecutionContext()
    const req = withRequest(invalidAddress, realPieceCid)
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)

    expect(res.status).toBe(400)
    expect(await res.text()).toContain('Invalid address')

    const { count: countAfter } = await env.DB.prepare(
      'SELECT COUNT(*) AS count FROM retrieval_logs',
    ).first()

    expect(countAfter).toEqual(countBefore)
  })
  it('responds with 502 and a useful message when SP is unavailable', async () => {
    const { pieceCid, dataSetId } = CONTENT_STORED_ON_CALIBRATION[0]
    const url = 'https://example.com/piece/123'
    const mockRetrieveFile = vi.fn().mockResolvedValue({
      response: new Response(null, { status: 500 }),
      cacheMiss: true,
      url,
    })
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, pieceCid)
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
    })
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(502)
    expect(await res.text()).toBe(`Service provider 2 is unavailable at ${url}`)
    expect(res.headers.get('X-Data-Set-ID')).toBe(String(dataSetId))
  })
})

/**
 * @param {string} payerWalletAddress
 * @param {string} pieceCid
 * @param {string} method
 * @param {Object} headers
 * @returns {Request}
 */
function withRequest(
  payerWalletAddress,
  pieceCid,
  method = 'GET',
  headers = {},
) {
  let url = 'http://'
  if (payerWalletAddress) url += `${payerWalletAddress}.`
  url += DNS_ROOT.slice(1) // remove the leading '.'
  if (pieceCid) url += `/${pieceCid}`

  return new Request(url, { method, headers })
}
