import { describe, it, expect, vi, beforeAll } from 'vitest'
import worker from '../bin/piece-retriever.js'
import { createHash } from 'node:crypto'
import { retrieveFile } from '../lib/retrieval.js'
import {
  env,
  createExecutionContext,
  waitOnExecutionContext,
} from 'cloudflare:test'
import {
  withDataSetPieces,
  withApprovedProvider,
  withBadBits,
  withWalletDetails,
  withRequest,
} from './test-helpers.js'
import { CONTENT_STORED_ON_CALIBRATION } from './test-data.js'

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

export const DNS_ROOT = '.filbeam.io'
env.DNS_ROOT = DNS_ROOT
const botTokens = { secret: 'testbot' }
env.BOT_TOKENS = JSON.stringify(botTokens)

const botName = Object.values(botTokens)[0]
const botHeaders = { authorization: `Bearer ${Object.keys(botTokens)[0]}` }

describe('piece-retriever.fetch', () => {
  const defaultPayerAddress = '0xc83dbfdf61616778537211a7e5ca2e87ec6cf0ed'
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
        pieceId,
        pieceCid,
        dataSetId,
        serviceProviderId,
        payerAddress: defaultPayerAddress,
        withCDN: true,
        cdnEgressQuota: 100,
        cacheMissEgressQuota: 100,
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
      '3fde6bc0f4d21dd3b033b6100e3fa4023810f699b005b556bd28909b39fd87cf'
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
    expect(res.status).toBe(200)
    const readOutput = await env.DB.prepare(
      `SELECT id, response_status, egress_bytes, cache_miss, bot_name
       FROM retrieval_logs
       WHERE data_set_id = ?`,
    )
      .bind(String(realDataSetId))
      .all()
    expect(readOutput.results).toStrictEqual([
      {
        id: 1, // Assuming this is the first log entry
        response_status: 200,
        egress_bytes: expectedEgressBytes,
        cache_miss: 1, // 1 for true, 0 for false
        bot_name: null, // No authorization header provided
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
    expect(res.status).toBe(200)
    const readOutput = await env.DB.prepare(
      `SELECT id, response_status, egress_bytes, cache_miss, bot_name
       FROM retrieval_logs
       WHERE data_set_id = ?`,
    )
      .bind(String(realDataSetId))
      .all()
    expect(readOutput.results).toStrictEqual([
      {
        id: 1, // Assuming this is the first log entry
        response_status: 200,
        egress_bytes: expectedEgressBytes,
        cache_miss: 0, // 1 for true, 0 for false
        bot_name: null, // No authorization header provided
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
    expect(res.status).toBe(200)
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
    expect(readOutput.results.length).toBe(1)
    const result = readOutput.results[0]

    expect(result.response_status).toBe(200)
    expect(typeof result.fetch_ttfb).toBe('number')
    expect(typeof result.fetch_ttlb).toBe('number')
    expect(typeof result.worker_ttfb).toBe('number')
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
    expect(res.status).toBe(200)
    const { results } = await env.DB.prepare(
      `SELECT request_country_code
       FROM retrieval_logs
       WHERE data_set_id = ?`,
    )
      .bind(String(realDataSetId))
      .all()
    expect(results).toStrictEqual([
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
    expect(res.status).toBe(200)
    const readOutput = await env.DB.prepare(
      'SELECT egress_bytes FROM retrieval_logs WHERE data_set_id = ?',
    )
      .bind(String(realDataSetId))
      .all()
    expect(readOutput.results).toStrictEqual([
      expect.objectContaining({
        egress_bytes: 0,
      }),
    ])
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

              expect(res.status).toBe(200)

              const content = await res.arrayBuffer()
              const actualBytes = content.byteLength

              const { results } = await env.DB.prepare(
                'SELECT egress_bytes FROM retrieval_logs WHERE data_set_id = ?',
              )
                .bind(String(dataSetId))
                .all()

              expect(results).toStrictEqual([
                expect.objectContaining({
                  egress_bytes: actualBytes,
                }),
              ])

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

  it('charges bots for egress', async () => {
    const botToken = Object.keys(botTokens)[0]
    /** @type {string} */
    const botName = env.BOT_TOKENS[botToken]
    console.log({ botToken, botName })

    const mockRetrieveFile = vi.fn().mockResolvedValue({
      response: new Response('fake'),
      cacheMiss: true,
    })
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, realPieceCid, 'GET', {
      authorization: `Bearer ${botToken}`,
    })
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
    })
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(200)
    const readOutput = await env.DB.prepare(
      'SELECT egress_bytes FROM retrieval_logs WHERE data_set_id = ?',
    )
      .bind(String(realDataSetId))
      .all()
    expect(readOutput.results).toStrictEqual([
      expect.objectContaining({
        egress_bytes: 4,
      }),
    ])
  })

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

    expect(res.status).toBe(402)
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
      cdnEgressQuota: 100,
      cacheMissEgressQuota: 100,
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

    expect(res.status).toBe(200)
    const { results } = await env.DB.prepare(
      `SELECT id, response_status, cache_miss
       FROM retrieval_logs
       WHERE data_set_id = ?`,
    )
      .bind(String(dataSetId))
      .all()
    expect(results).toStrictEqual([
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
      dataSetId,
      withCDN: true,
      pieceCid,
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

    expect(res.status).toBe(403)
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

    await withDataSetPieces(env, {
      dataSetId,
      serviceProviderId: unsupportedServiceProviderId,
      payerAddress: defaultPayerAddress,
      withCDN: true,
      cdnEgressQuota: 100,
      cacheMissEgressQuota: 100,
      pieceCid: invalidPieceCid,
      pieceId: 'piece-unsupported',
    })

    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, invalidPieceCid)
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)

    expect(res.status).toBe(404)
    expect(await res.text()).toContain('No approved service provider found')

    const result = await env.DB.prepare(
      'SELECT * FROM retrieval_logs WHERE data_set_id IS NULL AND response_status = 404 and CACHE_MISS IS NULL and egress_bytes IS NULL',
    ).first()
    expect(result).toMatchObject({
      bot_name: null,
    })
  })
  it('logs to retrieval_logs on SP error', async () => {
    const { pieceCid, dataSetId } = CONTENT_STORED_ON_CALIBRATION[0]
    const url = 'https://example.com/piece/123'
    const mockRetrieveFile = vi.fn().mockResolvedValue({
      response: new Response(null, { status: 510 }),
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

    const result = await env.DB.prepare(
      'SELECT * FROM retrieval_logs WHERE data_set_id = ? AND response_status = 502 and CACHE_MISS IS NULL and egress_bytes IS NULL',
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

  it('allows full transfer even when exceeding quota (quota goes negative)', async () => {
    const payerAddress = '0xaaaa567890abcdef1234567890abcdef12345678'
    const pieceCid =
      'bafkquotatestexceedquotatestexceedquotatestexceedquotatestexce'
    const dataSetId = 'quota-test-dataset-exceed'
    const serviceProviderId = 'quota-test-provider-exceed'

    // Set up provider and data set with small quota (100 bytes)
    await withApprovedProvider(env, {
      id: serviceProviderId,
      serviceUrl: 'https://test-provider.com',
    })

    await withDataSetPieces(env, {
      dataSetId,
      serviceProviderId,
      payerAddress,
      withCDN: true,
      cdnEgressQuota: 100,
      cacheMissEgressQuota: 100,
      pieceCid,
      pieceId: 'piece-quota-test',
    })

    // Mock a response with more data than quota allows (500 bytes)
    const largeContent = new Uint8Array(500).fill(65) // 500 'A's
    const fakeResponse = new Response(largeContent, {
      status: 200,
      headers: { 'CF-Cache-Status': 'MISS', 'Content-Length': '500' },
    })

    const ctx = createExecutionContext()
    const res = await worker.fetch(
      withRequest(payerAddress, pieceCid),
      { ...env, ENFORCE_EGRESS_QUOTA: true },
      ctx,
      {
        retrieveFile: async () => ({ response: fakeResponse, cacheMiss: true }),
      },
    )
    await waitOnExecutionContext(ctx)

    // Should get full content even when quota is exceeded
    // Response should be successful with all 500 bytes
    const body = await res.arrayBuffer()
    expect(body.byteLength).toBe(500)

    // Check logs after execution context completes
    const { results } = await env.DB.prepare(
      'SELECT egress_bytes, response_status FROM retrieval_logs WHERE data_set_id = ?',
    )
      .bind(dataSetId)
      .all()

    expect(results).toStrictEqual([
      {
        egress_bytes: 500,
        response_status: 200,
      },
    ])

    // Check that both quotas went negative (100 - 500 = -400)
    const quotaResult = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_set_egress_quotas WHERE data_set_id = ?',
    )
      .bind(dataSetId)
      .first()

    expect(quotaResult).toStrictEqual({
      cdn_egress_quota: -400,
      cache_miss_egress_quota: -400,
    })
  })

  it('allows full transfer when within quota', async () => {
    const payerAddress = '0xbbbb567890abcdef1234567890abcdef12345678'
    const pieceCid =
      'bafkquotaokquotaokquotaokquotaokquotaokquotaokquotaokquotaok'
    const dataSetId = 'quota-ok-dataset-unique'
    const serviceProviderId = 'quota-ok-provider-unique'

    // Set up provider and data set with sufficient quota (1000 bytes)
    await withApprovedProvider(env, {
      id: serviceProviderId,
      serviceUrl: 'https://test-provider.com',
    })

    await withDataSetPieces(env, {
      dataSetId,
      serviceProviderId,
      payerAddress,
      withCDN: true,
      cdnEgressQuota: 1000,
      cacheMissEgressQuota: 1000,
      pieceCid,
      pieceId: 'piece-quota-ok',
    })

    // Mock a response that fits within quota (100 bytes)
    const content = new Uint8Array(100).fill(65) // 100 'A's
    const fakeResponse = new Response(content, {
      status: 200,
      headers: { 'CF-Cache-Status': 'HIT', 'Content-Length': '100' },
    })

    const ctx = createExecutionContext()
    const res = await worker.fetch(
      withRequest(payerAddress, pieceCid),
      { ...env, ENFORCE_EGRESS_QUOTA: true },
      ctx,
      {
        retrieveFile: async () => ({
          response: fakeResponse,
          cacheMiss: false,
        }),
      },
    )
    await waitOnExecutionContext(ctx)

    // Should succeed
    expect(res.status).toBe(200)
    const body = await res.arrayBuffer()
    expect(body.byteLength).toBe(100)

    // Check that full content was logged
    const { results } = await env.DB.prepare(
      'SELECT egress_bytes, response_status FROM retrieval_logs WHERE data_set_id = ?',
    )
      .bind(dataSetId)
      .all()

    expect(results).toStrictEqual([
      {
        egress_bytes: 100,
        response_status: 200,
      },
    ])

    // Check that quotas were decremented correctly (cache hit)
    const quotaResult = await env.DB.prepare(
      'SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_set_egress_quotas WHERE data_set_id = ?',
    )
      .bind(dataSetId)
      .first()

    expect(quotaResult).toStrictEqual({
      cdn_egress_quota: 900,
      cache_miss_egress_quota: 1000,
    })
  })

  it('responds with 502 and a useful message when SP responds with an error', async () => {
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
    expect(await res.text()).toMatch(
      /^No available service provider found. Attempted: ID=/,
    )
    expect(res.headers.get('X-Data-Set-ID')).toBe(String(dataSetId))

    const result = await env.DB.prepare(
      'SELECT * FROM retrieval_logs WHERE data_set_id = ?',
    )
      .bind(String(dataSetId))
      .first()
    expect(result).toMatchObject({ bot_name: null })
  })

  it('stores bot name in retrieval logs when valid authorization header is provided', async () => {
    const body = 'file content'
    const fakeResponse = new Response(body, {
      status: 200,
    })
    const mockRetrieveFile = vi.fn().mockResolvedValue({
      response: fakeResponse,
      cacheMiss: true,
    })
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, realPieceCid, 'GET', {
      ...botHeaders,
    })
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
    })
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(200)

    const result = await env.DB.prepare(
      'SELECT * FROM retrieval_logs WHERE data_set_id = ?',
    )
      .bind(String(realDataSetId))
      .first()

    expect(result).toMatchObject({ bot_name: botName })
  })

  it('stores bot name in retrieval logs for empty response body', async () => {
    const mockRetrieveFile = vi.fn().mockResolvedValue({
      response: new Response(null, { status: 404 }),
      cacheMiss: false,
    })
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, realPieceCid, 'GET', {
      ...botHeaders,
    })
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
    })
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(404)

    const result = await env.DB.prepare(
      'SELECT * FROM retrieval_logs WHERE data_set_id = ?',
    )
      .bind(String(realDataSetId))
      .first()
    expect(result).toMatchObject({ egress_bytes: 0, bot_name: botName })
  })

  it('stores bot name in retrieval logs when SP returns 502', async () => {
    const { pieceCid, dataSetId } = CONTENT_STORED_ON_CALIBRATION[0]
    const url = 'https://example.com/piece/502test'
    const mockRetrieveFile = vi.fn().mockResolvedValue({
      response: new Response(null, { status: 503 }),
      cacheMiss: true,
      url,
    })
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, pieceCid, 'GET', {
      ...botHeaders,
    })
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
    })
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(502)

    const result = await env.DB.prepare(
      'SELECT * FROM retrieval_logs WHERE data_set_id = ?',
    )
      .bind(String(dataSetId))
      .first()
    expect(result).toMatchObject({ egress_bytes: 0, bot_name: botName })
  })

  it('stores bot name in retrieval logs on error (404 unsupported SP)', async () => {
    const invalidPieceCid = 'baga6ea4seaq3invalidbotnameerrortest'
    const dataSetId = 'bot-name-error-test'
    const unsupportedServiceProviderId = 0

    await withDataSetPieces(env, {
      serviceProviderId: unsupportedServiceProviderId,
      pieceCid: invalidPieceCid,
      payerAddress: defaultPayerAddress,
      dataSetId,
      withCDN: true,
      pieceId: 'piece-bot-error',
    })

    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, invalidPieceCid, 'GET', {
      ...botHeaders,
    })
    const res = await worker.fetch(req, env, ctx)
    await waitOnExecutionContext(ctx)

    expect(res.status).toBe(404)

    const result = await env.DB.prepare(
      'SELECT * FROM retrieval_logs WHERE data_set_id IS NULL',
    ).first()
    expect(result).toMatchObject({
      cache_miss: null,
      egress_bytes: null,
      bot_name: botName,
    })
  })
  it('responds with 502 and a useful message when SP is unavailable', async () => {
    const { pieceCid, dataSetId } = CONTENT_STORED_ON_CALIBRATION[0]
    const mockRetrieveFile = vi.fn().mockRejectedValue(new Error('oh no'))
    const ctx = createExecutionContext()
    const req = withRequest(defaultPayerAddress, pieceCid)
    const res = await worker.fetch(req, env, ctx, {
      retrieveFile: mockRetrieveFile,
    })
    await waitOnExecutionContext(ctx)
    expect(res.status).toBe(502)
    expect(await res.text()).toMatch(
      /^No available service provider found. Attempted: ID=/,
    )
    expect(res.headers.get('X-Data-Set-ID')).toBe(String(dataSetId))
  })
})
