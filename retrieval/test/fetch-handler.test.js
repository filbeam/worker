import { describe, it, expect } from 'vitest'
import { handleFetchRequest } from '../lib/fetch-handler.js'
import {
  env,
  createExecutionContext,
  waitOnExecutionContext,
} from 'cloudflare:test'

const testEnv = {
  ...env,
  CLIENT_CACHE_TTL: 31536000,
  ENFORCE_EGRESS_QUOTA: false,
  BOT_TOKENS: '{}',
}

function retrievalResult(overrides = {}) {
  return {
    response: new Response('hello world', { status: 200 }),
    cacheMiss: true,
    dataSetId: 'fh-test',
    fetchStartedAt: performance.now(),
    finalizeCacheMiss: async () => ({ cacheMissResponseValid: true }),
    ...overrides,
  }
}

/** A `run` that resolves to a retrieve function yielding the given outcome. */
function runYielding(overrides = {}) {
  return async () => async () => retrievalResult(overrides)
}

describe('handleFetchRequest', () => {
  it('returns a plain handler response unchanged', async () => {
    const ctx = createExecutionContext()
    const res = await handleFetchRequest(
      new Request('https://example.com/'),
      testEnv,
      ctx,
      async () => new Response('ok', { status: 200 }),
    )

    expect(res.status).toBe(200)
    expect(await res.text()).toBe('ok')
  })

  it('rejects non-GET/HEAD methods with 405 without running the handler', async () => {
    const ctx = createExecutionContext()
    let ran = false
    const res = await handleFetchRequest(
      new Request('https://example.com/', { method: 'POST' }),
      testEnv,
      ctx,
      async () => {
        ran = true
        return new Response('ok')
      },
    )

    expect(res.status).toBe(405)
    expect(await res.text()).toBe('Method Not Allowed')
    expect(ran).toBe(false)
  })

  it('allows HEAD requests', async () => {
    const ctx = createExecutionContext()
    const res = await handleFetchRequest(
      new Request('https://example.com/', { method: 'HEAD' }),
      testEnv,
      ctx,
      async () => new Response('ok', { status: 200 }),
    )

    expect(res.status).toBe(200)
  })

  it('redirects legacy *.filcdn.io requests before running the handler', async () => {
    const ctx = createExecutionContext()
    let ran = false
    const res = await handleFetchRequest(
      new Request('https://0xabc.filcdn.io/baga123?format=car'),
      testEnv,
      ctx,
      async () => {
        ran = true
        return new Response('ok')
      },
    )

    expect(res.status).toBe(301)
    expect(res.headers.get('Location')).toBe(
      'https://0xabc.filbeam.io/baga123?format=car',
    )
    expect(ran).toBe(false)
  })

  it('turns a thrown error into a response via handleError', async () => {
    const ctx = createExecutionContext()
    const res = await handleFetchRequest(
      new Request('https://example.com/'),
      testEnv,
      ctx,
      async () => {
        throw Object.assign(new Error('Bad Request'), { status: 400 })
      },
    )

    expect(res.status).toBe(400)
    expect(await res.text()).toBe('Bad Request')
  })

  it('hides the message for server errors', async () => {
    const ctx = createExecutionContext()
    const res = await handleFetchRequest(
      new Request('https://example.com/'),
      testEnv,
      ctx,
      async () => {
        throw new Error('boom')
      },
    )

    expect(res.status).toBe(500)
    expect(await res.text()).toBe('Internal Server Error')
  })

  it('logs the error status with no egress and no data set when the retrieval throws', async () => {
    const ctx = createExecutionContext()
    const res = await handleFetchRequest(
      new Request('https://example.com/', {
        headers: { 'CF-IPCountry': 'US' },
      }),
      testEnv,
      ctx,
      async () => async () => {
        throw Object.assign(new Error("I'm a teapot"), { status: 418 })
      },
    )
    expect(res.status).toBe(418)
    await waitOnExecutionContext(ctx)

    const log = await env.DB.prepare(
      `SELECT response_status, egress_bytes, cache_miss, data_set_id
       FROM retrieval_logs WHERE response_status = 418`,
    ).first()
    expect(log).toEqual({
      response_status: 418,
      egress_bytes: null,
      cache_miss: null,
      data_set_id: null,
    })
  })

  it('streams a retrieval result, measuring egress and logging it', async () => {
    const ctx = createExecutionContext()
    const dataSetId = 'fh-stream'
    let finalizedEgress
    const res = await handleFetchRequest(
      new Request('https://example.com/', {
        headers: { 'CF-IPCountry': 'US' },
      }),
      testEnv,
      ctx,
      runYielding({
        dataSetId,
        finalizeCacheMiss: async (egressBytes) => {
          finalizedEgress = egressBytes
          return { cacheMissResponseValid: true }
        },
      }),
    )

    expect(res.status).toBe(200)
    expect(res.headers.get('X-Data-Set-ID')).toBe(dataSetId)
    expect(res.headers.get('Content-Security-Policy')).toMatch(
      /^default-src 'self'/,
    )
    expect(await res.text()).toBe('hello world')
    await waitOnExecutionContext(ctx)

    expect(finalizedEgress).toBe('hello world'.length)
    const log = await env.DB.prepare(
      `SELECT response_status, egress_bytes, cache_miss, request_country_code
       FROM retrieval_logs WHERE data_set_id = ?`,
    )
      .bind(dataSetId)
      .first()
    // request_country_code is sourced by handleFetchRequest from the request.
    expect(log).toEqual({
      response_status: 200,
      egress_bytes: 'hello world'.length,
      cache_miss: 1,
      request_country_code: 'US',
    })
  })

  it('logs a zero-egress result for a response without a body', async () => {
    const ctx = createExecutionContext()
    const dataSetId = 'fh-empty'
    const res = await handleFetchRequest(
      new Request('https://example.com/', {
        headers: { 'CF-IPCountry': 'US', authorization: 'Bearer tok' },
      }),
      { ...testEnv, BOT_TOKENS: JSON.stringify({ tok: 'bot-1' }) },
      ctx,
      runYielding({
        dataSetId,
        response: new Response(null, { status: 404 }),
      }),
    )
    await waitOnExecutionContext(ctx)

    expect(res.status).toBe(404)
    expect(res.body).toBeNull()
    expect(res.headers.get('X-Data-Set-ID')).toBe(dataSetId)
    expect(res.headers.get('Cache-Control')).toBe(
      `public, max-age=${testEnv.CLIENT_CACHE_TTL}`,
    )
    const log = await env.DB.prepare(
      `SELECT response_status, egress_bytes, cache_miss_egress_bytes, cache_miss, cache_miss_response_valid, bot_name
       FROM retrieval_logs WHERE data_set_id = ?`,
    )
      .bind(dataSetId)
      .first()
    expect(log).toEqual({
      response_status: 404,
      egress_bytes: 0,
      cache_miss_egress_bytes: 0,
      cache_miss: 1,
      cache_miss_response_valid: null,
      bot_name: 'bot-1',
    })
  })

  it('logs a 900 result when streaming the body errors', async () => {
    const ctx = createExecutionContext()
    const dataSetId = 'fh-stream-error'
    const erroringBody = new ReadableStream({
      pull(controller) {
        controller.enqueue(new Uint8Array([1, 2, 3]))
        controller.error(new Error('stream boom'))
      },
    })
    const res = await handleFetchRequest(
      new Request('https://example.com/'),
      testEnv,
      ctx,
      runYielding({
        dataSetId,
        response: new Response(erroringBody, { status: 200 }),
      }),
    )
    expect(res.status).toBe(200)
    await waitOnExecutionContext(ctx)

    const log = await env.DB.prepare(
      'SELECT response_status FROM retrieval_logs WHERE data_set_id = ? AND response_status = 900',
    )
      .bind(dataSetId)
      .first()
    expect(log).toEqual({ response_status: 900 })
  })

  it('resolves the bot name from the Authorization header and logs it', async () => {
    const ctx = createExecutionContext()
    const dataSetId = 'fh-bot'
    const res = await handleFetchRequest(
      new Request('https://example.com/', {
        headers: { authorization: 'Bearer tok' },
      }),
      { ...testEnv, BOT_TOKENS: JSON.stringify({ tok: 'bot-1' }) },
      ctx,
      runYielding({ dataSetId }),
    )
    expect(res.status).toBe(200)
    expect(await res.text()).toBe('hello world')
    await waitOnExecutionContext(ctx)

    const log = await env.DB.prepare(
      'SELECT bot_name FROM retrieval_logs WHERE data_set_id = ?',
    )
      .bind(dataSetId)
      .first()
    expect(log).toEqual({ bot_name: 'bot-1' })
  })

  it('rejects an unknown bot token with 401 without running the retrieval', async () => {
    const ctx = createExecutionContext()
    let ran = false
    const res = await handleFetchRequest(
      new Request('https://example.com/', {
        headers: { authorization: 'Bearer wrong' },
      }),
      { ...testEnv, BOT_TOKENS: JSON.stringify({ tok: 'bot-1' }) },
      ctx,
      () => {
        ran = true
        return Promise.resolve(async () => retrievalResult())
      },
    )

    expect(res.status).toBe(401)
    expect(ran).toBe(false)
  })
})
