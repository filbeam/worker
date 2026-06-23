import { describe, it, expect } from 'vitest'
import { maybeHandleNoServiceProvider } from '../lib/retrieval-failure.js'
import {
  env,
  createExecutionContext,
  waitOnExecutionContext,
} from 'cloudflare:test'

const attempts = [
  { serviceProviderId: '1', serviceUrl: 'https://a.example/', dataSetId: '10' },
  { serviceProviderId: '2', serviceUrl: 'https://b.example/', dataSetId: '11' },
]

describe('maybeHandleNoServiceProvider', () => {
  it('responds with a 502 listing every attempted provider', async () => {
    const ctx = createExecutionContext()
    const response = maybeHandleNoServiceProvider(env, ctx, {
      retrievalResult: {
        response: new Response(null, { status: 503 }),
        cacheMiss: true,
      },
      attempts,
      dataSetId: '10',
      requestCountryCode: 'US',
      timestamp: new Date().toISOString(),
      botName: undefined,
    })
    await waitOnExecutionContext(ctx)

    expect(response.status).toBe(502)
    expect(response.headers.get('X-Data-Set-ID')).toBe('10,11')
    expect(await response.text()).toBe(
      'No available service provider found. Attempted: ID=1 (Service URL=https://a.example/), ID=2 (Service URL=https://b.example/)',
    )
  })

  it('sets a content security policy on the response', async () => {
    const ctx = createExecutionContext()
    const response = maybeHandleNoServiceProvider(env, ctx, {
      retrievalResult: {
        response: new Response(null, { status: 503 }),
        cacheMiss: true,
      },
      attempts,
      dataSetId: '10',
      requestCountryCode: 'US',
      timestamp: new Date().toISOString(),
      botName: undefined,
    })
    await waitOnExecutionContext(ctx)

    expect(response.headers.get('Content-Security-Policy')).toBeTruthy()
  })

  it('logs the failure with the cache miss flag and zero egress', async () => {
    const dataSetId = 'no-sp-cache-miss'
    const ctx = createExecutionContext()
    maybeHandleNoServiceProvider(env, ctx, {
      retrievalResult: {
        response: new Response(null, { status: 503 }),
        cacheMiss: true,
      },
      attempts,
      dataSetId,
      requestCountryCode: 'US',
      timestamp: new Date().toISOString(),
      botName: 'bot1',
    })
    await waitOnExecutionContext(ctx)

    const log = await env.DB.prepare(
      `SELECT response_status, egress_bytes, cache_miss_egress_bytes, cache_miss, bot_name
       FROM retrieval_logs WHERE data_set_id = ?`,
    )
      .bind(dataSetId)
      .first()

    expect(log).toEqual({
      response_status: 502,
      egress_bytes: 0,
      cache_miss_egress_bytes: 0,
      cache_miss: 1,
      bot_name: 'bot1',
    })
  })

  it('returns null and logs nothing when the retrieval succeeded', async () => {
    const dataSetId = 'sp-available'
    const ctx = createExecutionContext()
    const response = maybeHandleNoServiceProvider(env, ctx, {
      retrievalResult: {
        response: new Response(null, { status: 200 }),
        cacheMiss: false,
      },
      attempts,
      dataSetId,
      requestCountryCode: 'US',
      timestamp: new Date().toISOString(),
      botName: undefined,
    })
    await waitOnExecutionContext(ctx)

    expect(response).toBeNull()

    const log = await env.DB.prepare(
      'SELECT response_status FROM retrieval_logs WHERE data_set_id = ?',
    )
      .bind(dataSetId)
      .first()
    expect(log).toBeNull()
  })

  it('logs a null cache miss when every attempt threw', async () => {
    const dataSetId = 'no-sp-all-threw'
    const ctx = createExecutionContext()
    maybeHandleNoServiceProvider(env, ctx, {
      retrievalResult: undefined,
      attempts,
      dataSetId,
      requestCountryCode: 'US',
      timestamp: new Date().toISOString(),
      botName: undefined,
    })
    await waitOnExecutionContext(ctx)

    const log = await env.DB.prepare(
      `SELECT response_status, cache_miss, bot_name
       FROM retrieval_logs WHERE data_set_id = ?`,
    )
      .bind(dataSetId)
      .first()

    expect(log).toEqual({
      response_status: 502,
      cache_miss: null,
      bot_name: null,
    })
  })
})
