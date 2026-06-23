import { describe, it, expect } from 'vitest'
import { handleEmptyBodyResponse } from '../lib/empty-body-response.js'
import {
  env,
  createExecutionContext,
  waitOnExecutionContext,
} from 'cloudflare:test'

describe('handleEmptyBodyResponse', () => {
  it('returns the upstream status and a null body with retrieval headers', async () => {
    const ctx = createExecutionContext()
    const response = handleEmptyBodyResponse(env, ctx, {
      response: new Response(null, { status: 404 }),
      cacheMiss: true,
      dataSetId: '42',
      requestCountryCode: 'US',
      timestamp: new Date().toISOString(),
      botName: undefined,
    })
    await waitOnExecutionContext(ctx)

    expect(response.status).toBe(404)
    expect(response.body).toBeNull()
    expect(response.headers.get('X-Data-Set-ID')).toBe('42')
    expect(response.headers.get('Cache-Control')).toBe(
      `public, max-age=${env.CLIENT_CACHE_TTL}`,
    )
  })

  it('logs a zero-egress retrieval result', async () => {
    const dataSetId = 'empty-body-log'
    const ctx = createExecutionContext()
    handleEmptyBodyResponse(env, ctx, {
      response: new Response(null, { status: 200 }),
      cacheMiss: true,
      dataSetId,
      requestCountryCode: 'US',
      timestamp: new Date().toISOString(),
      botName: 'bot1',
    })
    await waitOnExecutionContext(ctx)

    const log = await env.DB.prepare(
      `SELECT response_status, egress_bytes, cache_miss_egress_bytes, cache_miss, cache_miss_response_valid, bot_name
       FROM retrieval_logs WHERE data_set_id = ?`,
    )
      .bind(dataSetId)
      .first()

    expect(log).toEqual({
      response_status: 200,
      egress_bytes: 0,
      cache_miss_egress_bytes: 0,
      cache_miss: 1,
      cache_miss_response_valid: null,
      bot_name: 'bot1',
    })
  })
})
