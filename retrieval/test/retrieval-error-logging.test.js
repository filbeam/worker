import { describe, it, expect } from 'vitest'
import { withRetrievalErrorLogging } from '../lib/retrieval-error-logging.js'
import {
  env,
  createExecutionContext,
  waitOnExecutionContext,
} from 'cloudflare:test'

const request = new Request('https://example.com/', {
  headers: { 'CF-IPCountry': 'US' },
})

describe('withRetrievalErrorLogging', () => {
  it('passes the per-request telemetry context to the handler', async () => {
    const ctx = createExecutionContext()
    let received
    const result = await withRetrievalErrorLogging(
      request,
      env,
      ctx,
      { botName: undefined },
      async (context) => {
        received = context
        return 'ok'
      },
    )
    await waitOnExecutionContext(ctx)

    expect(result).toBe('ok')
    expect(received).toEqual({
      requestTimestamp: expect.any(String),
      workerStartedAt: expect.any(Number),
      requestCountryCode: 'US',
    })
  })

  it('logs a retrieval error and rethrows when the handler throws', async () => {
    const ctx = createExecutionContext()
    const error = Object.assign(new Error('Not Found'), { status: 404 })

    await expect(
      withRetrievalErrorLogging(
        request,
        env,
        ctx,
        { botName: 'bot1' },
        async () => {
          throw error
        },
      ),
    ).rejects.toBe(error)
    await waitOnExecutionContext(ctx)

    const log = await env.DB.prepare(
      `SELECT response_status, egress_bytes, data_set_id, request_country_code, bot_name
       FROM retrieval_logs WHERE bot_name = ? AND response_status = 404`,
    )
      .bind('bot1')
      .first()

    expect(log).toEqual({
      response_status: 404,
      egress_bytes: null,
      data_set_id: null,
      request_country_code: 'US',
      bot_name: 'bot1',
    })
  })
})
