import { describe, it, expect } from 'vitest'
import { selectRetrievalCandidate } from '../lib/candidate-selection.js'
import {
  env,
  createExecutionContext,
  waitOnExecutionContext,
} from 'cloudflare:test'

const ok = () => ({
  response: new Response('ok', { status: 200 }),
  cacheMiss: true,
})
const notFound = () => ({
  response: new Response('nope', { status: 404 }),
  cacheMiss: false,
})
const serverError = () => ({
  response: new Response(null, { status: 503 }),
  cacheMiss: true,
})

function candidate(overrides = {}) {
  return {
    serviceUrl: 'https://a.example/',
    serviceProviderId: '1',
    dataSetId: '10',
    ...overrides,
  }
}

function context(ctx) {
  return {
    env,
    ctx,
    requestCountryCode: 'US',
    timestamp: new Date().toISOString(),
    botName: undefined,
  }
}

describe('selectRetrievalCandidate', () => {
  it('throws when the candidate list is empty', async () => {
    const ctx = createExecutionContext()
    await expect(
      selectRetrievalCandidate([], async () => ok(), context(ctx)),
    ).rejects.toThrow('should never happen')
  })

  it('returns the first candidate that responds OK without retrying', async () => {
    const ctx = createExecutionContext()
    let calls = 0
    const selection = await selectRetrievalCandidate(
      [
        candidate({ serviceUrl: 'https://a.example/' }),
        candidate({ serviceUrl: 'https://b.example/' }),
      ],
      async () => {
        calls++
        return ok()
      },
      context(ctx),
    )

    expect(calls).toBe(1)
    expect(selection.failureResponse).toBeUndefined()
    expect(selection.candidate).toBeDefined()
    expect(selection.result?.response.ok).toBe(true)
  })

  it('retries after a 5xx response until one succeeds', async () => {
    const ctx = createExecutionContext()
    let calls = 0
    const selection = await selectRetrievalCandidate(
      [
        candidate({ serviceUrl: 'https://a.example/' }),
        candidate({ serviceUrl: 'https://b.example/' }),
        candidate({ serviceUrl: 'https://c.example/' }),
      ],
      async () => {
        calls++
        return calls < 2 ? serverError() : ok()
      },
      context(ctx),
    )

    expect(calls).toBe(2)
    expect(selection.failureResponse).toBeUndefined()
    expect(selection.result?.response.ok).toBe(true)
  })

  it('retries after a thrown error until one succeeds', async () => {
    const ctx = createExecutionContext()
    let calls = 0
    const selection = await selectRetrievalCandidate(
      [
        candidate({ serviceUrl: 'https://a.example/' }),
        candidate({ serviceUrl: 'https://b.example/' }),
      ],
      async () => {
        calls++
        if (calls === 1) throw new Error('boom')
        return ok()
      },
      context(ctx),
    )

    expect(calls).toBe(2)
    expect(selection.result?.response.ok).toBe(true)
  })

  it('treats a 4xx response as a success', async () => {
    const ctx = createExecutionContext()
    const selection = await selectRetrievalCandidate(
      [candidate()],
      async () => notFound(),
      context(ctx),
    )

    expect(selection.failureResponse).toBeUndefined()
    expect(selection.result?.response.status).toBe(404)
  })

  it('returns a 502 failure response when every candidate returns a 5xx', async () => {
    const ctx = createExecutionContext()
    const selection = await selectRetrievalCandidate(
      [
        candidate({
          serviceProviderId: '1',
          serviceUrl: 'https://a.example/',
          dataSetId: '10',
        }),
      ],
      async () => serverError(),
      context(ctx),
    )
    await waitOnExecutionContext(ctx)

    expect(selection.candidate).toBeUndefined()
    expect(selection.result).toBeUndefined()
    expect(selection.failureResponse?.status).toBe(502)
    expect(selection.failureResponse?.headers.get('X-Data-Set-ID')).toBe('10')
    expect(await selection.failureResponse?.text()).toBe(
      'No available service provider found. Attempted: ID=1 (Service URL=https://a.example/)',
    )
  })

  it('lists every attempted provider in the failure response', async () => {
    const ctx = createExecutionContext()
    const selection = await selectRetrievalCandidate(
      [
        candidate({
          serviceProviderId: '1',
          serviceUrl: 'https://a.example/',
          dataSetId: '10',
        }),
        candidate({
          serviceProviderId: '2',
          serviceUrl: 'https://b.example/',
          dataSetId: '11',
        }),
      ],
      async () => serverError(),
      context(ctx),
    )
    await waitOnExecutionContext(ctx)

    const body = await selection.failureResponse?.text()
    expect(body).toContain('ID=1 (Service URL=https://a.example/)')
    expect(body).toContain('ID=2 (Service URL=https://b.example/)')
    const dataSetIds = selection.failureResponse?.headers.get('X-Data-Set-ID')
    expect(dataSetIds?.split(',').sort()).toEqual(['10', '11'])
  })

  it('sets a content security policy on the failure response', async () => {
    const ctx = createExecutionContext()
    const selection = await selectRetrievalCandidate(
      [candidate()],
      async () => serverError(),
      context(ctx),
    )
    await waitOnExecutionContext(ctx)

    expect(
      selection.failureResponse?.headers.get('Content-Security-Policy'),
    ).toBeTruthy()
  })

  it('logs the failure with the cache miss flag and zero egress', async () => {
    const dataSetId = 'no-sp-cache-miss'
    const ctx = createExecutionContext()
    await selectRetrievalCandidate(
      [candidate({ dataSetId })],
      async () => serverError(),
      {
        env,
        ctx,
        requestCountryCode: 'US',
        timestamp: new Date().toISOString(),
        botName: 'bot1',
      },
    )
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

  it('logs a null cache miss when every attempt threw', async () => {
    const dataSetId = 'no-sp-all-threw'
    const ctx = createExecutionContext()
    await selectRetrievalCandidate(
      [candidate({ dataSetId })],
      async () => {
        throw new Error('boom')
      },
      context(ctx),
    )
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

  it('does not mutate the input candidate list', async () => {
    const ctx = createExecutionContext()
    const candidates = [
      candidate({ serviceUrl: 'https://a.example/' }),
      candidate({ serviceUrl: 'https://b.example/' }),
    ]
    const snapshot = [...candidates]

    await selectRetrievalCandidate(candidates, async () => ok(), context(ctx))

    expect(candidates).toEqual(snapshot)
  })
})
