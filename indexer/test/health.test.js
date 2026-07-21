import { describe, it, expect, vi, beforeEach } from 'vitest'
import { env } from 'cloudflare:test'
import workerImpl from '../bin/indexer.js'
import {
  checkDeliveryHealth,
  DEFAULT_DELIVERY_LAG_BUFFER_SECONDS,
} from '../lib/health.js'
import { randomId } from './test-helpers.js'

env.SECRET_HEADER_KEY = 'secret-header-key'
env.SECRET_HEADER_VALUE = 'secret-header-value'

/** @param {{ dataSetId: string; pieceId: string; blockTimestamp: string }[]} pieceAddeds */
function mockSubgraphFetch(pieceAddeds) {
  return vi
    .fn()
    .mockResolvedValue(new Response(JSON.stringify({ data: { pieceAddeds } })))
}

/**
 * @param {object} [options]
 * @param {string} [options.dataSetId]
 * @param {string} [options.pieceId]
 * @param {string | null} [options.cid]
 * @param {boolean} [options.isDeleted]
 */
async function givenPiece({
  dataSetId = randomId(),
  pieceId = randomId(),
  cid = 'bafkqaaa',
  isDeleted = false,
} = {}) {
  await env.DB.prepare(
    `INSERT INTO pieces (id, data_set_id, cid, is_deleted)
     VALUES (?, ?, ?, ?)`,
  )
    .bind(pieceId, dataSetId, cid, isDeleted)
    .run()
  return { dataSetId, pieceId }
}

/** @param {object} [overrides] */
function onchainPiece(overrides = {}) {
  return {
    dataSetId: randomId(),
    pieceId: randomId(),
    blockTimestamp: String(
      Math.floor(Date.now() / 1000) - 2 * DEFAULT_DELIVERY_LAG_BUFFER_SECONDS,
    ),
    ...overrides,
  }
}

describe('checkDeliveryHealth', () => {
  beforeEach(async () => {
    await env.DB.exec('DELETE FROM pieces')
  })

  it('returns healthy when the newest eligible piece is in D1', async () => {
    const { dataSetId, pieceId } = await givenPiece()
    const piece = onchainPiece({ dataSetId, pieceId })

    const result = await checkDeliveryHealth(env, {
      fetch: mockSubgraphFetch([piece]),
    })

    expect(result).toEqual({ status: 'healthy', checked: piece })
  })

  it('returns healthy for pieces that were delivered and later removed', async () => {
    const { dataSetId, pieceId } = await givenPiece({ isDeleted: true })
    const piece = onchainPiece({ dataSetId, pieceId })

    const result = await checkDeliveryHealth(env, {
      fetch: mockSubgraphFetch([piece]),
    })

    expect(result).toEqual({ status: 'healthy', checked: piece })
  })

  it('returns delivery_lagging for a removal-only tombstone', async () => {
    const { dataSetId, pieceId } = await givenPiece({
      cid: null,
      isDeleted: true,
    })
    const piece = onchainPiece({ dataSetId, pieceId })

    const result = await checkDeliveryHealth(env, {
      fetch: mockSubgraphFetch([piece]),
    })

    expect(result).toEqual({ status: 'delivery_lagging', checked: piece })
  })

  it('returns delivery_lagging when the newest eligible piece is missing from D1', async () => {
    const piece = onchainPiece()

    const result = await checkDeliveryHealth(env, {
      fetch: mockSubgraphFetch([piece]),
    })

    expect(result).toEqual({ status: 'delivery_lagging', checked: piece })
  })

  it('returns idle when there are no eligible on-chain events', async () => {
    const result = await checkDeliveryHealth(env, {
      fetch: mockSubgraphFetch([]),
    })

    expect(result).toEqual({ status: 'idle' })
  })

  it('queries only events older than the buffer period', async () => {
    const fetch = mockSubgraphFetch([])
    const now = 1_800_000_000_000

    await checkDeliveryHealth(
      { ...env, DELIVERY_LAG_BUFFER_SECONDS: '600' },
      { fetch, now },
    )

    const body = JSON.parse(fetch.mock.calls[0][1].body)
    expect(body.query).toContain(`blockTimestamp_lte: "${1_800_000_000 - 600}"`)
  })

  it('falls back to the default buffer when the configured value is invalid', async () => {
    const fetch = mockSubgraphFetch([])
    const now = 1_800_000_000_000

    await checkDeliveryHealth(
      { ...env, DELIVERY_LAG_BUFFER_SECONDS: 'not-a-number' },
      { fetch, now },
    )

    const body = JSON.parse(fetch.mock.calls[0][1].body)
    expect(body.query).toContain(
      `blockTimestamp_lte: "${1_800_000_000 - DEFAULT_DELIVERY_LAG_BUFFER_SECONDS}"`,
    )
  })

  for (const [name, fetchImpl] of [
    [
      'subgraph returns a non-2xx response',
      vi.fn().mockResolvedValue(new Response('oops', { status: 502 })),
    ],
    [
      'subgraph request fails',
      vi.fn().mockRejectedValue(new Error('network down')),
    ],
    [
      'subgraph returns invalid JSON',
      vi.fn().mockResolvedValue(new Response('<html>gateway error</html>')),
    ],
    [
      'subgraph returns GraphQL errors',
      vi
        .fn()
        .mockResolvedValue(
          new Response(
            JSON.stringify({ errors: [{ message: 'query failed' }] }),
          ),
        ),
    ],
    [
      'subgraph response has an unexpected shape',
      vi
        .fn()
        .mockResolvedValue(
          new Response(JSON.stringify({ data: { pieceAddeds: null } })),
        ),
    ],
  ]) {
    it(`returns subgraph_unknown when the ${name}`, async () => {
      const result = await checkDeliveryHealth(env, { fetch: fetchImpl })
      expect(result).toEqual({ status: 'subgraph_unknown' })
    })
  }

  it('returns d1_unhealthy when the D1 query fails', async () => {
    const brokenDb = {
      prepare: () => ({
        bind: () => ({
          first: () => Promise.reject(new Error('D1 boom')),
        }),
      }),
    }

    const result = await checkDeliveryHealth(
      { ...env, DB: brokenDb },
      { fetch: mockSubgraphFetch([onchainPiece()]) },
    )

    expect(result).toEqual({ status: 'd1_unhealthy' })
  })
})

describe('GET /health', () => {
  beforeEach(async () => {
    await env.DB.exec('DELETE FROM pieces')
  })

  it('is accessible without the webhook secret', async () => {
    const { dataSetId, pieceId } = await givenPiece()
    const req = new Request('https://host/health', { method: 'GET' })

    const res = await workerImpl.fetch(req, env, undefined, {
      fetch: mockSubgraphFetch([onchainPiece({ dataSetId, pieceId })]),
    })

    expect(res.status).toBe(200)
    expect(await res.json()).toMatchObject({ status: 'healthy' })
  })

  it('returns 503 when delivery is lagging', async () => {
    const req = new Request('https://host/health', { method: 'GET' })

    const res = await workerImpl.fetch(req, env, undefined, {
      fetch: mockSubgraphFetch([onchainPiece()]),
    })

    expect(res.status).toBe(503)
    expect(await res.json()).toMatchObject({ status: 'delivery_lagging' })
  })

  it('does not bypass authentication for other requests', async () => {
    for (const req of [
      new Request('https://host/health', { method: 'POST' }),
      new Request('https://host/fwss/piece-added', { method: 'POST' }),
      new Request('https://host/other', { method: 'GET' }),
    ]) {
      const res = await workerImpl.fetch(req, env)
      expect(res.status).toBe(401)
    }
  })
})
