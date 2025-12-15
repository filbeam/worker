import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  env,
  createExecutionContext,
  waitOnExecutionContext,
} from 'cloudflare:test'
import worker from '../bin/x402-piece-retriever.js'

const DNS_ROOT = '.x402.calibration.filbeam.io'
const TEST_PAYEE = '0xc83dbfdf61616778537211a7e5ca2e87ec6cf0ed'
const TEST_CID =
  'baga6ea4seaqaleibb6ud4xeemuzzpsyhl6cxlsymsnfco4cdjka5uzajo2x4ipa'
const TEST_PRICE = '1000000' // 1 USDC (6 decimals)

// Set test environment
env.DNS_ROOT = DNS_ROOT
env.NETWORK = 'base-sepolia'
env.FACILITATOR_URL = 'https://x402.org/facilitator'
env.USDC_ADDRESS = '0x036CbD53842c5426634e7929541eC2318f3dCF7e'

/** Helper to create a request with the proper URL format */
function createRequest(payeeAddress, pieceCid, options = {}) {
  const { method = 'GET', headers = {} } = options
  let url = 'https://'
  if (payeeAddress) url += payeeAddress
  url += DNS_ROOT
  if (pieceCid) url += `/${pieceCid}`
  return new Request(url, { method, headers })
}

/** Helper to add x402 metadata to KV */
async function withX402Metadata(payeeAddress, pieceCid, price, block = '1000') {
  await env.X402_METADATA_KV.put(
    `${payeeAddress.toLowerCase()}:${pieceCid}`,
    JSON.stringify({ price, block }),
  )
}

/** Helper to clear KV store */
async function clearKV() {
  let cursor
  do {
    const list = await env.X402_METADATA_KV.list({ cursor })
    for (const key of list.keys) {
      await env.X402_METADATA_KV.delete(key.name)
    }
    cursor = list.list_complete ? undefined : list.cursor
  } while (cursor)
}

describe('x402-piece-retriever', () => {
  beforeEach(async () => {
    await clearKV()
    // Reset mock
    vi.clearAllMocks()
  })

  describe('free content (no x402 metadata)', () => {
    it('proxies requests directly to piece-retriever when no metadata exists', async () => {
      const ctx = createExecutionContext()
      const req = createRequest(TEST_PAYEE, TEST_CID)

      // Mock piece-retriever response
      env.PIECE_RETRIEVER = {
        fetch: vi.fn().mockResolvedValue(
          new Response('piece content', {
            status: 200,
            headers: { 'Content-Type': 'application/octet-stream' },
          }),
        ),
      }

      const res = await worker.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(200)
      expect(await res.text()).toBe('piece content')
      expect(env.PIECE_RETRIEVER.fetch).toHaveBeenCalledWith(req)
    })
  })

  describe('paid content (x402 metadata exists)', () => {
    beforeEach(async () => {
      await withX402Metadata(TEST_PAYEE, TEST_CID, TEST_PRICE)
    })

    it('returns 402 when no X-PAYMENT header is provided', async () => {
      const ctx = createExecutionContext()
      const req = createRequest(TEST_PAYEE, TEST_CID)

      const res = await worker.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(402)
      const body = await res.json()
      expect(body.x402Version).toBe(1)
      expect(body.error).toBe('Payment Required')
      expect(body.accepts).toHaveLength(1)
      expect(body.accepts[0].scheme).toBe('exact')
      expect(body.accepts[0].network).toBe('base-sepolia')
      expect(body.accepts[0].maxAmountRequired).toBe(TEST_PRICE)
      expect(body.accepts[0].payTo).toBe(TEST_PAYEE.toLowerCase())
    })

    it('returns 402 with HTML for browser requests', async () => {
      const ctx = createExecutionContext()
      const req = createRequest(TEST_PAYEE, TEST_CID, {
        headers: {
          Accept: 'text/html,application/xhtml+xml',
          'User-Agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        },
      })

      const res = await worker.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(402)
      expect(res.headers.get('Content-Type')).toContain('text/html')
    })

    it('returns 402 with JSON for API clients', async () => {
      const ctx = createExecutionContext()
      const req = createRequest(TEST_PAYEE, TEST_CID, {
        headers: {
          Accept: 'application/json',
        },
      })

      const res = await worker.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(402)
      expect(res.headers.get('Content-Type')).toContain('application/json')
    })

    it('returns 402 for invalid payment header', async () => {
      const ctx = createExecutionContext()
      const req = createRequest(TEST_PAYEE, TEST_CID, {
        headers: {
          'X-PAYMENT': 'invalid-base64!!!',
        },
      })

      const res = await worker.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)

      // Invalid payment header is treated as no payment
      expect(res.status).toBe(402)
    })
  })

  describe('request validation', () => {
    it('returns 405 for unsupported methods', async () => {
      const ctx = createExecutionContext()
      const req = createRequest(TEST_PAYEE, TEST_CID, { method: 'POST' })

      const res = await worker.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(405)
      expect(await res.text()).toBe('Method Not Allowed')
    })

    it('returns 400 for invalid payee address', async () => {
      const ctx = createExecutionContext()
      const req = createRequest('invalid-address', TEST_CID)

      const res = await worker.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(400)
      expect(await res.text()).toContain('Invalid payee address')
    })

    it('returns 404 for invalid piece CID', async () => {
      const ctx = createExecutionContext()
      const req = createRequest(TEST_PAYEE, 'invalid-cid')

      const res = await worker.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(404)
    })

    it('allows HEAD requests', async () => {
      const ctx = createExecutionContext()
      const req = createRequest(TEST_PAYEE, TEST_CID, { method: 'HEAD' })

      // Mock piece-retriever response
      env.PIECE_RETRIEVER = {
        fetch: vi.fn().mockResolvedValue(new Response(null, { status: 200 })),
      }

      const res = await worker.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(200)
    })
  })
})
