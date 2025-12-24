import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  env,
  createExecutionContext,
  waitOnExecutionContext,
} from 'cloudflare:test'
import worker from '../bin/x402-piece-gateway.js'
import {
  createMockUseFacilitator,
  createTestPaymentHeader,
  TEST_CID,
  TEST_PAYEE,
  TEST_PRICE,
  withX402Piece,
  withWalletDetails,
  createRequest,
} from './test-helpers.js'

describe('x402-piece-retriever', () => {
  beforeEach(async () => {
    await env.DB.batch([
      env.DB.prepare('DELETE FROM pieces'),
      env.DB.prepare('DELETE FROM data_sets'),
      env.DB.prepare('DELETE FROM wallet_details'),
    ])
    vi.clearAllMocks()
  })

  describe('free content (no x402 metadata)', () => {
    it('proxies requests to piece-retriever when no metadata exists', async () => {
      const ctx = createExecutionContext()
      const req = createRequest(env, TEST_PAYEE, TEST_CID)

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

      // Verify the request was forwarded to piece-retriever
      expect(env.PIECE_RETRIEVER.fetch).toHaveBeenCalledTimes(1)
      expect(env.PIECE_RETRIEVER.fetch).toHaveBeenCalledWith(req)
    })
  })

  describe('paid content (x402 metadata exists)', () => {
    beforeEach(async () => {
      await withX402Piece(env)
    })

    it('returns 402 when no X-PAYMENT header is provided', async () => {
      const ctx = createExecutionContext()
      const req = createRequest(env, TEST_PAYEE, TEST_CID)

      const res = await worker.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(402)
      const body = await res.json()
      expect(body).toStrictEqual({
        x402Version: 1,
        error: 'Missing X-PAYMENT header',
        accepts: [
          {
            scheme: 'exact',
            network: 'base-sepolia',
            maxAmountRequired: TEST_PRICE,
            payTo: TEST_PAYEE,
            resource: `https://${TEST_PAYEE.toLowerCase()}${env.DNS_ROOT}/${TEST_CID}`,
            description: '',
            mimeType: '',
            maxTimeoutSeconds: 300,
            asset: env.TOKEN.ADDRESS,
            extra: {
              name: env.TOKEN.NAME,
              version: env.TOKEN.VERSION,
            },
          },
        ],
      })
    })

    it('returns 402 with HTML for browser requests', async () => {
      const ctx = createExecutionContext()
      const req = createRequest(env, TEST_PAYEE, TEST_CID, {
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
      const req = createRequest(env, TEST_PAYEE, TEST_CID, {
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
      const req = createRequest(env, TEST_PAYEE, TEST_CID, {
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
      const req = createRequest(env, TEST_PAYEE, TEST_CID, { method: 'POST' })

      const res = await worker.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(405)
      expect(await res.text()).toBe('Method Not Allowed')
    })

    it('returns 400 for invalid payee address', async () => {
      const ctx = createExecutionContext()
      const req = createRequest(env, 'invalid-address', TEST_CID)

      const res = await worker.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(400)
      expect(await res.text()).toContain('Invalid payee address')
    })

    it('returns 404 for invalid piece CID', async () => {
      const ctx = createExecutionContext()
      const req = createRequest(env, TEST_PAYEE, 'invalid-cid')

      const res = await worker.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(404)
    })

    it('allows HEAD requests', async () => {
      const ctx = createExecutionContext()
      const req = createRequest(env, TEST_PAYEE, TEST_CID, { method: 'HEAD' })

      // Mock piece-retriever response
      env.PIECE_RETRIEVER = {
        fetch: vi.fn().mockResolvedValue(new Response(null, { status: 200 })),
      }

      const res = await worker.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(200)
    })
  })

  describe('sanctioned wallet', () => {
    it('returns 403 when payee wallet is sanctioned', async () => {
      await withX402Piece(env)
      await withWalletDetails(env, TEST_PAYEE, true)

      const ctx = createExecutionContext()
      const req = createRequest(env, TEST_PAYEE, TEST_CID)

      const res = await worker.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(403)
      const body = await res.text()
      expect(body).toContain('sanctioned')
      expect(body).toContain(TEST_PAYEE.toLowerCase())
    })

    it('returns 403 for sanctioned wallet even with valid payment header', async () => {
      await withX402Piece(env)
      await withWalletDetails(env, TEST_PAYEE, true)

      const ctx = createExecutionContext()
      const paymentHeader = createTestPaymentHeader()
      const req = createRequest(env, TEST_PAYEE, TEST_CID, {
        headers: { 'X-PAYMENT': paymentHeader },
      })

      const res = await worker.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(403)
      expect(await res.text()).toContain('sanctioned')
    })

    it('allows retrieval when wallet is not sanctioned', async () => {
      await withX402Piece(env)
      await withWalletDetails(env, TEST_PAYEE, false)

      const ctx = createExecutionContext()
      const req = createRequest(env, TEST_PAYEE, TEST_CID)

      const res = await worker.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)

      // Should return 402 (payment required), not 403
      expect(res.status).toBe(402)
    })

    it('allows retrieval when wallet has no screening record', async () => {
      await withX402Piece(env)
      // No wallet_details entry for TEST_PAYEE

      const ctx = createExecutionContext()
      const req = createRequest(env, TEST_PAYEE, TEST_CID)

      const res = await worker.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)

      // Should return 402 (payment required), not 403
      expect(res.status).toBe(402)
    })
  })

  describe('payment verification and settlement', () => {
    beforeEach(async () => {
      await withX402Piece(env)
    })

    it('returns 402 when payment cannot be decoded', async () => {
      const ctx = createExecutionContext()
      const req = createRequest(env, TEST_PAYEE, TEST_CID, {
        headers: { 'X-PAYMENT': 'not-valid-base64-payment!!!' },
      })

      const mockUseFacilitator = createMockUseFacilitator()

      const res = await worker.fetch(req, env, ctx, {
        useFacilitator: mockUseFacilitator,
      })
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(402)
      const body = await res.json()
      expect(body).toStrictEqual({
        x402Version: 1,
        error: expect.any(String),
        accepts: expect.any(Object),
      })
    })

    it('verifies payment, proxies request, and settles on success', async () => {
      const ctx = createExecutionContext()
      const paymentHeader = createTestPaymentHeader()

      env.PIECE_RETRIEVER = {
        fetch: vi.fn().mockResolvedValue(
          new Response('piece content', {
            status: 200,
            headers: { 'Content-Type': 'application/octet-stream' },
          }),
        ),
      }

      const mockUseFacilitator = createMockUseFacilitator()

      const req = createRequest(env, TEST_PAYEE, TEST_CID, {
        headers: { 'X-PAYMENT': paymentHeader },
      })

      const res = await worker.fetch(req, env, ctx, {
        useFacilitator: mockUseFacilitator,
      })
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(200)
      expect(await res.text()).toBe('piece content')
      expect(env.PIECE_RETRIEVER.fetch).toHaveBeenCalledTimes(1)
    })

    it('returns 402 when payment verification fails', async () => {
      const ctx = createExecutionContext()
      const paymentHeader = createTestPaymentHeader()

      const mockUseFacilitator = createMockUseFacilitator({
        verifyResult: { isValid: false, invalidReason: 'Insufficient funds' },
      })

      const req = createRequest(env, TEST_PAYEE, TEST_CID, {
        headers: { 'X-PAYMENT': paymentHeader },
      })

      const res = await worker.fetch(req, env, ctx, {
        useFacilitator: mockUseFacilitator,
      })
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(402)
    })

    it('returns content with X-PAYMENT-RESPONSE header when settlement succeeds', async () => {
      const ctx = createExecutionContext()
      const paymentHeader = createTestPaymentHeader()

      env.PIECE_RETRIEVER = {
        fetch: vi
          .fn()
          .mockResolvedValue(new Response('piece content', { status: 200 })),
      }

      const mockUseFacilitator = createMockUseFacilitator({
        settleResult: {
          success: true,
          transaction: '0xdef456',
          network: 'base-sepolia',
        },
      })

      const req = createRequest(env, TEST_PAYEE, TEST_CID, {
        headers: { 'X-PAYMENT': paymentHeader },
      })

      const res = await worker.fetch(req, env, ctx, {
        useFacilitator: mockUseFacilitator,
      })
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(200)
      expect(await res.text()).toBe('piece content')
      expect(res.headers.has('X-PAYMENT-RESPONSE')).toBe(true)

      // Decode and verify the settlement response
      const paymentResponse = res.headers.get('X-PAYMENT-RESPONSE')
      const decoded = JSON.parse(atob(paymentResponse))
      expect(decoded).toStrictEqual({
        success: true,
        transaction: '0xdef456',
        network: 'base-sepolia',
      })
    })

    it('returns 402 when settlement fails', async () => {
      const ctx = createExecutionContext()
      const paymentHeader = createTestPaymentHeader()

      env.PIECE_RETRIEVER = {
        fetch: vi
          .fn()
          .mockResolvedValue(new Response('piece content', { status: 200 })),
      }

      const mockUseFacilitator = createMockUseFacilitator({
        settleResult: {
          success: false,
          errorReason: 'Settlement failed',
        },
      })

      const req = createRequest(env, TEST_PAYEE, TEST_CID, {
        headers: { 'X-PAYMENT': paymentHeader },
      })

      const res = await worker.fetch(req, env, ctx, {
        useFacilitator: mockUseFacilitator,
      })
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(402)
      const body = await res.json()
      expect(body.error).toContain('Settlement failed')
    })

    it('does not attempt settlement when piece-retriever returns error status', async () => {
      const ctx = createExecutionContext()
      const paymentHeader = createTestPaymentHeader()

      env.PIECE_RETRIEVER = {
        fetch: vi
          .fn()
          .mockResolvedValue(new Response('Not Found', { status: 404 })),
      }

      let settleCalled = false
      const mockUseFacilitator = () => ({
        verify: async () => ({ isValid: true, payee: TEST_PAYEE }),
        settle: async () => {
          settleCalled = true
          return { success: true, transaction: '0x123' }
        },
      })

      const req = createRequest(env, TEST_PAYEE, TEST_CID, {
        headers: { 'X-PAYMENT': paymentHeader },
      })

      const res = await worker.fetch(req, env, ctx, {
        useFacilitator: mockUseFacilitator,
      })
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(404)
      expect(settleCalled).toBe(false)
    })

    it('returns 402 when verify function throws an error', async () => {
      const ctx = createExecutionContext()
      const paymentHeader = createTestPaymentHeader()

      const mockUseFacilitator = () => ({
        verify: async () => {
          throw new Error('Verification service unavailable')
        },
        settle: async () => ({ success: true }),
      })

      const req = createRequest(env, TEST_PAYEE, TEST_CID, {
        headers: { 'X-PAYMENT': paymentHeader },
      })

      const res = await worker.fetch(req, env, ctx, {
        useFacilitator: mockUseFacilitator,
      })
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(402)
    })

    it('returns 402 when settle function throws an error', async () => {
      const ctx = createExecutionContext()
      const paymentHeader = createTestPaymentHeader()

      env.PIECE_RETRIEVER = {
        fetch: vi
          .fn()
          .mockResolvedValue(new Response('piece content', { status: 200 })),
      }

      const mockUseFacilitator = () => ({
        verify: async () => ({ isValid: true, payee: TEST_PAYEE }),
        settle: async () => {
          throw new Error('Settlement service unavailable')
        },
      })

      const req = createRequest(env, TEST_PAYEE, TEST_CID, {
        headers: { 'X-PAYMENT': paymentHeader },
      })

      const res = await worker.fetch(req, env, ctx, {
        useFacilitator: mockUseFacilitator,
      })
      await waitOnExecutionContext(ctx)

      expect(res.status).toBe(402)
    })
  })
})
