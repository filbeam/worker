import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { fetchAndStoreBadBits } from '../lib/bad-bits.js'
import { getAllBadBitHashes } from './util.js'
import { env } from 'cloudflare:test'
import { testData, testDataHashes } from './testData.js'

describe('fetchAndStoreBadBits', () => {
  beforeEach(async () => {
    // Clear the database before each test to avoid interference
    const { keys } = await env.KV.list()
    for (const key of keys) {
      if (key.startsWith('bad-bits:')) {
        await env.KV.delete(key)
      }
    }

    // tell vitest we use mocked time
    vi.useFakeTimers()
  })

  afterEach(() => {
    // restoring date after each test run
    vi.useRealTimers()
  })

  it('fetches and stores bad bits from the denylist', async () => {
    const text = testData

    await fetchAndStoreBadBits(env, {
      fetch: async () =>
        new Response(text, { headers: { 'Content-Type': 'text/plain' } }),
    })
    const storedHashes = new Set(await getAllBadBitHashes(env))

    // Verify the database contains the expected hashes
    expect(storedHashes).toEqual(testDataHashes)
  })

  it('removes hashes not in the current denylist', async () => {
    // Insert some initial hashes into the database
    const initialHashes = ['hash1', 'hash2', 'hash3']
    await initialHashes.map((hash) => env.KV.put(`bad-bits:${hash}`, 'true'))
    await env.KV.put(`bad-bits:_latest-hashes:0`, initialHashes.join(','))

    const text = testData

    await fetchAndStoreBadBits(env, {
      fetch: () =>
        new Response(text, { headers: { 'Content-Type': 'text/plain' } }),
    })
    const storedHashes = new Set(await getAllBadBitHashes(env))

    // Verify the database contains only the current hashes
    expect(storedHashes).toEqual(testDataHashes)
  })

  it('retries on 5xx server errors and eventually succeeds', async () => {
    vi.useRealTimers()
    const text = testData

    let fetchCallCount = 0
    await fetchAndStoreBadBits(env, {
      fetch: () => {
        console.log(`Fetch attempt #${fetchCallCount + 1}`)
        fetchCallCount++
        // Return 500 error for first 2 attempts, then succeed
        if (fetchCallCount <= 2) {
          return new Response('Test Server Error', { status: 500 })
        }
        return new Response(text, {
          headers: { 'Content-Type': 'text/plain' },
        })
      },
    })

    // Verify that fetch was called multiple times due to retries
    expect(fetchCallCount).toBe(3)

    const storedHashes = new Set(await getAllBadBitHashes(env))
    // Verify the database contains the expected hashes after successful retry
    expect(storedHashes).toEqual(testDataHashes)
  })

  it('uses ETag to detect when the bad bit list was not changed since the last check', async () => {
    const etag = 'bafybeig123'
    const lastUpdateAt = new Date('2020-01-01T00:00:00Z')
    vi.setSystemTime(lastUpdateAt)

    /** @type {Request} */
    let request
    await fetchAndStoreBadBits(env, {
      fetch: (req) => {
        request = req
        return new Response(testData, {
          headers: { 'Content-Type': 'text/plain', etag },
        })
      },
    })

    expect(
      request.headers?.get('If-None-Match'),
      'If-None-Match in the first request',
    ).toBe(null)

    const now = new Date('2025-07-01T00:00:00Z')
    vi.setSystemTime(now)

    await fetchAndStoreBadBits(env, {
      fetch: (req) => {
        request = req
        return new Response(null, { status: 304 /* Not Modified */ })
      },
    })

    expect(
      request.headers?.get('If-None-Match'),
      'If-None-Match in the second request',
    ).toBe(etag)
  })

  it(
    'successfully fetched bad bits from real denylist',
    { timeout: 20000 },
    async () => {
      vi.useRealTimers()

      const bits = await (
        await fetch('https://badbits.dwebops.pub/badbits.deny')
      ).text()

      await fetchAndStoreBadBits(env, {
        fetch: () =>
          new Response(bits, { headers: { 'Content-Type': 'text/plain' } }),
      })

      // Wait a tick to flush finalizers and prevent Workerd crash
      await new Promise((resolve) => setTimeout(resolve, 100))

      const storedHashes = new Set(await getAllBadBitHashes(env))

      // Verify the database contains the expected hashes
      expect(storedHashes.size).toBeGreaterThan(0)

      // Choose a random hash from the real denylist file to verify it's in the database
      const isBadBit = await env.KV.get(
        'bad-bits:00003f3fdeb4f3ee55e2c601ca263b1c8eef866e7f8e018a9f8ce03e7aea4d8a',
        { type: 'json' },
      )
      expect(isBadBit).toBe(true)
    },
  )
})
