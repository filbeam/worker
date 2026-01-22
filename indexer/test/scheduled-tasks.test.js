import { beforeEach, describe, it, expect, vi, afterEach } from 'vitest'
import {
  env,
  createExecutionContext,
  createScheduledController,
} from 'cloudflare:test'
import { assertCloseToNow } from './test-helpers.js'
import workerImpl from '../bin/indexer.js'

describe('scheduled monitoring', () => {
  afterEach(() => {
    vi.useRealTimers()
  })

  it('passes when everything is healthy', async () => {
    const mockFetch = vi.fn()
    mockFetch.mockImplementationOnce((url, opts) => {
      expect(url).toMatch('goldsky')
      expect(opts.method).toBe('POST')
      expect(opts.body).toMatch('hasIndexingErrors')
      expect(opts.body).toMatch('number')
      return new Response(
        JSON.stringify({
          data: {
            _meta: {
              hasIndexingErrors: false,
              block: {
                number: 100,
              },
            },
          },
        }),
      )
    })
    await workerImpl.scheduled(
      createScheduledController(),
      env,
      createExecutionContext(),
      { fetch: mockFetch, checkIfAddressIsSanctioned: async () => false },
    )
    expect(mockFetch).toHaveBeenCalledTimes(1)
  })
  it('fails when there is a goldsky indexing issue', async () => {
    const mockFetch = vi.fn()
    mockFetch.mockImplementationOnce((url, opts) => {
      expect(url).toMatch('goldsky')
      return new Response(
        JSON.stringify({
          data: {
            _meta: {
              hasIndexingErrors: true,
              block: {
                number: 100,
              },
            },
          },
        }),
      )
    })
    await expect(
      workerImpl.scheduled(
        createScheduledController(),
        env,
        createExecutionContext(),
        {
          fetch: mockFetch,
          checkIfAddressIsSanctioned: async () => false,
        },
      ),
    ).rejects.toThrow('Goldsky has indexing errors')
    expect(mockFetch).toHaveBeenCalledTimes(1)
  })

  it('retries on 502 error from Goldsky and eventually fails', async () => {
    vi.useFakeTimers()
    const mockFetch = vi.fn()
    // Mock consecutive failures
    mockFetch.mockImplementation((url, opts) => {
      expect(url).toMatch('goldsky')
      return new Response('error code: 502', {
        status: 502,
        statusText: 'Bad Gateway',
      })
    })

    const promise = workerImpl.scheduled(
      createScheduledController(),
      env,
      createExecutionContext(),
      {
        fetch: mockFetch,
        checkIfAddressIsSanctioned: async () => false,
      },
    )

    // Fast-forward through all retries
    // With 10 retries and exponential backoff (1s, 2s, 4s, 8s, 16s, 32s, 64s, 128s, 256s, 512s)
    // Total time would be 1023 seconds, so we advance more than that
    await vi.advanceTimersByTimeAsync(1100000)

    await expect(promise).rejects.toThrow(
      'Cannot fetch  (502): error code: 502',
    )
    // Should have been called 11 times (initial + 10 retries)
    expect(mockFetch).toHaveBeenCalledTimes(11)
  })

  it('retries on 503 error and succeeds on retry', async () => {
    const mockFetch = vi.fn()
    // First call fails with 503, second call succeeds
    mockFetch
      .mockImplementationOnce((url, opts) => {
        expect(url).toMatch('goldsky')
        return new Response('Service Unavailable', {
          status: 503,
          statusText: 'Service Unavailable',
        })
      })
      .mockImplementationOnce((url, opts) => {
        expect(url).toMatch('goldsky')
        return new Response(
          JSON.stringify({
            data: {
              _meta: {
                hasIndexingErrors: false,
                block: {
                  number: 100,
                },
              },
            },
          }),
        )
      })
    await workerImpl.scheduled(
      createScheduledController(),
      env,
      createExecutionContext(),
      {
        fetch: mockFetch,
        checkIfAddressIsSanctioned: async () => false,
      },
    )
    // Should have been called twice (initial failure + 1 retry success)
    expect(mockFetch).toHaveBeenCalledTimes(2)
  })

  it('does not retry on 4xx errors', async () => {
    const mockFetch = vi.fn()
    mockFetch.mockImplementationOnce((url, opts) => {
      expect(url).toMatch('goldsky')
      return new Response('Bad Request', {
        status: 400,
        statusText: 'Bad Request',
      })
    })
    await expect(
      workerImpl.scheduled(
        createScheduledController(),
        env,
        createExecutionContext(),
        {
          fetch: mockFetch,
          checkIfAddressIsSanctioned: async () => false,
        },
      ),
    ).rejects.toThrow('Cannot fetch  (400): Bad Request')
    // Should only be called once (no retries for 4xx)
    expect(mockFetch).toHaveBeenCalledTimes(1)
  })
})

describe('scheduled wallet screening', () => {
  beforeEach(async () => {
    // Clear the database before each test
    await env.DB.exec('DELETE FROM wallet_details')
  })

  it('screens wallets for sanctions', async () => {
    const TEST_WALLET = '0xabcd001'
    await env.DB.prepare(
      `
        INSERT INTO wallet_details (address, is_sanctioned, last_screened_at)
        VALUES (?, 0, NULL)
      `,
    )
      .bind(TEST_WALLET)
      .run()

    await workerImpl.scheduled(
      createScheduledController(),
      env,
      createExecutionContext(),
      {
        fetch: async (url, opts) => {
          if (url.startsWith('https://api.goldsky.com')) {
            return new Response(
              JSON.stringify({
                data: {
                  _meta: {
                    hasIndexingErrors: false,
                    block: { number: 123 },
                  },
                },
              }),
            )
          }
          throw new Error(`Unexpected URL in fetch: ${url}`)
        },
        checkIfAddressIsSanctioned: async (address) => true,
      },
    )

    // eslint-disable-next-line camelcase
    const { last_screened_at } = await env.DB.prepare(
      `SELECT last_screened_at FROM wallet_details WHERE address = ?`,
    )
      .bind(TEST_WALLET)
      .first()
    assertCloseToNow(last_screened_at, 'last_screened_at')
  })
})
