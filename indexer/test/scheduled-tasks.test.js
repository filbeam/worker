import { beforeEach, describe, it, expect, vi } from 'vitest'
import {
  env,
  createExecutionContext,
  createScheduledController,
} from 'cloudflare:test'
import { assertCloseToNow } from './test-helpers.js'
import workerImpl from '../bin/indexer.js'

describe('scheduled monitoring', () => {
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

  it('handles 502 error from Goldsky gracefully', async () => {
    const mockFetch = vi.fn()
    mockFetch.mockImplementationOnce((url, opts) => {
      expect(url).toMatch('goldsky')
      return new Response('error code: 502', {
        status: 502,
        statusText: 'Bad Gateway',
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
    ).rejects.toThrow('Goldsky API returned 502 Bad Gateway: error code: 502')
    expect(mockFetch).toHaveBeenCalledTimes(1)
  })

  it('handles 503 error from Goldsky gracefully', async () => {
    const mockFetch = vi.fn()
    mockFetch.mockImplementationOnce((url, opts) => {
      expect(url).toMatch('goldsky')
      return new Response('Service Unavailable', {
        status: 503,
        statusText: 'Service Unavailable',
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
    ).rejects.toThrow('Goldsky API returned 503 Service Unavailable')
    expect(mockFetch).toHaveBeenCalledTimes(1)
  })

  it('handles non-JSON response from Goldsky gracefully', async () => {
    const mockFetch = vi.fn()
    mockFetch.mockImplementationOnce((url, opts) => {
      expect(url).toMatch('goldsky')
      return new Response('Not JSON response', {
        status: 500,
        statusText: 'Internal Server Error',
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
    ).rejects.toThrow(
      'Goldsky API returned 500 Internal Server Error: Not JSON response',
    )
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
