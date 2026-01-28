import { beforeEach, describe, it, expect, vi } from 'vitest'
import {
  env,
  createExecutionContext,
  createScheduledController,
} from 'cloudflare:test'
import { assertCloseToNow } from './test-helpers.js'
import workerImpl from '../bin/indexer.js'

describe('scheduled monitoring', () => {
  it('checks goldsky status and fetches subgraph data', async () => {
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
    const writeDataPoint = vi.fn()
    const testEnv = { ...env, GOLDSKY_STATS: { writeDataPoint } }
    await workerImpl.scheduled(
      createScheduledController(),
      testEnv,
      createExecutionContext(),
      { fetch: mockFetch, checkIfAddressIsSanctioned: async () => false },
    )
    expect(mockFetch).toHaveBeenCalledTimes(1)
    expect(writeDataPoint).toHaveBeenCalledWith({
      doubles: [100, 0],
    })
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

    const testEnv = { ...env, GOLDSKY_STATS: { writeDataPoint: vi.fn() } }
    await workerImpl.scheduled(
      createScheduledController(),
      testEnv,
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
