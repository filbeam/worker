import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { env } from 'cloudflare:test'
import workerImpl from '../bin/indexer.js'
import { epochToTimestampMs } from '../lib/epoch.js'

describe('checkGoldskyStatus', () => {
  let writeDataPoint
  let testEnv

  beforeEach(() => {
    writeDataPoint = vi.fn()
    testEnv = {
      ...env,
      GOLDSKY_STATS: { writeDataPoint },
    }
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('writes data point when subgraph returns valid response', async () => {
    const mockFetch = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          data: {
            _meta: {
              hasIndexingErrors: false,
              block: { number: 12345 },
            },
          },
        }),
      ),
    )

    await workerImpl.checkGoldskyStatus(testEnv, { fetch: mockFetch })

    expect(writeDataPoint).toHaveBeenCalledWith({
      doubles: [12345, 0, expect.any(Number)],
    })
  })

  it('writes hasIndexingErrors=1 when subgraph has errors', async () => {
    const mockFetch = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          data: {
            _meta: {
              hasIndexingErrors: true,
              block: { number: 67890 },
            },
          },
        }),
      ),
    )

    await workerImpl.checkGoldskyStatus(testEnv, { fetch: mockFetch })

    expect(writeDataPoint).toHaveBeenCalledWith({
      doubles: [67890, 1, expect.any(Number)],
    })
  })

  it('warns and returns without data point when fetch fails', async () => {
    const mockFetch = vi
      .fn()
      .mockResolvedValue(new Response('Internal Server Error', { status: 500 }))
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})

    await workerImpl.checkGoldskyStatus(testEnv, { fetch: mockFetch })

    expect(writeDataPoint).not.toHaveBeenCalled()
    expect(warnSpy).toHaveBeenCalledWith(
      'Goldsky returned 500: Internal Server Error',
    )
  })

  it('warns and returns without data point when fetch throws a network error', async () => {
    const mockFetch = vi.fn().mockRejectedValue(new Error('Network error'))
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})

    await workerImpl.checkGoldskyStatus(testEnv, { fetch: mockFetch })

    expect(writeDataPoint).not.toHaveBeenCalled()
    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining('Network error'),
    )
  })

  it('calculates indexer lag in milliseconds', async () => {
    const fakeNow = 1700000000000
    vi.useFakeTimers()
    vi.setSystemTime(fakeNow)

    const blockNumber = 12345
    const genesisTimestampMs = Number(env.FILECOIN_GENESIS_BLOCK_TIMESTAMP_MS)
    const expectedLagMs =
      fakeNow - epochToTimestampMs(blockNumber, genesisTimestampMs)

    const mockFetch = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          data: {
            _meta: {
              hasIndexingErrors: false,
              block: { number: blockNumber },
            },
          },
        }),
      ),
    )

    await workerImpl.checkGoldskyStatus(testEnv, { fetch: mockFetch })

    expect(writeDataPoint).toHaveBeenCalledWith({
      doubles: [blockNumber, 0, expectedLagMs],
    })

    vi.useRealTimers()
  })

  it('warns and returns without data point when response is malformed', async () => {
    const mockFetch = vi
      .fn()
      .mockResolvedValue(
        new Response(JSON.stringify({ data: { unexpected: 'structure' } })),
      )
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})

    await workerImpl.checkGoldskyStatus(testEnv, { fetch: mockFetch })

    expect(writeDataPoint).not.toHaveBeenCalled()
    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining('Unexpected Goldsky response'),
    )
  })
})
