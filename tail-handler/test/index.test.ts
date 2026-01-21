import { describe, it, expect, vi, beforeEach } from 'vitest'
import { env, createExecutionContext } from 'cloudflare:test'
import worker from '../src/index.ts'

describe('tail-handler worker', () => {
  beforeEach(() => {
    vi.restoreAllMocks()
  })

  it('writes data point for each tail event', async () => {
    const events = [
      buildTraceItem({ serviceName: 'filbeam-piece-retriever-dev' }),
      buildTraceItem({ serviceName: 'filbeam-indexer' }),
    ]
    const writeDataPointSpy = vi.spyOn(env.RETRIEVAL_STATS, 'writeDataPoint')

    await worker.tail(events, env, createExecutionContext())

    expect(writeDataPointSpy).toHaveBeenCalledTimes(2)
  })

  it('extracts service name from scriptTags', async () => {
    const events = [
      buildTraceItem({ serviceName: 'filbeam-piece-retriever-dev' }),
    ]
    const writeDataPointSpy = vi.spyOn(env.RETRIEVAL_STATS, 'writeDataPoint')

    await worker.tail(events, env, createExecutionContext())

    expect(writeDataPointSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        indexes: ['filbeam-piece-retriever-dev'],
      }),
    )
  })

  it('falls back to scriptName when scriptTags is undefined', async () => {
    const events = [buildTraceItem({ scriptTags: undefined })]
    const writeDataPointSpy = vi.spyOn(env.RETRIEVAL_STATS, 'writeDataPoint')

    await worker.tail(events, env, createExecutionContext())

    expect(writeDataPointSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        indexes: ['piece-retriever-dev'],
      }),
    )
  })

  it('falls back to scriptName when no cf:service tag present', async () => {
    const events = [buildTraceItem({ scriptTags: ['cf:environment=dev'] })]
    const writeDataPointSpy = vi.spyOn(env.RETRIEVAL_STATS, 'writeDataPoint')

    await worker.tail(events, env, createExecutionContext())

    expect(writeDataPointSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        indexes: ['piece-retriever-dev'],
      }),
    )
  })

  it('uses "unknown" when scriptName is null and no cf:service tag', async () => {
    const events = [buildTraceItem({ scriptTags: undefined, scriptName: null })]
    const writeDataPointSpy = vi.spyOn(env.RETRIEVAL_STATS, 'writeDataPoint')

    await worker.tail(events, env, createExecutionContext())

    expect(writeDataPointSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        indexes: ['unknown'],
      }),
    )
  })

  it('extracts response status from fetch event', async () => {
    const events = [buildTraceItem({ responseStatus: 404 })]
    const writeDataPointSpy = vi.spyOn(env.RETRIEVAL_STATS, 'writeDataPoint')

    await worker.tail(events, env, createExecutionContext())

    expect(writeDataPointSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        doubles: [expect.any(Number), expect.any(Number), 404],
      }),
    )
  })

  it('uses status code 0 when response is missing', async () => {
    const events = [
      buildTraceItem({
        event: {
          request: { url: 'https://example.com', method: 'GET' },
        } as TraceItemFetchEventInfo,
      }),
    ]
    const writeDataPointSpy = vi.spyOn(env.RETRIEVAL_STATS, 'writeDataPoint')

    await worker.tail(events, env, createExecutionContext())

    expect(writeDataPointSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        doubles: [expect.any(Number), expect.any(Number), 0],
      }),
    )
  })

  it('uses status code 0 when event is null', async () => {
    const events = [buildTraceItem({ event: null })]
    const writeDataPointSpy = vi.spyOn(env.RETRIEVAL_STATS, 'writeDataPoint')

    await worker.tail(events, env, createExecutionContext())

    expect(writeDataPointSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        doubles: [expect.any(Number), expect.any(Number), 0],
      }),
    )
  })

  it('records outcome from event', async () => {
    const events = [buildTraceItem({ outcome: 'exception' })]
    const writeDataPointSpy = vi.spyOn(env.RETRIEVAL_STATS, 'writeDataPoint')

    await worker.tail(events, env, createExecutionContext())

    expect(writeDataPointSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        blobs: ['exception'],
      }),
    )
  })

  it('records wallTime and cpuTime from event', async () => {
    const events = [buildTraceItem({ wallTime: 100, cpuTime: 25 })]
    const writeDataPointSpy = vi.spyOn(env.RETRIEVAL_STATS, 'writeDataPoint')

    await worker.tail(events, env, createExecutionContext())

    expect(writeDataPointSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        doubles: [100, 25, expect.any(Number)],
      }),
    )
  })

  it('continues processing events when writeDataPoint throws', async () => {
    const events = [
      buildTraceItem({ serviceName: 'first-service' }),
      buildTraceItem({ serviceName: 'second-service' }),
      buildTraceItem({ serviceName: 'third-service' }),
    ]
    const writeDataPointSpy = vi
      .spyOn(env.RETRIEVAL_STATS, 'writeDataPoint')
      .mockImplementationOnce(() => {
        throw new Error('Simulated writeDataPoint failure')
      })
    const consoleErrorSpy = vi
      .spyOn(console, 'error')
      .mockImplementation(() => {})

    await worker.tail(events, env, createExecutionContext())

    expect(writeDataPointSpy).toHaveBeenCalledTimes(3)
    expect(consoleErrorSpy).toHaveBeenCalledWith(
      'Failed to write data point for event',
      expect.any(Error),
    )
  })
})

interface TraceItemOptions {
  serviceName?: string
  scriptName?: string | null
  scriptTags?: string[] | undefined
  outcome?: string
  wallTime?: number
  cpuTime?: number
  responseStatus?: number
  event?: TraceItemFetchEventInfo | null
}

function buildTraceItem(options: TraceItemOptions = {}): TraceItem {
  const {
    serviceName = 'filbeam-piece-retriever-dev',
    scriptName = 'piece-retriever-dev',
    outcome = 'ok',
    wallTime = 50,
    cpuTime = 10,
    responseStatus = 200,
  } = options

  const resolvedScriptTags =
    'scriptTags' in options
      ? options.scriptTags
      : [`cf:service=${serviceName}`, 'cf:environment=dev']

  const resolvedEvent =
    'event' in options
      ? options.event
      : ({
          request: {
            url: 'https://example.com/piece/123',
            method: 'GET',
          },
          response: { status: responseStatus },
        } as TraceItemFetchEventInfo)

  return {
    scriptName,
    entrypoint: 'default',
    scriptTags: resolvedScriptTags,
    event: resolvedEvent,
    eventTimestamp: Date.now(),
    logs: [],
    exceptions: [],
    diagnosticsChannelEvents: [],
    outcome,
    scriptVersion: { id: 'test-version' },
    truncated: false,
    executionModel: 'stateless',
    cpuTime,
    wallTime,
  }
}
