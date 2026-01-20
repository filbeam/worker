import { describe, it, expect, vi } from 'vitest'
import { env, createExecutionContext } from 'cloudflare:test'
import worker from '../src/index.ts'

describe('piece-retriever-tail worker', () => {
  it('processes tail events without errors', async () => {
    const mockEvents: TraceItem[] = [buildTraceItem()]

    const ctx = createExecutionContext()
    const consoleSpy = vi.spyOn(console, 'log')

    worker.tail(mockEvents, env, ctx)

    expect(consoleSpy).toHaveBeenCalledWith(mockEvents)
    consoleSpy.mockRestore()
  })
})

function buildTraceItem(overrides: Partial<TraceItem> = {}): TraceItem {
  return {
    scriptName: 'piece-retriever',
    entrypoint: 'default',
    event: {
      request: {
        url: 'https://example.com/piece/123',
        method: 'GET',
      },
    } as TraceItemFetchEventInfo,
    eventTimestamp: Date.now(),
    logs: [],
    exceptions: [],
    outcome: 'ok',
    scriptVersion: { id: 'test-version' },
    truncated: false,
    diagnosticsChannelEvents: [],
    executionModel: 'stateless',
    cpuTime: 10,
    wallTime: 50,
    ...overrides,
  }
}
