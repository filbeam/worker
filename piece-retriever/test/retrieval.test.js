import { createExecutionContext, waitOnExecutionContext } from 'cloudflare:test'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { retrieveFile, getRetrievalUrl } from '../lib/retrieval.js'

describe('retrieveFile', () => {
  const baseUrl = 'https://example.com'
  const pieceCid = 'bafy123abc'
  let fetchMock
  let cachesMock

  beforeEach(() => {
    fetchMock = vi
      .fn()
      .mockResolvedValue({ ok: true, status: 200, headers: new Headers({}) })
    global.fetch = fetchMock
    cachesMock = {
      match: vi.fn(),
      put: vi.fn().mockResolvedValueOnce(),
    }
    global.caches = { default: cachesMock }
  })

  it('constructs the correct URL', async () => {
    cachesMock.match.mockResolvedValueOnce(null)
    const ctx = createExecutionContext()
    await retrieveFile(ctx, baseUrl, pieceCid, new Request(baseUrl))
    await waitOnExecutionContext(ctx)
    expect(fetchMock).toHaveBeenCalledWith(
      `${baseUrl}/piece/${pieceCid}`,
      expect.any(Object),
    )
  })

  it('uses the default cacheTtl if not provided', async () => {
    cachesMock.match.mockResolvedValueOnce(null)
    const ctx = createExecutionContext()
    await retrieveFile(ctx, baseUrl, pieceCid, new Request(baseUrl))
    await waitOnExecutionContext(ctx)
    expect(cachesMock.put.mock.calls[0][1].headers.get('Cache-Control')).toBe(
      'public, max-age=86400',
    )
  })

  it('uses the provided cacheTtl', async () => {
    cachesMock.match.mockResolvedValueOnce(null)
    const ctx = createExecutionContext()
    await retrieveFile(ctx, baseUrl, pieceCid, new Request(baseUrl), 1234)
    await waitOnExecutionContext(ctx)
    expect(cachesMock.put.mock.calls[0][1].headers.get('Cache-Control')).toBe(
      'public, max-age=1234',
    )
  })

  it('returns the cached response', async () => {
    const response = { ok: true, status: 200, headers: new Headers({}) }
    cachesMock.match.mockResolvedValueOnce(response)
    const ctx = createExecutionContext()
    const result = await retrieveFile(
      ctx,
      baseUrl,
      pieceCid,
      new Request(baseUrl),
    )
    await waitOnExecutionContext(ctx)
    expect(result.response).toBe(response)
  })

  it('returns the not ok fetch response', async () => {
    cachesMock.match.mockResolvedValueOnce(null)
    const response = { ok: false, status: 500, headers: new Headers({}) }
    fetchMock.mockResolvedValueOnce(response)
    const ctx = createExecutionContext()
    const result = await retrieveFile(
      ctx,
      baseUrl,
      pieceCid,
      new Request(baseUrl),
    )
    await waitOnExecutionContext(ctx)
    expect(result.response).toBe(response)
  })

  it('caches and returns a newly cached response', async () => {
    cachesMock.match.mockResolvedValueOnce(null)
    const response = {
      ok: true,
      status: 201,
      headers: new Headers({ foo: 'bar' }),
    }
    fetchMock.mockResolvedValueOnce(response)
    const ctx = createExecutionContext()
    const result = await retrieveFile(
      ctx,
      baseUrl,
      pieceCid,
      new Request(baseUrl),
    )
    await waitOnExecutionContext(ctx)
    expect(result.response.status).toBe(201)
    expect(result.response.headers.get('foo')).toBe('bar')
    expect(cachesMock.put.mock.calls[0][0]).toBe(`${baseUrl}/piece/${pieceCid}`)
  })
})

describe('getRetrievalUrl', () => {
  it('appends the endpoint name and piece CID to the base URL', () => {
    const url = getRetrievalUrl('https://example.com', 'bafy123abc')
    expect(url).toBe('https://example.com/piece/bafy123abc')
  })

  it('avoids double slash in path when the base URL ends with a slash', () => {
    const url = getRetrievalUrl('https://example.com/', 'bafy123abc')
    expect(url).toBe('https://example.com/piece/bafy123abc')
  })
})
