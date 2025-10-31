import {
  createExecutionContext,
  waitOnExecutionContext,
  fetchMock,
} from 'cloudflare:test'
import {
  describe,
  it,
  expect,
  vi,
  beforeEach,
  beforeAll,
  afterEach,
} from 'vitest'
import { retrieveFile, getRetrievalUrl } from '../lib/retrieval.js'

describe('retrieveFile', () => {
  const baseUrl = 'https://example.com'
  const pieceCid = 'bafy123abc'
  let cachesMock

  beforeAll(() => {
    fetchMock.activate()
    fetchMock.disableNetConnect()
  })

  beforeEach(() => {
    cachesMock = {
      match: vi.fn(),
      put: vi.fn().mockResolvedValueOnce(),
    }
    global.caches = { default: cachesMock }
  })

  afterEach(() => {
    fetchMock.assertNoPendingInterceptors()
  })

  it('constructs the correct URL', async () => {
    cachesMock.match.mockResolvedValueOnce(null)
    const expectedUrl = `${baseUrl}/piece/${pieceCid}`
    fetchMock.get(baseUrl).intercept({ path: expectedUrl }).reply(200)
    const ctx = createExecutionContext()
    const { url } = await retrieveFile(
      ctx,
      baseUrl,
      pieceCid,
      new Request(baseUrl),
    )
    await waitOnExecutionContext(ctx)
    expect(url).toBe(expectedUrl)
  })

  it('uses the default cacheTtl if not provided', async () => {
    cachesMock.match.mockResolvedValueOnce(null)
    fetchMock
      .get(baseUrl)
      .intercept({ path: `/piece/${pieceCid}` })
      .reply(200)
    const ctx = createExecutionContext()
    await retrieveFile(ctx, baseUrl, pieceCid, new Request(baseUrl))
    await waitOnExecutionContext(ctx)
    expect(cachesMock.put.mock.calls[0][1].headers.get('Cache-Control')).toBe(
      'public, max-age=86400',
    )
  })

  it('uses the provided cacheTtl', async () => {
    cachesMock.match.mockResolvedValueOnce(null)
    fetchMock
      .get(baseUrl)
      .intercept({ path: `/piece/${pieceCid}` })
      .reply(200)
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
    fetchMock
      .get(baseUrl)
      .intercept({ path: `/piece/${pieceCid}` })
      .reply(500)
    const response = { ok: false, status: 500, headers: new Headers({}) }
    const ctx = createExecutionContext()
    const result = await retrieveFile(
      ctx,
      baseUrl,
      pieceCid,
      new Request(baseUrl),
    )
    await waitOnExecutionContext(ctx)
    expect(result.response).toMatchObject(response)
  })

  it('caches and returns a newly cached response', async () => {
    cachesMock.match.mockResolvedValueOnce(null)
    fetchMock
      .get(baseUrl)
      .intercept({ path: `/piece/${pieceCid}` })
      .reply(201, null, {
        headers: {
          foo: 'bar',
        },
      })
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

  it('supports range requests (uncached)', async () => {
    cachesMock.match.mockResolvedValueOnce(null)
    fetchMock
      .get(baseUrl)
      .intercept({ path: `/piece/${pieceCid}` })
      .reply(206, '', {
        headers: { 'content-range': 'bytes 0-1/100' },
      })
    const ctx = createExecutionContext()
    const result = await retrieveFile(
      ctx,
      baseUrl,
      pieceCid,
      new Request(baseUrl, {
        headers: new Headers({
          Range: 'bytes=0-1',
        }),
      }),
    )
    await waitOnExecutionContext(ctx)
    expect(result.response.status).toBe(206)
    expect(result.response.headers.get('content-range')).toBe('bytes 0-1/100')
  })

  it('supports range requests (cached)', async () => {
    const response = {
      ok: true,
      status: 206,
      headers: new Headers({ 'content-range': 'bytes 0-1/100' }),
    }
    cachesMock.match.mockResolvedValueOnce(response)
    const ctx = createExecutionContext()
    const result = await retrieveFile(
      ctx,
      baseUrl,
      pieceCid,
      new Request(baseUrl, {
        headers: new Headers({
          Range: 'bytes=0-1',
        }),
      }),
    )
    await waitOnExecutionContext(ctx)
    expect(result.response.status).toBe(206)
    expect(result.response.headers.get('content-range')).toBe('bytes 0-1/100')
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
