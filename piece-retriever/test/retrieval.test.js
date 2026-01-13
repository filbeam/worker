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
    let requestedCfOptions = 'fetch() was not called'
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
      requestedCfOptions = init?.cf
      return new Response(200)
    })
    const ctx = createExecutionContext()
    await retrieveFile(ctx, baseUrl, pieceCid, new Request(baseUrl))
    await waitOnExecutionContext(ctx)
    expect(requestedCfOptions).toEqual({
      cacheTtlByStatus: {
        '200-299': 86400,
        404: 0,
        '500-599': 0,
      },
      cacheEverything: true,
    })
  })

  it('uses the provided cacheTtl', async () => {
    let requestedCfOptions = 'fetch() was not called'
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
      requestedCfOptions = init?.cf
      return new Response(200)
    })
    const ctx = createExecutionContext()
    await retrieveFile(ctx, baseUrl, pieceCid, new Request(baseUrl), 1234)
    await waitOnExecutionContext(ctx)
    expect(requestedCfOptions).toEqual({
      cacheTtlByStatus: {
        '200-299': 1234,
        404: 0,
        '500-599': 0,
      },
      cacheEverything: true,
    })
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

  it('returns the response from the origin', async () => {
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

  it("by default doesn't validate cache miss responses", async () => {
    cachesMock.match.mockResolvedValueOnce(null)
    fetchMock
      .get(baseUrl)
      .intercept({ path: `/piece/${pieceCid}` })
      .reply(200, 'invalid')
    const ctx = createExecutionContext()
    const result = await retrieveFile(
      ctx,
      baseUrl,
      pieceCid,
      new Request(baseUrl),
    )
    await waitOnExecutionContext(ctx)
    expect(result.validate).toBe(null)
  })

  it('validates an invalid cache miss response', async () => {
    cachesMock.match.mockResolvedValueOnce(null)
    fetchMock
      .get(baseUrl)
      .intercept({ path: `/piece/${pieceCid}` })
      .reply(200, 'invalid')
    const ctx = createExecutionContext()
    const result = await retrieveFile(
      ctx,
      baseUrl,
      pieceCid,
      new Request(baseUrl),
      null,
      { addCacheMissResponseValidation: true },
    )
    await waitOnExecutionContext(ctx)
    expect(result.validate).toBeInstanceOf(Function)
    expect(result.validate()).toBe(false)
  })

  it('validates a valid cache miss response', async () => {
    // Hash of `'valid'`
    const pieceCid =
      'bafkzcibcpibpuevmzufhyt73qvctc7ndhfpl7peuihgkl6mepmrm3w3rzwnnofa'
    cachesMock.match.mockResolvedValueOnce(null)
    fetchMock
      .get(baseUrl)
      .intercept({
        path: `/piece/${pieceCid}`,
      })
      .reply(200, 'valid')
    const ctx = createExecutionContext()
    const result = await retrieveFile(
      ctx,
      baseUrl,
      pieceCid,
      new Request(baseUrl),
      null,
      { addCacheMissResponseValidation: true },
    )
    const reader = result.response.body.getReader()
    while (true) {
      const { done } = await reader.read()
      if (done) {
        break
      }
    }
    await waitOnExecutionContext(ctx)
    expect(result.validate).toBeInstanceOf(Function)
    expect(result.validate()).toBe(true)
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
