import { describe, it, expect, vi, beforeEach } from 'vitest'
import { retrieveIpfsContent, getRetrievalUrl } from '../lib/retrieval.js'

describe('retrieveIpfsContent', () => {
  const baseUrl = 'https://example.com'
  const ipfsRootCid =
    'bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi'
  const defaultCacheTtl = 86400
  let fetchMock

  beforeEach(() => {
    fetchMock = vi
      .fn()
      .mockResolvedValue({ ok: true, status: 200, headers: new Headers({}) })
    global.fetch = fetchMock
  })

  it('constructs the correct URL with root path', async () => {
    await retrieveIpfsContent(baseUrl, ipfsRootCid, '/')
    expect(fetchMock).toHaveBeenCalledWith(
      `${baseUrl}/ipfs/${ipfsRootCid}?format=car`,
      expect.any(Object),
    )
  })

  it('constructs the correct URL with subpath', async () => {
    const subpath = '/path/to/file.txt'
    await retrieveIpfsContent(baseUrl, ipfsRootCid, subpath)
    expect(fetchMock).toHaveBeenCalledWith(
      `${baseUrl}/ipfs/${ipfsRootCid}${subpath}?format=car`,
      expect.any(Object),
    )
  })

  it('constructs the correct URL with nested subpath', async () => {
    const subpath = '/deep/nested/directory/file.json'
    await retrieveIpfsContent(baseUrl, ipfsRootCid, subpath)
    expect(fetchMock).toHaveBeenCalledWith(
      `${baseUrl}/ipfs/${ipfsRootCid}${subpath}?format=car`,
      expect.any(Object),
    )
  })

  it('uses the default cacheTtl if not provided', async () => {
    await retrieveIpfsContent(baseUrl, ipfsRootCid, '/')
    const options = fetchMock.mock.calls[0][1]
    expect(options.cf.cacheTtlByStatus['200-299']).toBe(defaultCacheTtl)
  })

  it('uses the provided cacheTtl', async () => {
    await retrieveIpfsContent(baseUrl, ipfsRootCid, '/', 1234)
    const options = fetchMock.mock.calls[0][1]
    expect(options.cf.cacheTtlByStatus['200-299']).toBe(1234)
  })

  it('sets correct cacheTtlByStatus and cacheEverything', async () => {
    await retrieveIpfsContent(baseUrl, ipfsRootCid, '/', 555)
    const options = fetchMock.mock.calls[0][1]
    expect(options.cf).toEqual({
      cacheTtlByStatus: {
        '200-299': 555,
        404: 0,
        '500-599': 0,
      },
      cacheEverything: true,
    })
  })

  it('passes the signal option correctly', async () => {
    const signal = new AbortController().signal
    await retrieveIpfsContent(baseUrl, ipfsRootCid, '/', 86400, { signal })
    const options = fetchMock.mock.calls[0][1]
    expect(options.signal).toBe(signal)
  })

  it('returns the fetch response and cache miss status', async () => {
    const response = { ok: true, status: 200, headers: new Headers({}) }
    fetchMock.mockResolvedValueOnce(response)
    const result = await retrieveIpfsContent(baseUrl, ipfsRootCid, '/')
    expect(result.response).toBe(response)
    expect(result.cacheMiss).toBe(true) // No CF-Cache-Status header means cache miss
  })

  it('detects cache hit from CF-Cache-Status header', async () => {
    const headers = new Headers({ 'CF-Cache-Status': 'HIT' })
    const response = { ok: true, status: 200, headers }
    fetchMock.mockResolvedValueOnce(response)
    const result = await retrieveIpfsContent(baseUrl, ipfsRootCid, '/')
    expect(result.cacheMiss).toBe(false)
  })

  it('detects cache miss from CF-Cache-Status header', async () => {
    const headers = new Headers({ 'CF-Cache-Status': 'MISS' })
    const response = { ok: true, status: 200, headers }
    fetchMock.mockResolvedValueOnce(response)
    const result = await retrieveIpfsContent(baseUrl, ipfsRootCid, '/')
    expect(result.cacheMiss).toBe(true)
  })

  it('always appends format=car query parameter', async () => {
    await retrieveIpfsContent(baseUrl, ipfsRootCid, '/file.txt')
    expect(fetchMock).toHaveBeenCalledWith(
      `${baseUrl}/ipfs/${ipfsRootCid}/file.txt?format=car`,
      expect.any(Object),
    )
  })
})

describe('getRetrievalUrl', () => {
  it('constructs URL with root path', () => {
    const url = getRetrievalUrl('https://example.com', 'bafy123abc', '/')
    expect(url).toBe('https://example.com/ipfs/bafy123abc')
  })

  it('constructs URL with subpath', () => {
    const url = getRetrievalUrl(
      'https://example.com',
      'bafy123abc',
      '/file.txt',
    )
    expect(url).toBe('https://example.com/ipfs/bafy123abc/file.txt')
  })

  it('constructs URL with nested subpath', () => {
    const url = getRetrievalUrl(
      'https://example.com',
      'bafy123abc',
      '/path/to/file.json',
    )
    expect(url).toBe('https://example.com/ipfs/bafy123abc/path/to/file.json')
  })

  it('avoids double slash in path when the base URL ends with a slash', () => {
    const url = getRetrievalUrl(
      'https://example.com/',
      'bafy123abc',
      '/file.txt',
    )
    expect(url).toBe('https://example.com/ipfs/bafy123abc/file.txt')
  })

  it('handles subpath with trailing slash', () => {
    const url = getRetrievalUrl(
      'https://example.com',
      'bafy123abc',
      '/directory/',
    )
    expect(url).toBe('https://example.com/ipfs/bafy123abc/directory/')
  })

  it('handles empty subpath correctly', () => {
    const url = getRetrievalUrl('https://example.com', 'bafy123abc', '')
    expect(url).toBe('https://example.com/ipfs/bafy123abc')
  })

  it('preserves special characters in subpath', () => {
    const url = getRetrievalUrl(
      'https://example.com',
      'bafy123abc',
      '/file%20with%20spaces.txt',
    )
    expect(url).toBe(
      'https://example.com/ipfs/bafy123abc/file%20with%20spaces.txt',
    )
  })
})
