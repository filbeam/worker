import { describe, it, expect } from 'vitest'
import {
  setRetrievalResponseHeaders,
  buildRetrievalResponse,
} from '../lib/response-headers.js'

describe('setRetrievalResponseHeaders', () => {
  it('sets the CSP, data set id and client cache headers', () => {
    const response = new Response('body')

    setRetrievalResponseHeaders(response, {
      dataSetId: '42',
      clientCacheTtl: 31536000,
    })

    expect(response.headers.get('Content-Security-Policy')).toMatch(
      /^default-src 'self'/,
    )
    expect(response.headers.get('X-Data-Set-ID')).toBe('42')
    expect(response.headers.get('Cache-Control')).toBe(
      'public, max-age=31536000',
    )
  })
})

describe('buildRetrievalResponse', () => {
  it('proxies the body, status and headers with retrieval headers applied', async () => {
    const headers = new Headers({ 'Content-Type': 'text/plain' })
    const response = buildRetrievalResponse(
      { CLIENT_CACHE_TTL: 31536000 },
      {
        body: 'hello',
        status: 206,
        statusText: 'Partial Content',
        headers,
        dataSetId: '42',
      },
    )

    expect(response.status).toBe(206)
    expect(response.statusText).toBe('Partial Content')
    expect(response.headers.get('Content-Type')).toBe('text/plain')
    expect(response.headers.get('X-Data-Set-ID')).toBe('42')
    expect(response.headers.get('Cache-Control')).toBe(
      'public, max-age=31536000',
    )
    expect(response.headers.get('Content-Security-Policy')).toMatch(
      /^default-src 'self'/,
    )
    expect(await response.text()).toBe('hello')
  })
})
