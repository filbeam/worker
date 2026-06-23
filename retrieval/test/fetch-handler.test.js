import { describe, it, expect } from 'vitest'
import { handleFetchRequest } from '../lib/fetch-handler.js'

describe('handleFetchRequest', () => {
  it('returns the handler response unchanged', async () => {
    const res = await handleFetchRequest(
      new Request('https://example.com/'),
      async () => new Response('ok', { status: 200 }),
    )

    expect(res.status).toBe(200)
    expect(await res.text()).toBe('ok')
  })

  it('rejects non-GET/HEAD methods with 405 without running the handler', async () => {
    let ran = false
    const res = await handleFetchRequest(
      new Request('https://example.com/', { method: 'POST' }),
      async () => {
        ran = true
        return new Response('ok')
      },
    )

    expect(res.status).toBe(405)
    expect(await res.text()).toBe('Method Not Allowed')
    expect(ran).toBe(false)
  })

  it('allows HEAD requests', async () => {
    const res = await handleFetchRequest(
      new Request('https://example.com/', { method: 'HEAD' }),
      async () => new Response('ok', { status: 200 }),
    )

    expect(res.status).toBe(200)
  })

  it('redirects legacy *.filcdn.io requests before running the handler', async () => {
    let ran = false
    const res = await handleFetchRequest(
      new Request('https://0xabc.filcdn.io/baga123'),
      async () => {
        ran = true
        return new Response('ok')
      },
    )

    expect(res.status).toBe(301)
    expect(res.headers.get('Location')).toBe('https://0xabc.filbeam.io/baga123')
    expect(ran).toBe(false)
  })

  it('turns a thrown error into a response via handleError', async () => {
    const res = await handleFetchRequest(
      new Request('https://example.com/'),
      async () => {
        throw Object.assign(new Error('Bad Request'), { status: 400 })
      },
    )

    expect(res.status).toBe(400)
    expect(await res.text()).toBe('Bad Request')
  })

  it('hides the message for server errors', async () => {
    const res = await handleFetchRequest(
      new Request('https://example.com/'),
      async () => {
        throw new Error('boom')
      },
    )

    expect(res.status).toBe(500)
    expect(await res.text()).toBe('Internal Server Error')
  })
})
