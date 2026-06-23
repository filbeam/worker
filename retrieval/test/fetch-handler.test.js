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
