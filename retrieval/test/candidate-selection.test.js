import { describe, it, expect } from 'vitest'
import { selectRetrievalCandidate } from '../lib/candidate-selection.js'

const ok = () => ({
  response: new Response('ok', { status: 200 }),
  cacheMiss: true,
})
const notFound = () => ({
  response: new Response('nope', { status: 404 }),
  cacheMiss: false,
})

describe('selectRetrievalCandidate', () => {
  it('returns no candidate or result for an empty candidate list', async () => {
    const { candidate, result, attempts } = await selectRetrievalCandidate(
      [],
      async () => ok(),
    )

    expect(candidate).toBeUndefined()
    expect(result).toBeUndefined()
    expect(attempts).toEqual([])
  })

  it('stops at the first candidate that returns an OK response', async () => {
    const candidates = [
      { serviceUrl: 'https://a.example' },
      { serviceUrl: 'https://b.example' },
      { serviceUrl: 'https://c.example' },
    ]

    const { candidate, result, attempts } = await selectRetrievalCandidate(
      candidates,
      async () => ok(),
    )

    expect(attempts).toHaveLength(1)
    expect(candidates).toContainEqual(candidate)
    expect(attempts).toEqual([candidate])
    expect(result?.response.ok).toBe(true)
  })

  it('retries after a non-OK response until one succeeds', async () => {
    const candidates = [
      { serviceUrl: 'https://a.example' },
      { serviceUrl: 'https://b.example' },
      { serviceUrl: 'https://c.example' },
    ]
    let calls = 0
    const attemptRetrieval = async () => {
      calls++
      return calls < 2 ? notFound() : ok()
    }

    const { candidate, result, attempts } = await selectRetrievalCandidate(
      candidates,
      attemptRetrieval,
    )

    expect(attempts).toHaveLength(2)
    expect(attempts[attempts.length - 1]).toEqual(candidate)
    expect(result?.response.ok).toBe(true)
  })

  it('retries after a thrown error until one succeeds', async () => {
    const candidates = [
      { serviceUrl: 'https://a.example' },
      { serviceUrl: 'https://b.example' },
    ]
    let calls = 0
    const attemptRetrieval = async () => {
      calls++
      if (calls === 1) throw new Error('boom')
      return ok()
    }

    const { result, attempts } = await selectRetrievalCandidate(
      candidates,
      attemptRetrieval,
    )

    expect(attempts).toHaveLength(2)
    expect(result?.response.ok).toBe(true)
  })

  it('returns the last result when every candidate responds non-OK', async () => {
    const candidates = [
      { serviceUrl: 'https://a.example' },
      { serviceUrl: 'https://b.example' },
    ]

    const { candidate, result, attempts } = await selectRetrievalCandidate(
      candidates,
      async () => notFound(),
    )

    expect(attempts).toHaveLength(2)
    expect(attempts).toEqual(expect.arrayContaining(candidates))
    expect(candidate).toEqual(attempts[attempts.length - 1])
    expect(result?.response.status).toBe(404)
  })

  it('returns no result but the last candidate when every attempt throws', async () => {
    const candidates = [
      { serviceUrl: 'https://a.example' },
      { serviceUrl: 'https://b.example' },
    ]

    const { candidate, result, attempts } = await selectRetrievalCandidate(
      candidates,
      async () => {
        throw new Error('boom')
      },
    )

    expect(attempts).toHaveLength(2)
    expect(result).toBeUndefined()
    expect(candidate).toEqual(attempts[attempts.length - 1])
  })

  it('does not mutate the input candidate list', async () => {
    const candidates = [
      { serviceUrl: 'https://a.example' },
      { serviceUrl: 'https://b.example' },
    ]
    const snapshot = [...candidates]

    await selectRetrievalCandidate(candidates, async () => notFound())

    expect(candidates).toEqual(snapshot)
  })
})
