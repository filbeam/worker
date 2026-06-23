import { describe, it, expect } from 'vitest'
import { redirectLegacyDomain } from '../lib/redirect.js'

describe('redirectLegacyDomain', () => {
  it('redirects *.filcdn.io to *.filbeam.io with a 301', () => {
    const res = redirectLegacyDomain(
      new Request('https://0xabc.filcdn.io/baga123?format=car'),
    )
    expect(res?.status).toBe(301)
    expect(res?.headers.get('Location')).toBe(
      'https://0xabc.filbeam.io/baga123?format=car',
    )
  })

  it('returns undefined for non-legacy domains', () => {
    expect(
      redirectLegacyDomain(new Request('https://0xabc.filbeam.io/baga123')),
    ).toBeUndefined()
  })
})
