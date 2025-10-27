import { describe, it, expect } from 'vitest'
import { getBadBitsEntry } from '../lib/bad-bits-util.js'

describe('getBadBitsEntry', () => {
  it('creates entry in the legacy double-hash format', async () => {
    const cid = 'bafybeiefwqslmf6zyyrxodaxx4vwqircuxpza5ri45ws3y5a62ypxti42e'

    const result = await getBadBitsEntry(cid)

    expect(result).toBe(
      'd9d295bde21f422d471a90f2a37ec53049fdf3e5fa3ee2e8f20e10003da429e7',
    )
  })
})
