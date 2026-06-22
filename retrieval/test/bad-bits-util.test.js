import { describe, it, expect } from 'vitest'
import { getBadBitsEntry, isCidDenied } from '../lib/bad-bits-util.js'

describe('getBadBitsEntry', () => {
  it('creates entry in the legacy double-hash format', async () => {
    const cid = 'bafybeiefwqslmf6zyyrxodaxx4vwqircuxpza5ri45ws3y5a62ypxti42e'

    const result = await getBadBitsEntry(cid)

    expect(result).toBe(
      'd9d295bde21f422d471a90f2a37ec53049fdf3e5fa3ee2e8f20e10003da429e7',
    )
  })
})

describe('isCidDenied', () => {
  it('returns true when the denylist has an entry for the CID, querying by double-hash key', async () => {
    const cid = 'bafybeiefwqslmf6zyyrxodaxx4vwqircuxpza5ri45ws3y5a62ypxti42e'
    const expectedKey = `bad-bits:${await getBadBitsEntry(cid)}`
    /** @type {string[]} */
    const queriedKeys = []
    const env = {
      BAD_BITS_KV: {
        get: async (/** @type {string} */ key) => {
          queriedKeys.push(key)
          return key === expectedKey ? {} : null
        },
      },
    }

    expect(await isCidDenied(env, cid)).toBe(true)
    expect(queriedKeys).toEqual([expectedKey])
  })

  it('returns false when the CID is not on the denylist', async () => {
    const env = { BAD_BITS_KV: { get: async () => null } }
    expect(await isCidDenied(env, 'bafytest')).toBe(false)
  })
})
