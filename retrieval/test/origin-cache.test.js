import { describe, it, expect } from 'vitest'
import { originCacheOptions } from '../lib/origin-cache.js'

describe('originCacheOptions', () => {
  it('caches 2xx for the given TTL and never caches 404 or 5xx', () => {
    expect(originCacheOptions(86400)).toEqual({
      cacheTtlByStatus: { '200-299': 86400, 404: 0, '500-599': 0 },
      cacheEverything: true,
    })
  })
})
