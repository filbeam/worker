/** Tests for analytics module */

import { describe, it, expect } from 'vitest'
import { calculateTotalBytes } from '../lib/analytics.js'

describe('Analytics Module', () => {
  describe('calculateTotalBytes', () => {
    it('should calculate total CDN and cache miss bytes correctly', () => {
      const usageData = [
        { cdn_bytes: 1000, cache_miss_bytes: 500 },
        { cdn_bytes: 2000, cache_miss_bytes: 800 },
        { cdn_bytes: 1500, cache_miss_bytes: 600 },
      ]

      const result = calculateTotalBytes(usageData)

      expect(result).toEqual({
        totalCdnBytes: 4500n,
        totalCacheMissBytes: 1900n,
      })
    })

    it('should handle empty usage data', () => {
      const usageData = []

      const result = calculateTotalBytes(usageData)

      expect(result).toEqual({
        totalCdnBytes: 0n,
        totalCacheMissBytes: 0n,
      })
    })

    it('should handle single dataset', () => {
      const usageData = [{ cdn_bytes: 5000, cache_miss_bytes: 2000 }]

      const result = calculateTotalBytes(usageData)

      expect(result).toEqual({
        totalCdnBytes: 5000n,
        totalCacheMissBytes: 2000n,
      })
    })
  })
})
