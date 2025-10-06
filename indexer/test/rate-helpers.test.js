import { describe, it, expect } from 'vitest'
import {
  BYTES_PER_KIB,
  BYTES_PER_MIB,
  BYTES_PER_GIB,
  BYTES_PER_TIB,
  USDFC_DECIMALS,
  USDFC_UNIT,
  calculateRatePerByte,
  calculateEgressQuota,
  formatUsdfcAmount,
} from '../lib/rate-helpers.js'

describe('rate-helpers', () => {
  describe('Storage unit constants', () => {
    it('defines correct byte values for storage units', () => {
      expect(BYTES_PER_KIB).toBe(1024n)
      expect(BYTES_PER_MIB).toBe(1024n * 1024n)
      expect(BYTES_PER_GIB).toBe(1024n * 1024n * 1024n)
      expect(BYTES_PER_TIB).toBe(1024n * 1024n * 1024n * 1024n)
      expect(BYTES_PER_TIB).toBe(1099511627776n)
    })
  })

  describe('USDFC constants', () => {
    it('defines correct USDFC decimal values', () => {
      expect(USDFC_DECIMALS).toBe(18n)
      expect(USDFC_UNIT).toBe(10n ** 18n)
      expect(USDFC_UNIT).toBe(1000000000000000000n)
    })
  })

  describe('calculateRatePerByte', () => {
    it('calculates rate per byte from rate per TiB', () => {
      // 5 USDFC per TiB = 5e18 units per TiB
      const ratePerTiB = '5000000000000000000'
      const ratePerByte = calculateRatePerByte(ratePerTiB)

      // Expected: 5e18 / 1099511627776 (integer division truncates)
      expect(ratePerByte).toBe(4547473n)
    })

    it('handles string input', () => {
      const ratePerByte = calculateRatePerByte('1099511627776000000000')
      expect(ratePerByte).toBe(1000000000n) // 1e9 units per byte
    })

    it('handles BigInt input', () => {
      const ratePerByte = calculateRatePerByte(1099511627776000000000n)
      expect(ratePerByte).toBe(1000000000n)
    })

    it('returns zero for zero rate', () => {
      const ratePerByte = calculateRatePerByte('0')
      expect(ratePerByte).toBe(0n)
    })
  })

  describe('calculateEgressQuota', () => {
    it('calculates quota for standard values', () => {
      // 5 USDFC lockup with 5 USDFC per TiB rate = 1 TiB quota
      const lockup = '5000000000000000000'
      const ratePerTiB = '5000000000000000000'
      const quota = calculateEgressQuota(lockup, ratePerTiB)

      expect(quota).toBe(BYTES_PER_TIB)
    })

    it('calculates quota for different lockup amounts', () => {
      const ratePerTiB = '5000000000000000000' // 5 USDFC per TiB

      // 10 USDFC = 2 TiB
      const quota1 = calculateEgressQuota('10000000000000000000', ratePerTiB)
      expect(quota1).toBe(BYTES_PER_TIB * 2n)

      // 2.5 USDFC = 0.5 TiB
      const quota2 = calculateEgressQuota('2500000000000000000', ratePerTiB)
      expect(quota2).toBe(BYTES_PER_TIB / 2n)

      // 1 USDFC = 0.2 TiB
      const quota3 = calculateEgressQuota('1000000000000000000', ratePerTiB)
      expect(quota3).toBe(BYTES_PER_TIB / 5n)
    })

    it('handles zero lockup amount', () => {
      const quota = calculateEgressQuota('0', '5000000000000000000')
      expect(quota).toBe(0n)
    })

    it('handles zero rate (returns 0 to avoid division by zero)', () => {
      const quota = calculateEgressQuota('5000000000000000000', '0')
      expect(quota).toBe(0n)
    })

    it('handles BigInt inputs', () => {
      const lockup = 10000000000000000000n // 10 USDFC
      const ratePerTiB = 5000000000000000000n // 5 USDFC per TiB
      const quota = calculateEgressQuota(lockup, ratePerTiB)

      expect(quota).toBe(BYTES_PER_TIB * 2n)
    })

    it('handles fractional results (rounds down)', () => {
      // 3 USDFC with 5 USDFC per TiB = 0.6 TiB
      // Should round down in integer division
      const lockup = '3000000000000000000'
      const ratePerTiB = '5000000000000000000'
      const quota = calculateEgressQuota(lockup, ratePerTiB)

      const expectedQuota = (BYTES_PER_TIB * 3n) / 5n
      expect(quota).toBe(expectedQuota)
      expect(quota).toBe(659706976665n) // Floor of 0.6 TiB in bytes
    })

    it('maintains precision for complex calculations', () => {
      // Test that we multiply before dividing to maintain precision
      const lockup = '7777777777777777777' // ~7.78 USDFC
      const ratePerTiB = '3333333333333333333' // ~3.33 USDFC per TiB
      const quota = calculateEgressQuota(lockup, ratePerTiB)

      // Expected: 7.78 / 3.33 * 1 TiB = ~2.33 TiB
      const expectedQuota =
        (BigInt(lockup) * BYTES_PER_TIB) / BigInt(ratePerTiB)
      expect(quota).toBe(expectedQuota)
      // Actual integer division result
      expect(quota).toBe(2565527131477n)
    })
  })

  describe('formatUsdfcAmount', () => {
    it('converts USDFC to smallest units', () => {
      // 1 USDFC = 1e18 units
      expect(formatUsdfcAmount(1)).toBe('1000000000000000000')
      expect(formatUsdfcAmount('1')).toBe('1000000000000000000')
    })

    it('handles decimal amounts', () => {
      // 0.5 USDFC = 5e17 units
      expect(formatUsdfcAmount(0.5)).toBe('500000000000000000')
      expect(formatUsdfcAmount('0.5')).toBe('500000000000000000')

      // 2.5 USDFC = 2.5e18 units
      expect(formatUsdfcAmount(2.5)).toBe('2500000000000000000')
    })

    it('handles large amounts', () => {
      expect(formatUsdfcAmount(1000)).toBe('1000000000000000000000')
      // JavaScript precision limitation for large numbers
      expect(formatUsdfcAmount('1000000')).toBe('999999999999999983222784')
    })

    it('handles very small amounts', () => {
      // 0.000000000000000001 USDFC = 1 unit
      expect(formatUsdfcAmount(0.000000000000000001)).toBe('1')
      expect(formatUsdfcAmount(1e-18)).toBe('1')
    })

    it('handles zero amount', () => {
      expect(formatUsdfcAmount(0)).toBe('0')
      expect(formatUsdfcAmount('0')).toBe('0')
    })

    it('truncates amounts smaller than 1 unit', () => {
      // 0.0000000000000000001 USDFC = 0.1 units, truncated to 0
      expect(formatUsdfcAmount(1e-19)).toBe('0')
    })

    it('handles string with decimals', () => {
      // JavaScript precision limitation with decimal strings
      expect(formatUsdfcAmount('3.141592653589793')).toBe('3141592653589793280')
    })

    it('handles scientific notation', () => {
      // JavaScript precision limitation for large numbers
      expect(formatUsdfcAmount(1e6)).toBe('999999999999999983222784')
      expect(formatUsdfcAmount('1e6')).toBe('999999999999999983222784')
    })

    it('returns string for BigInt compatibility', () => {
      const result = formatUsdfcAmount(5)
      expect(typeof result).toBe('string')
      expect(BigInt(result)).toBe(5000000000000000000n)
    })

    it('handles negative amounts (though not typical for this use case)', () => {
      expect(formatUsdfcAmount(-1)).toBe('-1000000000000000000')
    })
  })

  describe('Integration tests', () => {
    it('correctly converts rate and calculates quota', () => {
      // 5 USDFC per TiB pricing
      const ratePerTiB = formatUsdfcAmount(5)

      // User locks up 10 USDFC
      const lockup = formatUsdfcAmount(10)

      // Should get 2 TiB quota
      const quota = calculateEgressQuota(lockup, ratePerTiB)
      expect(quota).toBe(BYTES_PER_TIB * 2n)
    })

    it('handles fractional USDFC amounts', () => {
      // 0.5 USDFC per TiB pricing
      const ratePerTiB = formatUsdfcAmount(0.5)

      // User locks up 0.25 USDFC
      const lockup = formatUsdfcAmount(0.25)

      // Should get 0.5 TiB quota
      const quota = calculateEgressQuota(lockup, ratePerTiB)
      expect(quota).toBe(BYTES_PER_TIB / 2n)
    })

    it('maintains consistency across different input formats', () => {
      const rateString = '5000000000000000000'
      const rateBigInt = 5000000000000000000n
      const rateNumber = formatUsdfcAmount(5)

      const lockup = '10000000000000000000'

      const quota1 = calculateEgressQuota(lockup, rateString)
      const quota2 = calculateEgressQuota(lockup, rateBigInt)
      const quota3 = calculateEgressQuota(lockup, rateNumber)

      expect(quota1).toBe(quota2)
      expect(quota2).toBe(quota3)
      expect(quota1).toBe(BYTES_PER_TIB * 2n)
    })
  })
})
