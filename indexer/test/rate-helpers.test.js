import { describe, it, expect } from 'vitest'
import {
  BYTES_PER_TIB,
  calculateRatePerByte,
  calculateEgressQuota,
} from '../lib/rate-helpers.js'

describe('calculateRatePerByte', () => {
  it('calculates rate per byte from rate per TiB', () => {
    expect(calculateRatePerByte('5000000000000000000')).toBe(4547473n)
  })

  it('handles string input', () => {
    expect(calculateRatePerByte('1099511627776000000000')).toBe(1000000000n)
  })

  it('handles BigInt input', () => {
    expect(calculateRatePerByte(1099511627776000000000n)).toBe(1000000000n)
  })

  it('returns zero for zero rate', () => {
    expect(calculateRatePerByte('0')).toBe(0n)
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
    const expectedQuota = (BigInt(lockup) * BYTES_PER_TIB) / BigInt(ratePerTiB)
    expect(quota).toBe(expectedQuota)
    // Actual integer division result
    expect(quota).toBe(2565527131477n)
  })
})

describe('Integration tests', () => {
  it('correctly converts rate and calculates quota', () => {
    // 5 USDFC per TiB pricing
    const ratePerTiB = '5000000000000000000'

    // User locks up 10 USDFC
    const lockup = '10000000000000000000'

    // Should get 2 TiB quota
    const quota = calculateEgressQuota(lockup, ratePerTiB)
    expect(quota).toBe(BYTES_PER_TIB * 2n)
  })

  it('handles fractional USDFC amounts', () => {
    // 0.5 USDFC per TiB pricing
    const ratePerTiB = '500000000000000000'

    // User locks up 0.25 USDFC
    const lockup = '250000000000000000'

    // Should get 0.5 TiB quota
    const quota = calculateEgressQuota(lockup, ratePerTiB)
    expect(quota).toBe(BYTES_PER_TIB / 2n)
  })

  it('maintains consistency across different input formats', () => {
    const rateString = '5000000000000000000'
    const rateBigInt = 5000000000000000000n

    const lockup = '10000000000000000000'

    const quota1 = calculateEgressQuota(lockup, rateString)
    const quota2 = calculateEgressQuota(lockup, rateBigInt)

    expect(quota1).toBe(quota2)
    expect(quota1).toBe(BYTES_PER_TIB * 2n)
  })
})
