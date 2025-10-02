import { describe, it, expect } from 'vitest'
import {
  bigIntToUint8Array,
  uint8ArrayToBigInt,
  bigIntToBase32,
  base32ToBigInt,
} from '../lib/bigint-util.js'

describe('bigint-util', () => {
  describe('bigIntToUint8Array', () => {
    it('converts zero correctly', () => {
      const result = bigIntToUint8Array(0n)
      expect(result).toEqual(new Uint8Array([0]))
    })

    it('converts small positive single-byte values', () => {
      expect(bigIntToUint8Array(1n)).toEqual(new Uint8Array([1]))
      expect(bigIntToUint8Array(255n)).toEqual(new Uint8Array([255]))
    })

    it('converts two-byte values', () => {
      expect(bigIntToUint8Array(256n)).toEqual(new Uint8Array([1, 0]))
      expect(bigIntToUint8Array(257n)).toEqual(new Uint8Array([1, 1]))
      expect(bigIntToUint8Array(65535n)).toEqual(new Uint8Array([255, 255]))
    })

    it('converts medium values requiring multiple bytes', () => {
      // 3 bytes
      expect(bigIntToUint8Array(65536n)).toEqual(new Uint8Array([1, 0, 0]))
      expect(bigIntToUint8Array(16777215n)).toEqual(
        new Uint8Array([255, 255, 255]),
      )

      // 4 bytes
      expect(bigIntToUint8Array(16777216n)).toEqual(
        new Uint8Array([1, 0, 0, 0]),
      )
    })

    it('converts large values requiring 8 bytes', () => {
      const value = 2n ** 64n - 1n // Max 64-bit value
      const result = bigIntToUint8Array(value)
      expect(result.length).toBe(8)
      expect(result).toEqual(
        new Uint8Array([255, 255, 255, 255, 255, 255, 255, 255]),
      )
    })

    it('converts very large values requiring 32+ bytes', () => {
      const value = 2n ** 256n - 1n
      const result = bigIntToUint8Array(value)
      expect(result.length).toBe(32)
      expect(result.every((byte) => byte === 255)).toBe(true)
    })

    it('converts values requiring 64+ bytes', () => {
      const value = 2n ** 512n
      const result = bigIntToUint8Array(value)
      expect(result.length).toBe(65) // 512 bits = 64 bytes + 1 leading byte
      expect(result[0]).toBe(1)
      expect(result.slice(1).every((byte) => byte === 0)).toBe(true)
    })

    it('maintains big-endian byte order', () => {
      // 0x0102 should be [1, 2], not [2, 1]
      expect(bigIntToUint8Array(0x0102n)).toEqual(new Uint8Array([1, 2]))
      expect(bigIntToUint8Array(0x123456n)).toEqual(
        new Uint8Array([0x12, 0x34, 0x56]),
      )
      expect(bigIntToUint8Array(0xabcdefn)).toEqual(
        new Uint8Array([0xab, 0xcd, 0xef]),
      )
    })

    it('handles boundary values at byte transitions', () => {
      // Test values at byte boundaries
      expect(bigIntToUint8Array(254n)).toEqual(new Uint8Array([254]))
      expect(bigIntToUint8Array(255n)).toEqual(new Uint8Array([255]))
      expect(bigIntToUint8Array(256n)).toEqual(new Uint8Array([1, 0]))

      expect(bigIntToUint8Array(65534n)).toEqual(new Uint8Array([255, 254]))
      expect(bigIntToUint8Array(65535n)).toEqual(new Uint8Array([255, 255]))
      expect(bigIntToUint8Array(65536n)).toEqual(new Uint8Array([1, 0, 0]))
    })

    it('handles powers of 2 correctly', () => {
      expect(bigIntToUint8Array(2n ** 8n)).toEqual(new Uint8Array([1, 0]))
      expect(bigIntToUint8Array(2n ** 16n)).toEqual(new Uint8Array([1, 0, 0]))
      expect(bigIntToUint8Array(2n ** 24n)).toEqual(
        new Uint8Array([1, 0, 0, 0]),
      )
      expect(bigIntToUint8Array(2n ** 32n)).toEqual(
        new Uint8Array([1, 0, 0, 0, 0]),
      )
    })

    it('throws error for non-bigint input', () => {
      expect(() => bigIntToUint8Array(123)).toThrow()
      expect(() => bigIntToUint8Array('123')).toThrow()
      expect(() => bigIntToUint8Array(null)).toThrow()
      expect(() => bigIntToUint8Array(undefined)).toThrow()
      expect(() => bigIntToUint8Array({})).toThrow()
      expect(() => bigIntToUint8Array([])).toThrow()
    })
  })

  describe('uint8ArrayToBigInt', () => {
    it('converts single byte correctly', () => {
      expect(uint8ArrayToBigInt(new Uint8Array([0]))).toBe(0n)
      expect(uint8ArrayToBigInt(new Uint8Array([1]))).toBe(1n)
      expect(uint8ArrayToBigInt(new Uint8Array([255]))).toBe(255n)
    })

    it('converts multiple bytes correctly', () => {
      expect(uint8ArrayToBigInt(new Uint8Array([1, 0]))).toBe(256n)
      expect(uint8ArrayToBigInt(new Uint8Array([1, 1]))).toBe(257n)
      expect(uint8ArrayToBigInt(new Uint8Array([255, 255]))).toBe(65535n)
    })

    it('converts large arrays correctly', () => {
      // 4 bytes
      expect(uint8ArrayToBigInt(new Uint8Array([1, 0, 0, 0]))).toBe(16777216n)

      // 8 bytes (max 64-bit)
      const maxUint64 = new Uint8Array([255, 255, 255, 255, 255, 255, 255, 255])
      expect(uint8ArrayToBigInt(maxUint64)).toBe(2n ** 64n - 1n)
    })

    it('converts very large arrays (32+ bytes)', () => {
      const thirtyTwoBytes = new Uint8Array(32).fill(255)
      expect(uint8ArrayToBigInt(thirtyTwoBytes)).toBe(2n ** 256n - 1n)
    })

    it('handles empty array', () => {
      expect(uint8ArrayToBigInt(new Uint8Array([]))).toBe(0n)
    })

    it('handles arrays with leading zeros', () => {
      expect(uint8ArrayToBigInt(new Uint8Array([0, 0, 1]))).toBe(1n)
      expect(uint8ArrayToBigInt(new Uint8Array([0, 1, 0]))).toBe(256n)
      expect(uint8ArrayToBigInt(new Uint8Array([0, 0, 0, 255]))).toBe(255n)
    })

    it('handles all zeros', () => {
      expect(uint8ArrayToBigInt(new Uint8Array([0]))).toBe(0n)
      expect(uint8ArrayToBigInt(new Uint8Array([0, 0]))).toBe(0n)
      expect(uint8ArrayToBigInt(new Uint8Array([0, 0, 0]))).toBe(0n)
    })

    it('interprets bytes as big-endian', () => {
      // [1, 2] should be 0x0102 = 258, not 0x0201 = 513
      expect(uint8ArrayToBigInt(new Uint8Array([1, 2]))).toBe(0x0102n)
      expect(uint8ArrayToBigInt(new Uint8Array([0x12, 0x34, 0x56]))).toBe(
        0x123456n,
      )
      expect(uint8ArrayToBigInt(new Uint8Array([0xab, 0xcd, 0xef]))).toBe(
        0xabcdefn,
      )
    })

    it('handles boundary values', () => {
      expect(uint8ArrayToBigInt(new Uint8Array([254]))).toBe(254n)
      expect(uint8ArrayToBigInt(new Uint8Array([255]))).toBe(255n)
      expect(uint8ArrayToBigInt(new Uint8Array([255, 254]))).toBe(65534n)
      expect(uint8ArrayToBigInt(new Uint8Array([255, 255]))).toBe(65535n)
    })

    it('works with Node.js Buffer', () => {
      const buffer = Buffer.from([1, 2, 3])
      expect(uint8ArrayToBigInt(buffer)).toBe(0x010203n)
    })

    it('throws error for non-Uint8Array input', () => {
      expect(() => uint8ArrayToBigInt(123)).toThrow()
      expect(() => uint8ArrayToBigInt('123')).toThrow()
      expect(() => uint8ArrayToBigInt(null)).toThrow()
      expect(() => uint8ArrayToBigInt(undefined)).toThrow()
      expect(() => uint8ArrayToBigInt({})).toThrow()
    })

    it('throws error for regular arrays', () => {
      expect(() => uint8ArrayToBigInt([1, 2, 3])).toThrow()
    })
  })

  describe('Round-Trip Conversion', () => {
    it('bigint -> array -> bigint preserves value for small numbers', () => {
      const values = [0n, 1n, 127n, 128n, 255n, 256n, 65535n, 65536n]
      values.forEach((value) => {
        const array = bigIntToUint8Array(value)
        const result = uint8ArrayToBigInt(array)
        expect(result).toBe(value)
      })
    })

    it('bigint -> array -> bigint preserves value for large numbers', () => {
      const values = [2n ** 32n, 2n ** 64n, 2n ** 128n, 2n ** 256n, 2n ** 512n]
      values.forEach((value) => {
        const array = bigIntToUint8Array(value)
        const result = uint8ArrayToBigInt(array)
        expect(result).toBe(value)
      })
    })

    it('bigint -> array -> bigint preserves value for powers of 2 minus 1', () => {
      const values = [
        2n ** 8n - 1n,
        2n ** 16n - 1n,
        2n ** 32n - 1n,
        2n ** 64n - 1n,
        2n ** 128n - 1n,
      ]
      values.forEach((value) => {
        const array = bigIntToUint8Array(value)
        const result = uint8ArrayToBigInt(array)
        expect(result).toBe(value)
      })
    })

    it('array -> bigint -> array preserves array (without leading zeros)', () => {
      const arrays = [
        new Uint8Array([0]),
        new Uint8Array([1]),
        new Uint8Array([255]),
        new Uint8Array([1, 0]),
        new Uint8Array([255, 255]),
        new Uint8Array([1, 2, 3, 4, 5]),
      ]
      arrays.forEach((array) => {
        const bigint = uint8ArrayToBigInt(array)
        const result = bigIntToUint8Array(bigint)
        expect(result).toEqual(array)
      })
    })

    it('array with leading zeros -> bigint -> array removes leading zeros', () => {
      const arrayWithZeros = new Uint8Array([0, 0, 1, 2, 3])
      const bigint = uint8ArrayToBigInt(arrayWithZeros)
      const result = bigIntToUint8Array(bigint)
      expect(result).toEqual(new Uint8Array([1, 2, 3]))
    })

    it('handles random large values correctly', () => {
      // Generate some pseudo-random large bigints
      const randomValues = [
        123456789012345678901234567890n,
        987654321098765432109876543210n,
        111111111111111111111111111111n,
      ]
      randomValues.forEach((value) => {
        const array = bigIntToUint8Array(value)
        const result = uint8ArrayToBigInt(array)
        expect(result).toBe(value)
      })
    })
  })

  describe('bigIntToBase32', () => {
    it('converts zero to the special character "0"', () => {
      const result = bigIntToBase32(0n)
      expect(result).toBe('0')
    })

    it('converts small positive values', () => {
      expect(bigIntToBase32(1n)).toBe('bae')
    })

    it('converts single-byte values', () => {
      expect(bigIntToBase32(255n)).toBe('b74')
    })

    it('converts two-byte values', () => {
      expect(bigIntToBase32(256n)).toBe('baeaa')
      expect(bigIntToBase32(65535n)).toBe('b777q')
    })

    it('converts large values', () => {
      const large = 2n ** 64n - 1n
      expect(bigIntToBase32(large)).toBe('b7777777777776')
    })

    it('converts very large values (256-bit)', () => {
      const veryLarge = 2n ** 256n - 1n
      expect(bigIntToBase32(veryLarge)).toBe(
        'b777777777777777777777777777777777777777777777777777q',
      )
    })

    it('handles powers of 2', () => {
      expect(bigIntToBase32(2n ** 8n)).toBe('baeaa')
      expect(bigIntToBase32(2n ** 16n)).toBe('baeaaa')
      expect(bigIntToBase32(2n ** 32n)).toBe('baeaaaaaa')
      expect(bigIntToBase32(2n ** 64n)).toBe('baeaaaaaaaaaaaaa')
    })

    it('throws error for negative values', () => {
      expect(() => bigIntToBase32(-1n)).toThrow(
        'Cannot convert negative bigint to base32',
      )
      expect(() => bigIntToBase32(-100n)).toThrow()
    })

    it('throws error for non-bigint input', () => {
      expect(() => bigIntToBase32(123)).toThrow(TypeError)
      expect(() => bigIntToBase32('123')).toThrow(TypeError)
      expect(() => bigIntToBase32(null)).toThrow(TypeError)
      expect(() => bigIntToBase32(undefined)).toThrow(TypeError)
      expect(() => bigIntToBase32({})).toThrow(TypeError)
      expect(() => bigIntToBase32([])).toThrow(TypeError)
    })
  })

  describe('base32ToBigInt', () => {
    it('converts base32 strings to BigInt', () => {
      const base32String = bigIntToBase32(12345n)
      const result = base32ToBigInt(base32String)
      expect(typeof result).toBe('bigint')
    })

    it('converts small values correctly', () => {
      const original = 1n
      const base32String = bigIntToBase32(original)
      const result = base32ToBigInt(base32String)
      expect(result).toBe(original)
    })

    it('converts medium values correctly', () => {
      const original = 65535n
      const base32String = bigIntToBase32(original)
      const result = base32ToBigInt(base32String)
      expect(result).toBe(original)
    })

    it('converts large values correctly', () => {
      const original = 2n ** 64n - 1n
      const base32String = bigIntToBase32(original)
      const result = base32ToBigInt(base32String)
      expect(result).toBe(original)
    })

    it('converts very large values correctly', () => {
      const original = 2n ** 256n - 1n
      const base32String = bigIntToBase32(original)
      const result = base32ToBigInt(base32String)
      expect(result).toBe(original)
    })

    it('handles zero value using special character "0"', () => {
      const result = base32ToBigInt('0')
      expect(result).toBe(0n)
    })

    it('throws error for non-string input', () => {
      expect(() => base32ToBigInt(123)).toThrow(TypeError)
      expect(() => base32ToBigInt(123n)).toThrow(TypeError)
      expect(() => base32ToBigInt(null)).toThrow(TypeError)
      expect(() => base32ToBigInt(undefined)).toThrow(TypeError)
      expect(() => base32ToBigInt({})).toThrow(TypeError)
      expect(() => base32ToBigInt([])).toThrow(TypeError)
    })

    it('throws error for invalid base32 strings', () => {
      expect(() => base32ToBigInt('invalid!@#')).toThrow()
      expect(() => base32ToBigInt('not-base32')).toThrow()
    })
  })

  describe('Base32 Round-Trip Conversion', () => {
    const TEST_CASES = [
      // special case
      0n,
      // small numbers
      1n,
      127n,
      128n,
      255n,
      256n,
      65535n,
      65536n,
      // powers of 2
      2n ** 32n,
      2n ** 64n,
      2n ** 128n,
      2n ** 256n,
      // powers of 2 minus 1
      2n ** 8n - 1n,
      2n ** 16n - 1n,
      2n ** 32n - 1n,
      2n ** 64n - 1n,
      2n ** 128n - 1n,
      // random large values
      123456789012345678901234567890n,
      987654321098765432109876543210n,
      111111111111111111111111111111n,
    ]

    for (const tc of TEST_CASES) {
      it(`preserves ${tc} during the round-trip`, () => {
        const base32String = bigIntToBase32(tc)
        const result = base32ToBigInt(base32String)
        expect(result).toBe(tc)
      })
    }
  })
})
