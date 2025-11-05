import { describe, it, expect } from 'vitest'
import { isValidEthereumAddress } from '../lib/address.js'

describe('isValidEthereumAddress', () => {
  /** @type {{ name: string; input: string; expected: boolean }[]} */
  const cases = [
    {
      name: 'valid lowercase address',
      input: '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
      expected: true,
    },
    {
      name: 'valid uppercase address',
      input: '0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
      expected: true,
    },
    {
      name: 'valid mixed-case address',
      input: '0xAaBbCcDdEeFf00112233445566778899AaBbCcDd',
      expected: true,
    },
    {
      name: 'address without 0x prefix',
      input: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
      expected: false,
    },
    {
      name: 'address with less than 40 hex chars',
      input: '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
      expected: false,
    },
    {
      name: 'address with more than 40 hex chars',
      input: '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
      expected: false,
    },
    {
      name: 'address with invalid characters',
      input: '0xZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ',
      expected: false,
    },
    {
      name: 'empty string',
      input: '',
      expected: false,
    },
    {
      // @ts-expect-error
      input: null,
      expected: false,
    },
    {
      name: 'undefined',
      // @ts-expect-error
      input: undefined,
      expected: false,
    },
  ]

  cases.forEach(({ name, input, expected }) => {
    it(`returns ${expected} for ${name}`, () => {
      expect(isValidEthereumAddress(input)).toBe(expected)
    })
  })
})
