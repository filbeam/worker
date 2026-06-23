import { describe, it, expect } from 'vitest'
import { parseRequest } from '../lib/request.js'

const DNS_ROOT = '.filbeam.io'
const TEST_WALLET = '0x1234567890abcdef1234567890abcdef12345678'
const TEST_CID = 'baga123'
const BOT_TOKENS = JSON.stringify({ secret: 'bot1' })

describe('parseRequest', () => {
  it('should parse payerWalletAddress and pieceCid from a URL with both params', () => {
    const request = new Request(`https://${TEST_WALLET}${DNS_ROOT}/${TEST_CID}`)
    const result = parseRequest(request, { DNS_ROOT, BOT_TOKENS })
    expect(result).toEqual({
      payerWalletAddress: TEST_WALLET,
      pieceCid: TEST_CID,
      validateCacheMissResponse: false,
    })
  })

  it('should parse payerWalletAddress and pieceCid from a URL with leading slash', () => {
    const request = new Request(
      `https://${TEST_WALLET}${DNS_ROOT}//${TEST_CID}`,
    )
    const result = parseRequest(request, { DNS_ROOT, BOT_TOKENS })
    expect(result).toEqual({
      payerWalletAddress: TEST_WALLET,
      pieceCid: TEST_CID,
      validateCacheMissResponse: false,
    })
  })

  it('should return descriptive error for missing pieceCid', () => {
    const request = new Request(`https://${TEST_WALLET}${DNS_ROOT}/`)
    expect(() => parseRequest(request, { DNS_ROOT, BOT_TOKENS })).toThrowError(
      'Missing required path element: `/{CID}`',
    )
  })

  it('should return undefined for both if no params in path', () => {
    const request = new Request('https://filbeam.io')
    expect(() => parseRequest(request, { DNS_ROOT, BOT_TOKENS })).toThrowError(
      'Invalid hostname: filbeam.io. It must end with .filbeam.io.',
    )
  })

  it('throws for an invalid payer wallet address', () => {
    const request = new Request(`https://notanaddress${DNS_ROOT}/${TEST_CID}`)
    expect(() => parseRequest(request, { DNS_ROOT, BOT_TOKENS })).toThrowError(
      'Invalid address: notanaddress. Address must be a valid ethereum address.',
    )
  })

  it('should ignore query parameters', () => {
    const request = new Request(
      `https://${TEST_WALLET}${DNS_ROOT}/${TEST_CID}?foo=bar`,
    )
    const result = parseRequest(request, { DNS_ROOT, BOT_TOKENS })
    expect(result).toEqual({
      payerWalletAddress: TEST_WALLET,
      pieceCid: TEST_CID,
      validateCacheMissResponse: false,
    })
  })
})
