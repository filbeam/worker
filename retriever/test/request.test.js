import { describe, it, expect } from 'vitest'
import { parseRequest } from '../lib/request.js'

const DNS_ROOT = '.filbeam.io'
const TEST_WALLET = 'abc123'
const TEST_CID = 'baga123'

describe('parseRequest', () => {
  it('should parse payerWalletAddress and pieceCid from a URL with both params', () => {
    const request = { url: `https://${TEST_WALLET}${DNS_ROOT}/${TEST_CID}` }
    const result = parseRequest(request, { DNS_ROOT })
    expect(result).toEqual({
      payerWalletAddress: TEST_WALLET,
      pieceCid: TEST_CID,
    })
  })

  it('should parse payerWalletAddress and pieceCid from a URL with leading slash', () => {
    const request = { url: `https://${TEST_WALLET}${DNS_ROOT}//${TEST_CID}` }
    const result = parseRequest(request, { DNS_ROOT })
    expect(result).toEqual({
      payerWalletAddress: TEST_WALLET,
      pieceCid: TEST_CID,
    })
  })

  it('should return descriptive error for missing pieceCid', () => {
    const request = { url: `https://${TEST_WALLET}${DNS_ROOT}/` }
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError(
      'Missing required path element: `/{CID}`',
    )
  })

  it('should return undefined for both if no params in path', () => {
    const request = { url: 'https://filbeam.io' }
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError(
      'Invalid hostname: filbeam.io. It must end with .filbeam.io.',
    )
  })

  it('should ignore query parameters', () => {
    const request = {
      url: `https://${TEST_WALLET}${DNS_ROOT}/${TEST_CID}?foo=bar`,
    }
    const result = parseRequest(request, { DNS_ROOT })
    expect(result).toEqual({
      payerWalletAddress: TEST_WALLET,
      pieceCid: TEST_CID,
    })
  })
})
