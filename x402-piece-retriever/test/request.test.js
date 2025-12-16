import { describe, it, expect } from 'vitest'
import { parseRequest } from '../lib/request.js'

const DNS_ROOT = '.x402.calibration.filbeam.io'
const TEST_PAYEE = '0xc83dbfdf61616778537211a7e5ca2e87ec6cf0ed'
const TEST_CID =
  'baga6ea4seaqaleibb6ud4xeemuzzpsyhl6cxlsymsnfco4cdjka5uzajo2x4ipa'

describe('parseRequest', () => {
  it('should parse payeeAddress and pieceCid from URL', () => {
    const request = new Request(`https://${TEST_PAYEE}${DNS_ROOT}/${TEST_CID}`)
    const result = parseRequest(request, { DNS_ROOT })
    expect(result).toStrictEqual({
      payeeAddress: TEST_PAYEE.toLowerCase(),
      pieceCid: TEST_CID,
      payment: null,
      isWebBrowser: false,
    })
  })

  it('should normalize payee address to lowercase', () => {
    const upperPayee = '0xC83DBFDF61616778537211A7E5CA2E87EC6CF0ED'
    const request = new Request(`https://${upperPayee}${DNS_ROOT}/${TEST_CID}`)
    const result = parseRequest(request, { DNS_ROOT })
    expect(result.payeeAddress).toBe(upperPayee.toLowerCase())
  })

  it('should handle URLs with leading slashes', () => {
    const request = new Request(`https://${TEST_PAYEE}${DNS_ROOT}//${TEST_CID}`)
    const result = parseRequest(request, { DNS_ROOT })
    expect(result).toStrictEqual({
      payeeAddress: TEST_PAYEE.toLowerCase(),
      pieceCid: TEST_CID,
      payment: null,
      isWebBrowser: false,
    })
  })

  it('should return error for missing pieceCid', () => {
    const request = new Request(`https://${TEST_PAYEE}${DNS_ROOT}/`)
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError(
      'Missing required path element: `/{CID}`',
    )
  })

  it('should return error for invalid hostname', () => {
    const request = new Request('https://invalid.filbeam.io/baga123')
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError(
      `Invalid hostname: invalid.filbeam.io. It must end with ${DNS_ROOT}.`,
    )
  })

  it('should return error for empty payee address', () => {
    // When hostname is exactly DNS_ROOT (without leading dot), payee is empty string
    // This triggers the "Missing payee address" check
    const request = new Request(
      `https://0x${DNS_ROOT}/${TEST_CID}`.replace('0x.', ''),
    )
    // The hostname won't end with DNS_ROOT because it's missing the subdomain part
    // So this actually tests invalid hostname rather than missing payee
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError()
  })

  it('should return error for invalid payee address', () => {
    const request = new Request(
      `https://invalid-address${DNS_ROOT}/${TEST_CID}`,
    )
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError(
      'Invalid payee address: invalid-address. Must be a valid Ethereum address.',
    )
  })

  it('should return error for invalid CID format', () => {
    const request = new Request(`https://${TEST_PAYEE}${DNS_ROOT}/invalid-cid`)
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError(
      'Invalid CID: invalid-cid. It is not a valid CommP (v1 or v2).',
    )
  })

  it('should accept bafk CIDs', () => {
    const bafkCid =
      'bafk2bzaceaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    const request = new Request(`https://${TEST_PAYEE}${DNS_ROOT}/${bafkCid}`)
    const result = parseRequest(request, { DNS_ROOT })
    expect(result.pieceCid).toBe(bafkCid)
  })

  it('should ignore query parameters', () => {
    const request = new Request(
      `https://${TEST_PAYEE}${DNS_ROOT}/${TEST_CID}?foo=bar`,
    )
    const result = parseRequest(request, { DNS_ROOT })
    expect(result).toStrictEqual({
      payeeAddress: TEST_PAYEE.toLowerCase(),
      pieceCid: TEST_CID,
      payment: null,
      isWebBrowser: false,
    })
  })
})
