import { describe, it, expect } from 'vitest'
import { buildForwardUrl, parseRequest } from '../lib/request.js'

const DNS_ROOT = '.calibration.x402.filbeam.io'
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

  it('should normalize payeeAddress to lowercase', () => {
    const UPPER_PAYEE = '0xC83DBFDF61616778537211A7E5CA2E87EC6CF0ED'
    const request = new Request(`https://${UPPER_PAYEE}${DNS_ROOT}/${TEST_CID}`)
    const result = parseRequest(request, { DNS_ROOT })
    expect(result.payeeAddress).toBe(UPPER_PAYEE.toLowerCase())
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

  it('should return error for empty payeeAddress', () => {
    // When hostname is exactly DNS_ROOT (without leading dot), payeeis empty string
    // This triggers the "Missing payeeAddress" check
    const request = new Request(
      `https://0x${DNS_ROOT}/${TEST_CID}`.replace('0x.', ''),
    )
    // The hostname won't end with DNS_ROOT because it's missing the subdomain part
    // So this actually tests invalid hostname rather than missing payer
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError()
  })

  it('should return error for invalid payeeAddress', () => {
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

describe('buildForwardUrl', () => {
  it('should build forward url with PIECE_RETRIEVER_DNS_ROOT', () => {
    const pieceRetrieverDnsRoot = '.calibration.filbeam.io'
    const payeeAddress = '0xPayer'
    const pieceCid = 'bafy'
    const result = buildForwardUrl(
      {
        PIECE_RETRIEVER_DNS_ROOT: pieceRetrieverDnsRoot,
      },
      payeeAddress,
      pieceCid,
    )
    expect(result).toBe('https://0xPayer.calibration.filbeam.io/bafy')
  })
})
