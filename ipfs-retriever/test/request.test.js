import { describe, it, expect } from 'vitest'
import { parseRequest } from '../lib/request.js'

const DNS_ROOT = '.filbeam.io'
const TEST_WALLET = '0xabc123def456'
const TEST_CID = 'bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi'

describe('parseRequest', () => {
  it('should parse payerWalletAddress and ipfsRootCid from a URL with both params', () => {
    const request = { url: `https://${TEST_CID}-${TEST_WALLET}${DNS_ROOT}/` }
    const result = parseRequest(request, { DNS_ROOT })
    expect(result).toEqual({
      payerWalletAddress: TEST_WALLET,
      ipfsRootCid: TEST_CID,
      ipfsSubpath: '/',
    })
  })

  it('should parse subpath from URL pathname', () => {
    const subpath = '/path/to/file.txt'
    const request = {
      url: `https://${TEST_CID}-${TEST_WALLET}${DNS_ROOT}${subpath}`,
    }
    const result = parseRequest(request, { DNS_ROOT })
    expect(result).toEqual({
      payerWalletAddress: TEST_WALLET,
      ipfsRootCid: TEST_CID,
      ipfsSubpath: subpath,
    })
  })

  it('should default to "/" for empty pathname', () => {
    const request = { url: `https://${TEST_CID}-${TEST_WALLET}${DNS_ROOT}` }
    const result = parseRequest(request, { DNS_ROOT })
    expect(result).toEqual({
      payerWalletAddress: TEST_WALLET,
      ipfsRootCid: TEST_CID,
      ipfsSubpath: '/',
    })
  })

  it('should return descriptive error for invalid hostname format - missing dash', () => {
    const request = { url: `https://${TEST_CID}${TEST_WALLET}${DNS_ROOT}/` }
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError(
      `The hostname must be in the format: {IpfsRootCID}-{PayerWalletAddress}${DNS_ROOT}`,
    )
  })

  it('should return descriptive error for invalid hostname format - missing CID', () => {
    const request = { url: `https://-${TEST_WALLET}${DNS_ROOT}/` }
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError(
      `The hostname must be in the format: {IpfsRootCID}-{PayerWalletAddress}${DNS_ROOT}`,
    )
  })

  it('should return descriptive error for invalid hostname format - missing wallet', () => {
    const request = { url: `https://${TEST_CID}-${DNS_ROOT}/` }
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError(
      `The hostname must be in the format: {IpfsRootCID}-{PayerWalletAddress}${DNS_ROOT}`,
    )
  })

  it('should return error for wrong DNS root', () => {
    const request = { url: `https://${TEST_CID}-${TEST_WALLET}.wrong.io/` }
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError(
      `Invalid hostname: ${TEST_CID}-${TEST_WALLET}.wrong.io. It must end with ${DNS_ROOT}.`,
    )
  })

  it('should ignore query parameters', () => {
    const subpath = '/file.txt'
    const request = {
      url: `https://${TEST_CID}-${TEST_WALLET}${DNS_ROOT}${subpath}?foo=bar&baz=qux`,
    }
    const result = parseRequest(request, { DNS_ROOT })
    expect(result).toEqual({
      payerWalletAddress: TEST_WALLET,
      ipfsRootCid: TEST_CID,
      ipfsSubpath: subpath,
    })
  })

  it('should preserve trailing slash in subpath', () => {
    const subpath = '/directory/'
    const request = {
      url: `https://${TEST_CID}-${TEST_WALLET}${DNS_ROOT}${subpath}`,
    }
    const result = parseRequest(request, { DNS_ROOT })
    expect(result).toEqual({
      payerWalletAddress: TEST_WALLET,
      ipfsRootCid: TEST_CID,
      ipfsSubpath: subpath,
    })
  })

  it('should handle encoded characters in subpath', () => {
    const subpath = '/file%20with%20spaces.txt'
    const request = {
      url: `https://${TEST_CID}-${TEST_WALLET}${DNS_ROOT}${subpath}`,
    }
    const result = parseRequest(request, { DNS_ROOT })
    expect(result).toEqual({
      payerWalletAddress: TEST_WALLET,
      ipfsRootCid: TEST_CID,
      ipfsSubpath: subpath,
    })
  })
})
