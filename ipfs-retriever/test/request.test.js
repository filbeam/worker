import { describe, it, expect } from 'vitest'
import { parseRequest } from '../lib/request.js'
import { bigIntToBase32 } from '../lib/bigint-util.js'

const DNS_ROOT = '.filbeam.io'

describe('parseRequest', () => {
  it('should parse dataSetId and pieceId from a slug URL', () => {
    const dataSetId = '12345'
    const pieceId = '67890'
    const encodedDataSetId = bigIntToBase32(BigInt(dataSetId))
    const encodedPieceId = bigIntToBase32(BigInt(pieceId))
    const slug = `1-${encodedDataSetId}-${encodedPieceId}`

    const request = { url: `https://${slug}${DNS_ROOT}/` }
    const result = parseRequest(request, { DNS_ROOT })

    expect(result).toEqual({
      dataSetId,
      pieceId,
      ipfsSubpath: '/',
    })
  })

  it('should parse subpath from URL pathname', () => {
    const dataSetId = '100'
    const pieceId = '200'
    const encodedDataSetId = bigIntToBase32(BigInt(dataSetId))
    const encodedPieceId = bigIntToBase32(BigInt(pieceId))
    const slug = `1-${encodedDataSetId}-${encodedPieceId}`
    const subpath = '/path/to/file.txt'

    const request = { url: `https://${slug}${DNS_ROOT}${subpath}` }
    const result = parseRequest(request, { DNS_ROOT })

    expect(result).toEqual({
      dataSetId,
      pieceId,
      ipfsSubpath: subpath,
    })
  })

  it('should default to "/" for empty pathname', () => {
    const dataSetId = '999'
    const pieceId = '888'
    const encodedDataSetId = bigIntToBase32(BigInt(dataSetId))
    const encodedPieceId = bigIntToBase32(BigInt(pieceId))
    const slug = `1-${encodedDataSetId}-${encodedPieceId}`

    const request = { url: `https://${slug}${DNS_ROOT}` }
    const result = parseRequest(request, { DNS_ROOT })

    expect(result).toEqual({
      dataSetId,
      pieceId,
      ipfsSubpath: '/',
    })
  })

  it('should handle zero values for dataSetId and pieceId', () => {
    const slug = '1-0-0'

    const request = { url: `https://${slug}${DNS_ROOT}/` }
    const result = parseRequest(request, { DNS_ROOT })

    expect(result).toEqual({
      dataSetId: '0',
      pieceId: '0',
      ipfsSubpath: '/',
    })
  })

  it('should return descriptive error for invalid hostname format - missing parts', () => {
    const request = { url: `https://1-abc${DNS_ROOT}/` }
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError(
      `The hostname must be in the format: {version}-{dataSetId}-{pieceId}${DNS_ROOT}`,
    )
  })

  it('should return descriptive error for invalid hostname format - too many parts', () => {
    const request = { url: `https://1-abc-def-ghi${DNS_ROOT}/` }
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError(
      `The hostname must be in the format: {version}-{dataSetId}-{pieceId}${DNS_ROOT}`,
    )
  })

  it('should return descriptive error for invalid hostname format - no dashes', () => {
    const request = { url: `https://1abc${DNS_ROOT}/` }
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError(
      `The hostname must be in the format: {version}-{dataSetId}-{pieceId}${DNS_ROOT}`,
    )
  })

  it('should return descriptive error for missing dataSetId', () => {
    const request = { url: `https://1--abc${DNS_ROOT}/` }
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError(
      `The hostname must be in the format: {version}-{dataSetId}-{pieceId}${DNS_ROOT}`,
    )
  })

  it('should return descriptive error for missing pieceId', () => {
    const request = { url: `https://1-abc-${DNS_ROOT}/` }
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError(
      `The hostname must be in the format: {version}-{dataSetId}-{pieceId}${DNS_ROOT}`,
    )
  })

  it('should return descriptive error for unsupported version', () => {
    const dataSetId = '12345'
    const pieceId = '67890'
    const encodedDataSetId = bigIntToBase32(BigInt(dataSetId))
    const encodedPieceId = bigIntToBase32(BigInt(pieceId))
    const slug = `2-${encodedDataSetId}-${encodedPieceId}`

    const request = { url: `https://${slug}${DNS_ROOT}/` }
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError(
      'Unsupported slug version: 2. Expected version 1.',
    )
  })

  it('should return descriptive error for invalid base32 dataSetId', () => {
    const request = { url: `https://1-notbase32-baeete${DNS_ROOT}/` }
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError(
      /Invalid dataSetId encoding in slug: notbase32/,
    )
  })

  it('should return descriptive error for invalid base32 pieceId', () => {
    const request = { url: `https://1-bga4q-notbase32${DNS_ROOT}/` }
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError(
      /Invalid pieceId encoding in slug: notbase32/,
    )
  })

  it('should return error for wrong DNS root', () => {
    const dataSetId = '12345'
    const pieceId = '67890'
    const encodedDataSetId = bigIntToBase32(BigInt(dataSetId))
    const encodedPieceId = bigIntToBase32(BigInt(pieceId))
    const slug = `1-${encodedDataSetId}-${encodedPieceId}`

    const request = { url: `https://${slug}.wrong.io/` }
    expect(() => parseRequest(request, { DNS_ROOT })).toThrowError(
      `Invalid hostname: ${slug}.wrong.io. It must end with ${DNS_ROOT}.`,
    )
  })

  it('should ignore query parameters', () => {
    const dataSetId = '12345'
    const pieceId = '67890'
    const encodedDataSetId = bigIntToBase32(BigInt(dataSetId))
    const encodedPieceId = bigIntToBase32(BigInt(pieceId))
    const slug = `1-${encodedDataSetId}-${encodedPieceId}`
    const subpath = '/file.txt'

    const request = {
      url: `https://${slug}${DNS_ROOT}${subpath}?foo=bar&baz=qux`,
    }
    const result = parseRequest(request, { DNS_ROOT })

    expect(result).toEqual({
      dataSetId,
      pieceId,
      ipfsSubpath: subpath,
    })
  })

  it('should preserve trailing slash in subpath', () => {
    const dataSetId = '12345'
    const pieceId = '67890'
    const encodedDataSetId = bigIntToBase32(BigInt(dataSetId))
    const encodedPieceId = bigIntToBase32(BigInt(pieceId))
    const slug = `1-${encodedDataSetId}-${encodedPieceId}`
    const subpath = '/directory/'

    const request = {
      url: `https://${slug}${DNS_ROOT}${subpath}`,
    }
    const result = parseRequest(request, { DNS_ROOT })

    expect(result).toEqual({
      dataSetId,
      pieceId,
      ipfsSubpath: subpath,
    })
  })

  it('should handle encoded characters in subpath', () => {
    const dataSetId = '12345'
    const pieceId = '67890'
    const encodedDataSetId = bigIntToBase32(BigInt(dataSetId))
    const encodedPieceId = bigIntToBase32(BigInt(pieceId))
    const slug = `1-${encodedDataSetId}-${encodedPieceId}`
    const subpath = '/file%20with%20spaces.txt'

    const request = {
      url: `https://${slug}${DNS_ROOT}${subpath}`,
    }
    const result = parseRequest(request, { DNS_ROOT })

    expect(result).toEqual({
      dataSetId,
      pieceId,
      ipfsSubpath: subpath,
    })
  })

  it('should handle large BigInt values', () => {
    const dataSetId = '999999999999999999'
    const pieceId = '888888888888888888'
    const encodedDataSetId = bigIntToBase32(BigInt(dataSetId))
    const encodedPieceId = bigIntToBase32(BigInt(pieceId))
    const slug = `1-${encodedDataSetId}-${encodedPieceId}`

    const request = { url: `https://${slug}${DNS_ROOT}/` }
    const result = parseRequest(request, { DNS_ROOT })

    expect(result).toEqual({
      dataSetId,
      pieceId,
      ipfsSubpath: '/',
    })
  })
})
