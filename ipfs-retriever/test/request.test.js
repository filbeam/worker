import { describe, it, expect } from 'vitest'
import { parseRequest, checkBotAuthorization } from '../lib/request.js'
import { bigIntToBase32 } from '../lib/bigint-util.js'

const DNS_ROOT = '.filbeam.io'
const BOT_TOKENS = JSON.stringify({ secret: 'bot1' })

describe('parseRequest', () => {
  it('should parse dataSetId and pieceId from a slug URL', () => {
    const dataSetId = '12345'
    const pieceId = '67890'
    const encodedDataSetId = bigIntToBase32(BigInt(dataSetId))
    const encodedPieceId = bigIntToBase32(BigInt(pieceId))
    const slug = `1-${encodedDataSetId}-${encodedPieceId}`

    const request = new Request(`https://${slug}${DNS_ROOT}/`)
    const result = parseRequest(request, { DNS_ROOT, BOT_TOKENS })

    expect(result).toEqual({
      dataSetId,
      pieceId,
      ipfsSubpath: '/',
      ipfsFormat: null,
    })
  })

  it('should parse subpath from URL pathname', () => {
    const dataSetId = '100'
    const pieceId = '200'
    const encodedDataSetId = bigIntToBase32(BigInt(dataSetId))
    const encodedPieceId = bigIntToBase32(BigInt(pieceId))
    const slug = `1-${encodedDataSetId}-${encodedPieceId}`
    const subpath = '/path/to/file.txt'

    const request = new Request(`https://${slug}${DNS_ROOT}${subpath}`)
    const result = parseRequest(request, { DNS_ROOT, BOT_TOKENS })

    expect(result).toEqual({
      dataSetId,
      pieceId,
      ipfsSubpath: subpath,
      ipfsFormat: null,
    })
  })

  it('should default to "/" for empty pathname', () => {
    const dataSetId = '999'
    const pieceId = '888'
    const encodedDataSetId = bigIntToBase32(BigInt(dataSetId))
    const encodedPieceId = bigIntToBase32(BigInt(pieceId))
    const slug = `1-${encodedDataSetId}-${encodedPieceId}`

    const request = new Request(`https://${slug}${DNS_ROOT}`)
    const result = parseRequest(request, { DNS_ROOT, BOT_TOKENS })

    expect(result).toEqual({
      dataSetId,
      pieceId,
      ipfsSubpath: '/',
      ipfsFormat: null,
    })
  })

  it('should handle zero values for dataSetId and pieceId', () => {
    const slug = '1-0-0'

    const request = new Request(`https://${slug}${DNS_ROOT}/`)
    const result = parseRequest(request, { DNS_ROOT, BOT_TOKENS })

    expect(result).toEqual({
      dataSetId: '0',
      pieceId: '0',
      ipfsSubpath: '/',
      ipfsFormat: null,
    })
  })

  it('should return descriptive error for invalid hostname format - missing parts', () => {
    const request = new Request(`https://1-abc${DNS_ROOT}/`)
    expect(() => parseRequest(request, { DNS_ROOT, BOT_TOKENS })).toThrowError(
      `The hostname must be in the format: 1-{dataSetId}-{pieceId}${DNS_ROOT}`,
    )
  })

  it('should return descriptive error for invalid hostname format - too many parts', () => {
    const request = new Request(`https://1-abc-def-ghi${DNS_ROOT}/`)
    expect(() => parseRequest(request, { DNS_ROOT, BOT_TOKENS })).toThrowError(
      `The hostname must be in the format: 1-{dataSetId}-{pieceId}${DNS_ROOT}`,
    )
  })

  it('should return descriptive error for invalid hostname format - no dashes', () => {
    const request = new Request(`https://1abc${DNS_ROOT}/`)
    expect(() => parseRequest(request, { DNS_ROOT, BOT_TOKENS })).toThrowError(
      `The hostname must be in the format: 1-{dataSetId}-{pieceId}${DNS_ROOT}`,
    )
  })

  it('should return descriptive error for missing dataSetId', () => {
    const request = new Request(`https://1--abc${DNS_ROOT}/`)
    expect(() => parseRequest(request, { DNS_ROOT, BOT_TOKENS })).toThrowError(
      `The hostname must be in the format: 1-{dataSetId}-{pieceId}${DNS_ROOT}`,
    )
  })

  it('should return descriptive error for missing pieceId', () => {
    const request = new Request(`https://1-abc-${DNS_ROOT}/`)
    expect(() => parseRequest(request, { DNS_ROOT, BOT_TOKENS })).toThrowError(
      `The hostname must be in the format: 1-{dataSetId}-{pieceId}${DNS_ROOT}`,
    )
  })

  it('should return descriptive error for unsupported version', () => {
    const dataSetId = '12345'
    const pieceId = '67890'
    const encodedDataSetId = bigIntToBase32(BigInt(dataSetId))
    const encodedPieceId = bigIntToBase32(BigInt(pieceId))
    const slug = `2-${encodedDataSetId}-${encodedPieceId}`

    const request = new Request(`https://${slug}${DNS_ROOT}/`)
    expect(() => parseRequest(request, { DNS_ROOT, BOT_TOKENS })).toThrowError(
      'Unsupported slug version: 2. Expected version 1.',
    )
  })

  it('should return descriptive error for invalid base32 dataSetId', () => {
    const request = new Request(`https://1-invalid1-aeete${DNS_ROOT}/`)
    expect(() => parseRequest(request, { DNS_ROOT, BOT_TOKENS })).toThrowError(
      /Invalid dataSetId encoding in slug: invalid1/,
    )
  })

  it('should return descriptive error for invalid base32 pieceId', () => {
    const request = new Request(`https://1-ga4q-invalid1${DNS_ROOT}/`)
    expect(() => parseRequest(request, { DNS_ROOT, BOT_TOKENS })).toThrowError(
      /Invalid pieceId encoding in slug: invalid1/,
    )
  })

  it('should return error for wrong DNS root', () => {
    const dataSetId = '12345'
    const pieceId = '67890'
    const encodedDataSetId = bigIntToBase32(BigInt(dataSetId))
    const encodedPieceId = bigIntToBase32(BigInt(pieceId))
    const slug = `1-${encodedDataSetId}-${encodedPieceId}`

    const request = new Request(`https://${slug}.wrong.io/`)
    expect(() => parseRequest(request, { DNS_ROOT, BOT_TOKENS })).toThrowError(
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

    const request = new Request(`https://${slug}${DNS_ROOT}${subpath}?foo=bar&baz=qux`)
    const result = parseRequest(request, { DNS_ROOT, BOT_TOKENS })

    expect(result).toEqual({
      dataSetId,
      pieceId,
      ipfsSubpath: subpath,
      ipfsFormat: null,
    })
  })

  it('should parse format=car from URL with subpath', () => {
    const dataSetId = '100'
    const pieceId = '200'
    const encodedDataSetId = bigIntToBase32(BigInt(dataSetId))
    const encodedPieceId = bigIntToBase32(BigInt(pieceId))
    const slug = `1-${encodedDataSetId}-${encodedPieceId}`
    const subpath = '/path/to/file.txt'

    const request = new Request(`https://${slug}${DNS_ROOT}${subpath}?format=car`)
    const result = parseRequest(request, { DNS_ROOT, BOT_TOKENS })

    expect(result).toEqual({
      dataSetId,
      pieceId,
      ipfsSubpath: subpath,
      ipfsFormat: 'car',
    })
  })

  it('should parse any format value from URL', () => {
    const dataSetId = '12345'
    const pieceId = '67890'
    const encodedDataSetId = bigIntToBase32(BigInt(dataSetId))
    const encodedPieceId = bigIntToBase32(BigInt(pieceId))
    const slug = `1-${encodedDataSetId}-${encodedPieceId}`

    const request = new Request(`https://${slug}${DNS_ROOT}/?format=raw`)
    const result = parseRequest(request, { DNS_ROOT, BOT_TOKENS })

    expect(result).toEqual({
      dataSetId,
      pieceId,
      ipfsSubpath: '/',
      ipfsFormat: 'raw',
    })
  })

  it('should return null for ipfsFormat when format parameter is not present', () => {
    const dataSetId = '12345'
    const pieceId = '67890'
    const encodedDataSetId = bigIntToBase32(BigInt(dataSetId))
    const encodedPieceId = bigIntToBase32(BigInt(pieceId))
    const slug = `1-${encodedDataSetId}-${encodedPieceId}`

    const request = new Request(`https://${slug}${DNS_ROOT}/file.txt`)
    const result = parseRequest(request, { DNS_ROOT, BOT_TOKENS })

    expect(result).toEqual({
      dataSetId,
      pieceId,
      ipfsSubpath: '/file.txt',
      ipfsFormat: null,
    })
  })

  it('should preserve trailing slash in subpath', () => {
    const dataSetId = '12345'
    const pieceId = '67890'
    const encodedDataSetId = bigIntToBase32(BigInt(dataSetId))
    const encodedPieceId = bigIntToBase32(BigInt(pieceId))
    const slug = `1-${encodedDataSetId}-${encodedPieceId}`
    const subpath = '/directory/'

    const request = new Request(`https://${slug}${DNS_ROOT}${subpath}`)
    const result = parseRequest(request, { DNS_ROOT, BOT_TOKENS })

    expect(result).toEqual({
      dataSetId,
      pieceId,
      ipfsSubpath: subpath,
      ipfsFormat: null,
    })
  })

  it('should handle encoded characters in subpath', () => {
    const dataSetId = '12345'
    const pieceId = '67890'
    const encodedDataSetId = bigIntToBase32(BigInt(dataSetId))
    const encodedPieceId = bigIntToBase32(BigInt(pieceId))
    const slug = `1-${encodedDataSetId}-${encodedPieceId}`
    const subpath = '/file%20with%20spaces.txt'

    const request = new Request(`https://${slug}${DNS_ROOT}${subpath}`)
    const result = parseRequest(request, { DNS_ROOT, BOT_TOKENS })

    expect(result).toEqual({
      dataSetId,
      pieceId,
      ipfsSubpath: subpath,
      ipfsFormat: null,
    })
  })

  it('should handle large BigInt values', () => {
    const dataSetId = '999999999999999999'
    const pieceId = '888888888888888888'
    const encodedDataSetId = bigIntToBase32(BigInt(dataSetId))
    const encodedPieceId = bigIntToBase32(BigInt(pieceId))
    const slug = `1-${encodedDataSetId}-${encodedPieceId}`

    const request = new Request(`https://${slug}${DNS_ROOT}/`)
    const result = parseRequest(request, { DNS_ROOT, BOT_TOKENS })

    expect(result).toEqual({
      dataSetId,
      pieceId,
      ipfsSubpath: '/',
      ipfsFormat: null,
    })
  })
})

describe('checkBotAuthorization', () => {
  it('should return undefined when no authorization header is present', () => {
    const request = new Request('https://example.com', {
      headers: {},
    })
    const result = checkBotAuthorization(request, { BOT_TOKENS })
    expect(result).toBeUndefined()
  })

  it('should throw 401 error when authorization header is not Bearer format', () => {
    const request = new Request('https://example.com', {
      headers: { authorization: 'Basic sometoken' },
    })
    expect(() => checkBotAuthorization(request, { BOT_TOKENS })).toThrowError(
      'Unauthorized: Authorization header must use Bearer scheme',
    )
  })

  it('should throw 401 error when authorization header has no token after Bearer', () => {
    const request = new Request('https://example.com', {
      headers: { authorization: 'Bearer' },
    })
    expect(() => checkBotAuthorization(request, { BOT_TOKENS })).toThrowError(
      'Unauthorized: Authorization header must use Bearer scheme',
    )
  })

  it('should throw 401 error when token is not in BOT_TOKENS list', () => {
    const request = new Request('https://example.com', {
      headers: { authorization: 'Bearer invalid_token' },
    })
    expect(() => checkBotAuthorization(request, { BOT_TOKENS })).toThrowError(
      'Unauthorized: Invalid Access Token i...n',
    )
  })

  it('should return token prefix when valid token is provided', () => {
    const request = new Request('https://example.com', {
      headers: { authorization: 'Bearer secret' },
    })
    const result = checkBotAuthorization(request, {
      BOT_TOKENS: JSON.stringify({
        secret: 'bot1',
        secret_2: 'bot2',
      }),
    })
    expect(result).toBe('bot1')
  })
})
