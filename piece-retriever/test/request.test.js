import { describe, it, expect } from 'vitest'
import { parseRequest, checkBotAuthorization } from '../lib/request.js'

const DNS_ROOT = '.filbeam.io'
const TEST_WALLET = 'abc123'
const TEST_CID = 'baga123'
const BOT_TOKENS = JSON.stringify({ secret: 'bot1' })

describe('parseRequest', () => {
  it('should parse payerWalletAddress and pieceCid from a URL with both params', () => {
    const request = new Request(`https://${TEST_WALLET}${DNS_ROOT}/${TEST_CID}`)
    const result = parseRequest(request, { DNS_ROOT, BOT_TOKENS })
    expect(result).toEqual({
      payerWalletAddress: TEST_WALLET,
      pieceCid: TEST_CID,
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

  it('should ignore query parameters', () => {
    const request = new Request(
      `https://${TEST_WALLET}${DNS_ROOT}/${TEST_CID}?foo=bar`,
    )
    const result = parseRequest(request, { DNS_ROOT, BOT_TOKENS })
    expect(result).toEqual({
      payerWalletAddress: TEST_WALLET,
      pieceCid: TEST_CID,
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
