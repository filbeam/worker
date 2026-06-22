import { describe, it, expect } from 'vitest'
import { checkBotAuthorization } from '../lib/bot-auth.js'

const BOT_TOKENS = JSON.stringify({ secret: 'bot1', secret_2: 'bot2' })

describe('checkBotAuthorization', () => {
  it('returns undefined when no authorization header is present', () => {
    const request = new Request('https://example.com', { headers: {} })
    expect(checkBotAuthorization(request, { BOT_TOKENS })).toBeUndefined()
  })

  it('throws 401 when the authorization header is not Bearer format', () => {
    const request = new Request('https://example.com', {
      headers: { authorization: 'Basic sometoken' },
    })
    expect(() => checkBotAuthorization(request, { BOT_TOKENS })).toThrowError(
      'Unauthorized: Authorization header must use Bearer scheme',
    )
  })

  it('throws 401 when there is no token after Bearer', () => {
    const request = new Request('https://example.com', {
      headers: { authorization: 'Bearer' },
    })
    expect(() => checkBotAuthorization(request, { BOT_TOKENS })).toThrowError(
      'Unauthorized: Authorization header must use Bearer scheme',
    )
  })

  it('throws 401 when the token is not in BOT_TOKENS', () => {
    const request = new Request('https://example.com', {
      headers: { authorization: 'Bearer invalid_token' },
    })
    expect(() => checkBotAuthorization(request, { BOT_TOKENS })).toThrowError(
      'Unauthorized: Invalid Access Token i...n',
    )
  })

  it('returns the bot name for a valid token', () => {
    const request = new Request('https://example.com', {
      headers: { authorization: 'Bearer secret' },
    })
    expect(checkBotAuthorization(request, { BOT_TOKENS })).toBe('bot1')
  })
})
