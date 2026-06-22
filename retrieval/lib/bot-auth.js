import { httpAssert } from './http-assert.js'

/**
 * Resolves the bot name for a request's Bearer token, or `undefined` for
 * anonymous requests. Throws a 401 for a malformed Authorization header or an
 * unknown token.
 *
 * @param {Request} request
 * @param {object} args
 * @param {string} args.BOT_TOKENS - JSON object mapping access token to bot
 *   name.
 * @returns {string | undefined} Bot name or the access token
 */
export function checkBotAuthorization(request, { BOT_TOKENS }) {
  const botTokens = JSON.parse(BOT_TOKENS)

  const auth = request.headers.get('authorization')
  if (!auth) return undefined

  const [prefix, token, ...rest] = auth.split(' ')

  httpAssert(
    prefix === 'Bearer' && token && rest.length === 0,
    401,
    'Unauthorized: Authorization header must use Bearer scheme',
  )

  httpAssert(
    token in botTokens,
    401,
    `Unauthorized: Invalid Access Token ${token.slice(0, 1)}...${token.slice(-1)}`,
  )

  return botTokens[token]
}
