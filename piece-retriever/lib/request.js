import { httpAssert } from './http-assert.js'

/**
 * Parse params found in path of the request URL
 *
 * @param {Request} request
 * @param {object} options
 * @param {string} options.DNS_ROOT
 * @param {string} options.BOT_TOKENS
 * @returns {{
 *   payerWalletAddress?: string
 *   pieceCid?: string
 *   botName?: string
 * }}
 */
export function parseRequest(request, { DNS_ROOT, BOT_TOKENS }) {
  const url = new URL(request.url)
  console.log('retrieval request', { DNS_ROOT, url })

  httpAssert(
    url.hostname.endsWith(DNS_ROOT),
    400,
    `Invalid hostname: ${url.hostname}. It must end with ${DNS_ROOT}.`,
  )

  const payerWalletAddress = url.hostname.slice(0, -DNS_ROOT.length)
  const [pieceCid] = url.pathname.split('/').filter(Boolean)

  httpAssert(pieceCid, 404, 'Missing required path element: `/{CID}`')
  httpAssert(
    pieceCid.startsWith('baga') || pieceCid.startsWith('bafk'),
    404,
    `Invalid CID: ${pieceCid}. It is not a valid CommP (v1 or v2).`,
  )

  const botName = checkBotAuthorization(request, { BOT_TOKENS })

  return { payerWalletAddress, pieceCid, botName }
}

/**
 * @param {Request} request
 * @param {object} args
 * @param {string} args.BOT_TOKENS
 * @returns {string | undefined} Bot name or the access token
 */
export function checkBotAuthorization(request, { BOT_TOKENS }) {
  const allowedTokens = BOT_TOKENS.split(',').map((t) => t.trim())

  const auth = request.headers.get('authorization')
  if (!auth) return undefined

  const [prefix, token, ...rest] = auth.split(' ')

  httpAssert(
    prefix === 'Bearer' && token && rest.length === 0,
    401,
    'Unauthorized: Authorization header must use Bearer scheme',
  )

  httpAssert(
    allowedTokens.includes(token),
    401,
    'Unauthorized: Invalid Access Token',
  )

  return token.split('_')[0]
}
