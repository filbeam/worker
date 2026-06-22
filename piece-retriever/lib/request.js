import { httpAssert, checkBotAuthorization } from '@filbeam/retrieval'

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
 *   validateCacheMissResponse: boolean
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
  const validateCacheMissResponse = url.searchParams.has('validate')

  return { payerWalletAddress, pieceCid, botName, validateCacheMissResponse }
}
