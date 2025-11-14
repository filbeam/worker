import { httpAssert } from '@filbeam/retrieval'
import { base32ToBigInt } from './bigint-util.js'

/**
 * Parse params found in path of the request URL
 *
 * @param {Request} request
 * @param {object} options
 * @param {string} options.DNS_ROOT
 * @param {string} options.BOT_TOKENS
 * @returns {{
 *   dataSetId: string
 *   pieceId: string
 *   ipfsSubpath: string
 *   ipfsFormat: string | null
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

  const slug = url.hostname.slice(0, -DNS_ROOT.length)
  const parts = slug.split('-')

  httpAssert(
    parts.length === 3,
    400,
    `The hostname must be in the format: 1-{dataSetId}-{pieceId}${DNS_ROOT}`,
  )

  const [version, encodedDataSetId, encodedPieceId] = parts

  httpAssert(
    version === '1',
    400,
    `Unsupported slug version: ${version}. Expected version 1.`,
  )

  httpAssert(
    encodedDataSetId && encodedPieceId,
    400,
    `The hostname must be in the format: 1-{dataSetId}-{pieceId}${DNS_ROOT}`,
  )

  let dataSetId
  let pieceId

  try {
    dataSetId = base32ToBigInt(encodedDataSetId).toString()
  } catch (error) {
    httpAssert(
      false,
      400,
      `Invalid dataSetId encoding in slug: ${encodedDataSetId}. ${error instanceof Error ? error.message : String(error)}`,
    )
  }

  try {
    pieceId = base32ToBigInt(encodedPieceId).toString()
  } catch (error) {
    httpAssert(
      false,
      400,
      `Invalid pieceId encoding in slug: ${encodedPieceId}. ${error instanceof Error ? error.message : String(error)}`,
    )
  }

  const ipfsSubpath = url.pathname || '/'
  const ipfsFormat = url.searchParams.get('format')

  const botName = checkBotAuthorization(request, { BOT_TOKENS })

  return { dataSetId, pieceId, ipfsSubpath, ipfsFormat, botName }
}

/**
 * @param {Request} request
 * @param {object} args
 * @param {string} args.BOT_TOKENS
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
