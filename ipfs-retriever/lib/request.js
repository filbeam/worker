import { httpAssert } from './http-assert.js'

/**
 * Parse params found in path of the request URL
 *
 * @param {Request} request
 * @param {object} options
 * @param {string} options.DNS_ROOT
 * @returns {{
 *   payerWalletAddress?: string
 *   pieceCid?: string
 * }}
 */
export function parseRequest(request, { DNS_ROOT }) {
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

  return { payerWalletAddress, pieceCid }
}
