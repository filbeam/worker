import { httpAssert, isValidEthereumAddress } from '@filbeam/retrieval'

/**
 * Parse params found in path of the request URL URL format:
 * https://{payee_address}.x402.calibration.filbeam.io/{piece_cid}
 *
 * @param {Request} request
 * @param {Env} env
 * @returns {{
 *   payeeAddress: string
 *   pieceCid: string
 *   payment: string | null
 *   isWebBrowser: boolean
 * }}
 */
export function parseRequest(request, env) {
  const url = new URL(request.url)
  console.log('x402 retrieval request', {
    DNS_ROOT: env.DNS_ROOT,
    url: url.href,
  })

  httpAssert(
    url.hostname.endsWith(env.DNS_ROOT),
    400,
    `Invalid hostname: ${url.hostname}. It must end with ${env.DNS_ROOT}.`,
  )

  const payeeAddress = url.hostname.slice(0, -env.DNS_ROOT.length)

  httpAssert(payeeAddress, 400, 'Missing payee address in hostname')

  httpAssert(
    isValidEthereumAddress(payeeAddress),
    400,
    `Invalid payee address: ${payeeAddress}. Must be a valid Ethereum address.`,
  )

  const [pieceCid] = url.pathname.split('/').filter(Boolean)

  httpAssert(pieceCid, 404, 'Missing required path element: `/{CID}`')
  httpAssert(
    pieceCid.startsWith('baga') || pieceCid.startsWith('bafk'),
    404,
    `Invalid CID: ${pieceCid}. It is not a valid CommP (v1 or v2).`,
  )

  const payment = request.headers.get('X-PAYMENT')
  const userAgent = request.headers.get('User-Agent') || ''
  const acceptHeader = request.headers.get('Accept') || ''
  const isWebBrowser =
    acceptHeader.includes('text/html') && userAgent.includes('Mozilla')

  return {
    payeeAddress: payeeAddress.toLowerCase(),
    pieceCid,
    payment,
    isWebBrowser,
  }
}
