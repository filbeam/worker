import { httpAssert, isValidEthereumAddress } from '@filbeam/retrieval'

/**
 * Parse params found in path of the request URL URL format:
 * https://{payer_address}.x402.calibration.filbeam.io/{piece_cid}
 *
 * @param {Request} request
 * @param {Env} env
 * @returns {{
 *   payerAddress: string
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

  const payerAddress = url.hostname.slice(0, -env.DNS_ROOT.length)

  httpAssert(payerAddress, 400, 'Missing payee address in hostname')

  httpAssert(
    isValidEthereumAddress(payerAddress),
    400,
    `Invalid payer address: ${payerAddress}. Must be a valid Ethereum address.`,
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
    payerAddress: payerAddress.toLowerCase(),
    pieceCid,
    payment,
    isWebBrowser,
  }
}

/**
 * Build the URL for forwarding requests to piece-retriever.
 *
 * @param {Env} env - Environment bindings
 * @param {string} payerAddress - The payee's Ethereum address
 * @param {string} pieceCid - The CID of the requested piece
 * @returns {string} The transformed URL for piece-retriever
 */
export function buildForwardUrl(env, payerAddress, pieceCid) {
  return `https://${payerAddress}.${env.PIECE_RETRIEVER_DNS_ROOT}/${pieceCid}`
}
