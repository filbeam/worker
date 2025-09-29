import { httpAssert } from './http-assert.js'

/**
 * Parse params found in path of the request URL
 *
 * @param {Request} request
 * @param {object} options
 * @param {string} options.DNS_ROOT
 * @returns {{
 *   payerWalletAddress: string
 *   ipfsRootCid: string
 *   ipfsSubpath: string
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

  const rootCidAndPayer = url.hostname.slice(0, -DNS_ROOT.length)
  const [ipfsRootCid, payerWalletAddress] = rootCidAndPayer.split('-')

  httpAssert(
    ipfsRootCid && payerWalletAddress,
    400,
    `The hostname must be in the format: {IpfsRootCID}-{PayerWalletAddress}${DNS_ROOT}`,
  )

  const ipfsSubpath = url.pathname || '/'

  return { payerWalletAddress, ipfsRootCid, ipfsSubpath }
}
