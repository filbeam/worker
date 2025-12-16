import { env } from 'cloudflare:test'
import { exact } from 'x402/schemes'

export const TEST_PAYEE = '0xc83dbfdf61616778537211a7e5ca2e87ec6cf0ed'
export const TEST_CID =
  'baga6ea4seaqaleibb6ud4xeemuzzpsyhl6cxlsymsnfco4cdjka5uzajo2x4ipa'
export const TEST_PRICE = '1000000' // 1 USDC (6 decimals)

/** Helper to create mock useFacilitator for dependency injection */
export function createMockUseFacilitator({ verifyResult, settleResult } = {}) {
  return () => ({
    verify: async () => verifyResult ?? { isValid: true, payer: TEST_PAYEE },
    settle: async () =>
      settleResult ?? {
        success: true,
        transaction: '0xabc123',
        network: 'base-sepolia',
      },
  })
}

/** Helper to create a valid test payment header */
export function createTestPaymentHeader() {
  const payment = {
    x402Version: 1,
    scheme: 'exact',
    network: 'base-sepolia',
    payload: {
      signature: '0x' + '00'.repeat(65),
      authorization: {
        from: TEST_PAYEE,
        to: TEST_PAYEE,
        value: TEST_PRICE,
        validAfter: '0',
        validBefore: String(Math.floor(Date.now() / 1000) + 3600),
        nonce: '0x' + '00'.repeat(32),
      },
    },
  }
  return exact.evm.encodePayment(payment)
}

/** Helper to add x402 metadata to KV */
export async function withX402Metadata(
  payeeAddress,
  pieceCid,
  price,
  block = '1000',
) {
  await env.X402_METADATA_KV.put(
    `${payeeAddress.toLowerCase()}:${pieceCid}`,
    JSON.stringify({ price, block }),
  )
}
/** Helper to create a request with the proper URL format */

export function createRequest(env, payeeAddress, pieceCid, options = {}) {
  const { method = 'GET', headers = {} } = options
  let url = 'https://'
  if (payeeAddress) url += payeeAddress
  url += env.DNS_ROOT
  if (pieceCid) url += `/${pieceCid}`
  return new Request(url, { method, headers })
}
