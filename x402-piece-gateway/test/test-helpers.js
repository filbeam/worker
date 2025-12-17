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

/**
 * Creates a data set in the database
 *
 * @param {Env} env
 * @param {Object} options
 * @param {number} options.dataSetId
 * @param {string} options.payerAddress
 * @param {boolean} options.withCDN
 */
export async function withDataSet(
  env,
  { dataSetId = 0, payerAddress = TEST_PAYEE, withCDN = true } = {},
) {
  await env.DB.prepare(
    `INSERT INTO data_sets (id, payer_address, with_cdn)
     VALUES (?, ?, ?)`,
  )
    .bind(String(dataSetId), payerAddress.toLowerCase(), withCDN)
    .run()
}

/**
 * Creates a piece in the database
 *
 * @param {Env} env
 * @param {Object} options
 * @param {string} options.pieceId
 * @param {number} options.dataSetId
 * @param {string} options.pieceCid
 * @param {string} [options.x402Price]
 */
export async function withPiece(
  env,
  { pieceId = 0, dataSetId = 0, pieceCid = TEST_CID, x402Price = null } = {},
) {
  await env.DB.prepare(
    `INSERT INTO pieces (id, data_set_id, cid, x402_price)
     VALUES (?, ?, ?, ?)`,
  )
    .bind(String(pieceId), String(dataSetId), pieceCid, x402Price)
    .run()
}

/**
 * Convenience helper to create a data set and piece with x402 pricing.
 *
 * @param {Env} env
 * @param {Object} options
 * @param {number} options.dataSetId
 * @param {string} options.payerAddress
 * @param {string} options.pieceId
 * @param {string} options.pieceCid
 * @param {string} options.x402Price
 */
export async function withX402Piece(
  env,
  {
    dataSetId = 0,
    payerAddress = TEST_PAYEE,
    pieceId = 0,
    pieceCid = TEST_CID,
    x402Price = TEST_PRICE,
  } = {},
) {
  await withDataSet(env, { dataSetId, payerAddress })
  await withPiece(env, { pieceId, dataSetId, pieceCid, x402Price })
}

/**
 * Inserts a wallet into wallet_details with sanctioned status.
 *
 * @param {Env} env
 * @param {string} address
 * @param {boolean} [isSanctioned=false] Default is `false`
 */
export async function withWalletDetails(env, address, isSanctioned = false) {
  await env.DB.prepare(
    `INSERT INTO wallet_details (address, is_sanctioned)
     VALUES (?, ?)`,
  )
    .bind(address.toLowerCase(), isSanctioned ? 1 : 0)
    .run()
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
