import { getBadBitsEntry } from '../lib/bad-bits-util'
import { DNS_ROOT } from './retriever.test'

/**
 * @param {string} payerWalletAddress
 * @param {string} pieceCid
 * @param {string} method
 * @param {Object} headers
 * @returns {Request}
 */
export function withRequest(
  payerWalletAddress,
  pieceCid,
  method = 'GET',
  headers = {},
) {
  let url = 'http://'
  if (payerWalletAddress) url += `${payerWalletAddress}.`
  url += DNS_ROOT.slice(1) // remove the leading '.'
  if (pieceCid) url += `/${pieceCid}`

  return new Request(url, { method, headers })
}

/**
 * @param {Env} env
 * @param {Object} options
 * @param {number} options.dataSetId
 * @param {number} options.serviceProviderId
 * @param {string} options.payerAddress
 * @param {boolean} options.withCDN
 * @param {number} options.cdnEgressQuota
 * @param {number} options.cacheMissEgressQuota
 */
export async function withDataSet(
  env,
  {
    dataSetId = 0,
    serviceProviderId = 0,
    payerAddress = '0x1234567890abcdef1234567890abcdef12345608',
    withCDN = true,
    cdnEgressQuota = 0,
    cacheMissEgressQuota = 0,
  } = {},
) {
  await env.DB.prepare(
    `INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota)
     VALUES (?, ?, ?, ?, ?, ?)`,
  )
    .bind(
      String(dataSetId),
      String(serviceProviderId),
      payerAddress.toLowerCase(),
      withCDN,
      cdnEgressQuota,
      cacheMissEgressQuota,
    )
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
 */
export async function withPiece(
  env,
  { pieceId = 0, dataSetId = 0, pieceCid = 'bagaTEST' } = {},
) {
  await env.DB.prepare(
    `INSERT INTO pieces (id, data_set_id, cid)
     VALUES (?, ?, ?)`,
  )
    .bind(String(pieceId), String(dataSetId), pieceCid)
    .run()
}

/**
 * Convenience helper for tests with a single piece per data set. Creates both a
 * data set and a piece in a single call.
 *
 * @param {Env} env
 * @param {Object} options
 * @param {number} options.dataSetId
 * @param {number} options.serviceProviderId
 * @param {string} options.payerAddress
 * @param {boolean} options.withCDN
 * @param {number} options.cdnEgressQuota
 * @param {number} options.cacheMissEgressQuota
 * @param {string} options.pieceId
 * @param {string} options.pieceCid
 */
export async function withDataSetPieces(
  env,
  {
    dataSetId = 0,
    serviceProviderId = 0,
    payerAddress = '0x1234567890abcdef1234567890abcdef12345608',
    withCDN = true,
    cdnEgressQuota = 0,
    cacheMissEgressQuota = 0,
    pieceId = 0,
    pieceCid = 'bagaTEST',
  } = {},
) {
  await withDataSet(env, {
    dataSetId,
    serviceProviderId,
    payerAddress,
    withCDN,
    cdnEgressQuota,
    cacheMissEgressQuota,
  })
  await withPiece(env, { pieceId, dataSetId, pieceCid })
}

/**
 * @param {Env} env
 * @param {Object} options
 * @param {number} id
 * @param {string} [options.serviceUrl]
 */

export async function withApprovedProvider(
  env,
  { id, serviceUrl = 'https://pdp.xyz/' } = {},
) {
  await env.DB.prepare(
    `
    INSERT INTO service_providers (id, service_url)
    VALUES (?, ?)
  `,
  )
    .bind(String(id), serviceUrl)
    .run()
}
/**
 * @param {Env} env
 * @param {...string} cids
 */

export async function withBadBits(env, ...cids) {
  const stmt = await env.DB.prepare(
    'INSERT INTO bad_bits (hash, last_modified_at) VALUES (?, CURRENT_TIME)',
  )
  const entries = await Promise.all(cids.map(getBadBitsEntry))
  await env.DB.batch(entries.map((it) => stmt.bind(it)))
}
/**
 * Inserts an address into the database with an optional sanctioned flag.
 *
 * @param {Env} env
 * @param {string} address
 * @param {boolean} [isSanctioned=false] Default is `false`
 * @returns {Promise<void>}
 */

export async function withWalletDetails(env, address, isSanctioned = false) {
  await env.DB.prepare(
    `
    INSERT INTO wallet_details (address, is_sanctioned)
    VALUES (?, ?)
  `,
  )
    .bind(address.toLowerCase(), isSanctioned ? 1 : 0)
    .run()
}
