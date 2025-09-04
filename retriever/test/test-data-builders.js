import { getBadBitsEntry } from '../lib/bad-bits-util'

/**
 * @param {Env} env
 * @param {Object} options
 * @param {number} options.storageProviderId
 * @param {string} options.pieceCid
 * @param {number} options.dataSetId
 * @param {boolean} options.withCDN
 * @param {string} options.payerAddress
 * @param {string} options.pieceId
 */
export async function withDataSetPieces(
  env,
  {
    storageProviderId = 0,
    payerAddress = '0x1234567890abcdef1234567890abcdef12345608',
    pieceCid = 'bagaTEST',
    dataSetId = 0,
    withCDN = true,
    pieceId = 0,
  } = {},
) {
  await env.DB.batch([
    env.DB.prepare(
      `
      INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn)
      VALUES (?, ?, ?, ?)
    `,
    ).bind(
      String(dataSetId),
      storageProviderId,
      payerAddress.toLowerCase(),
      withCDN,
    ),

    env.DB.prepare(
      `
      INSERT INTO pieces (id, data_set_id, cid)
      VALUES (?, ?, ?)
    `,
    ).bind(String(pieceId), String(dataSetId), pieceCid),
  ])
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
    INSERT INTO providers (id, service_url)
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
