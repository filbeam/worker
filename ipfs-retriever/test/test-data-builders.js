import { getBadBitsEntry } from '@filbeam/retrieval'
import { CarWriter } from '@ipld/car'
import * as raw from 'multiformats/codecs/raw'
import { sha256 } from 'multiformats/hashes/sha2'
import { CID } from 'multiformats/cid'

/**
 * Builds an in-memory CAR holding a single raw block, so tests can exercise the
 * CAR-to-raw conversion without a live service provider. The CAR is larger than
 * the raw block it wraps (header + block framing).
 *
 * @param {Uint8Array} fileBytes - The raw content to wrap.
 * @returns {Promise<{ carBytes: Uint8Array; rootCid: string }>}
 */
export async function buildRawBlockCar(fileBytes) {
  const cid = CID.create(1, raw.code, await sha256.digest(fileBytes))
  const { writer, out } = CarWriter.create([cid])

  /** @type {Uint8Array[]} */
  const chunks = []
  const collecting = (async () => {
    for await (const chunk of out) chunks.push(chunk)
  })()
  await writer.put({ cid, bytes: fileBytes })
  await writer.close()
  await collecting

  const carBytes = new Uint8Array(chunks.reduce((sum, c) => sum + c.length, 0))
  let offset = 0
  for (const chunk of chunks) {
    carBytes.set(chunk, offset)
    offset += chunk.length
  }

  return { carBytes, rootCid: cid.toString() }
}

/**
 * @param {Env} env
 * @param {Object} options
 * @param {number} options.serviceProviderId
 * @param {string} options.pieceCid
 * @param {number} options.dataSetId
 * @param {boolean} options.withCDN
 * @param {string} options.payerAddress
 * @param {string} options.pieceId
 */
export async function withDataSetPiece(
  env,
  {
    serviceProviderId = 0,
    payerAddress = '0x1234567890abcdef1234567890abcdef12345608',
    pieceCid = 'bagaTEST',
    ipfsRootCid = 'bafk4test',
    dataSetId = 0,
    withCDN = true,
    withIpfsIndexing = true,
    pieceId = 0,
  } = {},
) {
  await env.DB.batch([
    env.DB.prepare(
      `
      INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, with_ipfs_indexing)
      VALUES (?, ?, ?, ?, ?)
    `,
    ).bind(
      String(dataSetId),
      String(serviceProviderId),
      payerAddress.toLowerCase(),
      withCDN,
      withIpfsIndexing,
    ),

    env.DB.prepare(
      `
      INSERT INTO pieces (id, data_set_id, cid, ipfs_root_cid)
      VALUES (?, ?, ?, ?)
    `,
    ).bind(String(pieceId), String(dataSetId), pieceCid, ipfsRootCid ?? null),
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
  await Promise.all(
    cids.map(async (cid) =>
      env.BAD_BITS_KV.put(`bad-bits:${await getBadBitsEntry(cid)}`, 'true'),
    ),
  )
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
