import { bigIntToBase32 } from './bigint-util.js'
import { httpAssert } from '@filbeam/retrieval'

const SELECT_CANDIDATES_BY_CID = `
   SELECT
     pieces.id as piece_id,
     pieces.data_set_id,
     pieces.ipfs_root_cid,
     data_sets.service_provider_id,
     data_sets.payer_address,
     data_sets.with_cdn,
     data_sets.with_ipfs_indexing,
     service_providers.service_url,
     wallet_details.is_sanctioned
   FROM pieces
   LEFT OUTER JOIN data_sets
     ON pieces.data_set_id = data_sets.id
   LEFT OUTER JOIN service_providers
     ON data_sets.service_provider_id = service_providers.id
   LEFT OUTER JOIN wallet_details
     ON data_sets.payer_address = wallet_details.address
   WHERE pieces.ipfs_root_cid = ?
 `

/**
 * Validates query results and returns every approved retrieval candidate. This
 * is a shared helper used by both getRetrievalCandidatesByWalletAndCid and
 * getRetrievalCandidatesByDataSetAndPiece.
 *
 * @param {object} params
 * @param {any[]} params.results - The query results to validate
 * @param {string} params.payerAddress - The lower-cased address of the client
 *   paying for the request
 * @param {string} params.lookupKey - Descriptive key for error messages (e.g.,
 *   "IPFS Root CID 'bafk...'")
 * @returns {{
 *   serviceProviderId: string
 *   serviceUrl: string
 *   dataSetId: string
 *   pieceId: string
 *   ipfsRootCid: string
 * }[]}
 */
function validateQueryResultsAndGetCandidates(params) {
  const { results, payerAddress, lookupKey } = params

  httpAssert(
    results && results.length > 0,
    404,
    `${lookupKey} does not exist or may not have been indexed yet.`,
  )

  const withServiceProvider = results.filter(
    (row) => row && row.service_provider_id != null,
  )
  httpAssert(
    withServiceProvider.length > 0,
    404,
    `${lookupKey} exists but has no associated service provider.`,
  )

  const withPaymentRail = withServiceProvider.filter(
    (row) =>
      row.payer_address && row.payer_address.toLowerCase() === payerAddress,
  )
  httpAssert(
    withPaymentRail.length > 0,
    402,
    `There is no Filecoin Warm Storage Service deal for payer '${payerAddress}' and ${lookupKey}.`,
  )

  const withCDN = withPaymentRail.filter(
    (row) => row.with_cdn && row.with_cdn === 1,
  )
  httpAssert(
    withCDN.length > 0,
    402,
    `The Filecoin Warm Storage Service deal for payer '${payerAddress}' and ${lookupKey} has withCDN=false.`,
  )

  const withIpfsIndexing = withCDN.filter((row) => row.with_ipfs_indexing === 1)
  httpAssert(
    withIpfsIndexing.length > 0,
    402,
    `The Filecoin Warm Storage Service deal for payer '${payerAddress}' and ${lookupKey} has withIpfsIndexing=false.`,
  )

  const withPayerNotSanctioned = withIpfsIndexing.filter(
    (row) => !row.is_sanctioned,
  )
  httpAssert(
    withPayerNotSanctioned.length > 0,
    403,
    `Wallet '${payerAddress}' is sanctioned and cannot retrieve ${lookupKey}.`,
  )

  const withApprovedProvider = withPayerNotSanctioned.filter(
    (row) => row.service_url,
  )
  httpAssert(
    withApprovedProvider.length > 0,
    404,
    `No approved service provider found for payer '${payerAddress}' and ${lookupKey}.`,
  )

  const withIpfsRootCid = withApprovedProvider.filter(
    (row) => row.ipfs_root_cid,
  )
  httpAssert(
    withIpfsRootCid.length > 0,
    404,
    `${lookupKey} exists but has no associated IPFS Root CID.`,
  )

  const candidates = withIpfsRootCid.map((row) => ({
    serviceProviderId: row.service_provider_id,
    // We need this cast to suppress a TypeScript error. The compiler cannot
    // infer that the filters above keep only rows with service_url defined.
    serviceUrl: /** @type {string} */ (row.service_url),
    dataSetId: row.data_set_id,
    pieceId: row.piece_id,
    ipfsRootCid: row.ipfs_root_cid,
  }))

  console.log(
    `Validated ${candidates.length} retrieval candidate(s) for ${lookupKey} and payer '${payerAddress}'`,
  )

  return candidates
}

/**
 * Retrieves every approved retrieval candidate (one per service provider) for a
 * given root CID and payer.
 *
 * @param {Pick<Env, 'DB'>} env - Cloudflare Worker environment with D1 DB
 *   binding
 * @param {string} payerAddress - The lower-cased address of the client paying
 *   for the request
 * @param {string} ipfsRootCid - The IPFS Root CID to look up
 * @returns {Promise<
 *   {
 *     serviceProviderId: string
 *     serviceUrl: string
 *     dataSetId: string
 *     pieceId: string
 *     ipfsRootCid: string
 *   }[]
 * >}
 */
export async function getRetrievalCandidatesByWalletAndCid(
  env,
  payerAddress,
  ipfsRootCid,
) {
  const results = /** @type {any[]} */ (
    (await env.DB.prepare(SELECT_CANDIDATES_BY_CID).bind(ipfsRootCid).all())
      .results
  )

  return validateQueryResultsAndGetCandidates({
    results,
    payerAddress,
    lookupKey: `IPFS Root CID '${ipfsRootCid}'`,
  })
}

/**
 * Retrieves every approved retrieval candidate for the content addressed by a
 * given data set ID and piece ID. The piece is resolved to its content CID and
 * the data set's payer, then every service provider serving that content for
 * that payer is returned so the worker can retry across them.
 *
 * @param {Pick<Env, 'DB'>} env - Cloudflare Worker environment with D1 DB
 *   binding
 * @param {string} dataSetId - The data set ID
 * @param {string} pieceId - The piece ID
 * @returns {Promise<
 *   {
 *     serviceProviderId: string
 *     serviceUrl: string
 *     dataSetId: string
 *     pieceId: string
 *     ipfsRootCid: string
 *   }[]
 * >}
 */
export async function getRetrievalCandidatesByDataSetAndPiece(
  env,
  dataSetId,
  pieceId,
) {
  const piece = /**
   * @type {{
   *   ipfs_root_cid: string | null
   *   payer_address: string | null
   * } | null}
   */ (
    await env.DB.prepare(
      `
      SELECT pieces.ipfs_root_cid, data_sets.payer_address
      FROM pieces
      LEFT OUTER JOIN data_sets ON pieces.data_set_id = data_sets.id
      WHERE pieces.id = ? AND pieces.data_set_id = ?
      `,
    )
      .bind(pieceId, dataSetId)
      .first()
  )

  httpAssert(
    piece,
    404,
    `Piece ID '${pieceId}' does not exist in data set ID '${dataSetId}' or may not have been indexed yet.`,
  )

  const ipfsRootCid = piece.ipfs_root_cid
  const payerAddress = piece.payer_address

  httpAssert(
    payerAddress,
    404,
    `Data set ID '${dataSetId}' exists but has no associated payer address.`,
  )
  httpAssert(
    ipfsRootCid,
    404,
    `data set ID '${dataSetId}' and piece ID '${pieceId}' exists but has no associated IPFS Root CID.`,
  )

  const results = /** @type {any[]} */ (
    (await env.DB.prepare(SELECT_CANDIDATES_BY_CID).bind(ipfsRootCid).all())
      .results
  )

  return validateQueryResultsAndGetCandidates({
    results,
    payerAddress: payerAddress.toLowerCase(),
    lookupKey: `data set ID '${dataSetId}' and piece ID '${pieceId}'`,
  })
}

/**
 * Builds a slug from dataSetId and pieceId.
 *
 * @param {bigint} dataSetId - The data set ID as BigInt
 * @param {bigint} pieceId - The piece ID as BigInt
 * @returns {string} - The slug in format:
 *   1-{base32(dataSetId)}-{base32(pieceId)}
 */
export function buildSlug(dataSetId, pieceId) {
  return [
    '1', // version
    bigIntToBase32(dataSetId),
    bigIntToBase32(pieceId),
  ].join('-')
}

/**
 * @param {Pick<Env, 'DB'>} env - Cloudflare Worker environment with D1 DB
 *   binding
 * @param {string} payerAddress
 * @param {string} ipfsRootCid
 */
export async function getSlugForWalletAndCid(env, payerAddress, ipfsRootCid) {
  const [{ dataSetId, pieceId }] = await getRetrievalCandidatesByWalletAndCid(
    env,
    payerAddress,
    ipfsRootCid,
  )

  return buildSlug(BigInt(dataSetId), BigInt(pieceId))
}
