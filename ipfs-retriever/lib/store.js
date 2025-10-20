import { bigIntToBase32 } from './bigint-util.js'
import { httpAssert } from './http-assert.js'

/**
 * Logs the result of a file retrieval attempt to the D1 database.
 *
 * @param {Pick<Env, 'DB'>} env - Worker environment (contains D1 binding).
 * @param {object} params - Parameters for the retrieval log.
 * @param {number | null} params.egressBytes - The egress bytes of the response.
 * @param {number} params.responseStatus - The HTTP response status code.
 * @param {boolean | null} params.cacheMiss - Whether the retrieval was a cache
 *   miss.
 * @param {{
 *   fetchTtfb: number
 *   fetchTtlb: number
 *   workerTtfb: number
 * } | null} [params.performanceStats]
 *   - Performance statistics.
 *
 * @param {string} params.timestamp - The timestamp of the retrieval.
 * @param {string | null} params.requestCountryCode - The country code where the
 *   request originated from
 * @param {string | null} params.dataSetId - The data set ID associated with the
 *   retrieval
 * @returns {Promise<void>} - A promise that resolves when the log is inserted.
 */
export async function logRetrievalResult(env, params) {
  console.log('retrieval log', params)
  const {
    cacheMiss,
    egressBytes,
    responseStatus,
    timestamp,
    performanceStats,
    requestCountryCode,
    dataSetId,
  } = params

  try {
    await env.DB.prepare(
      `
      INSERT INTO retrieval_logs (
        timestamp,
        response_status,
        egress_bytes,
        cache_miss,
        fetch_ttfb,
        fetch_ttlb,
        worker_ttfb,
        request_country_code,
        data_set_id
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
    )
      .bind(
        timestamp,
        responseStatus,
        egressBytes,
        cacheMiss,
        performanceStats?.fetchTtfb ?? null,
        performanceStats?.fetchTtlb ?? null,
        performanceStats?.workerTtfb ?? null,
        requestCountryCode,
        dataSetId,
      )
      .run()
  } catch (error) {
    console.error(`Error inserting log: ${error}`)
    // TODO: Handle specific SQL error codes if needed
    throw error
  }
}

/**
 * Validates query results and returns provider info. This is a shared helper
 * used by both getStorageProviderAndValidatePayerByWalletAndCid and
 * getStorageProviderAndValidatePayerByDataSetAndPiece.
 *
 * @param {object} params
 * @param {any[]} params.results - The query results to validate
 * @param {string} params.payerAddress - The address of the client paying for
 *   the request
 * @param {string} params.lookupKey - Descriptive key for error messages (e.g.,
 *   "IPFS Root CID 'bafk...'")
 * @returns {{
 *   serviceProviderId: string
 *   serviceUrl: string
 *   dataSetId: string
 *   pieceId: string
 *   ipfsRootCid: string
 * }}
 */
function validateQueryResultsAndGetProvider(params) {
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

  const {
    piece_id: pieceId,
    data_set_id: dataSetId,
    ipfs_root_cid: ipfsRootCid,
    service_provider_id: serviceProviderId,
    service_url: serviceUrl,
  } = withApprovedProvider[0]

  // We need this assertion to supress TypeScript error. The compiler is not able to infer that
  // `withApprovedProvider.filter()` above returns only rows with `service_url` defined.
  httpAssert(serviceUrl, 500, 'should never happen')

  console.log(
    `Validated data set ID '${dataSetId}', piece ID '${pieceId}', and service provider id '${serviceProviderId}' for ${lookupKey} and payer '${payerAddress}'. Service URL: ${serviceUrl}`,
  )

  return { serviceProviderId, serviceUrl, dataSetId, pieceId, ipfsRootCid }
}

/**
 * Retrieves the provider and data set id for a given root CID.
 *
 * @param {Pick<Env, 'DB'>} env - Cloudflare Worker environment with D1 DB
 *   binding
 * @param {string} payerAddress - The address of the client paying for the
 *   request
 * @param {string} ipfsRootCid - The IPFS Root CID to look up
 * @returns {Promise<{
 *   serviceProviderId: string
 *   serviceUrl: string
 *   dataSetId: string
 *   pieceId: string
 * }>}
 */
export async function getStorageProviderAndValidatePayerByWalletAndCid(
  env,
  payerAddress,
  ipfsRootCid,
) {
  if (
    payerAddress === '0x000000000000000000000000000000000000dead' &&
    ipfsRootCid ===
      'bafybeiagrjpf2rwth5oylc64czsrz2jm7a4fgo67b2luygqjrivjbswuku'
  ) {
    // Special case for testing purposes only
    return {
      serviceProviderId: '9999',
      serviceUrl: 'https://frisbii.fly.dev/',
      dataSetId: '9999',
      pieceId: '9999',
    }
  }

  const query = `
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

  const results = /**
   * @type {{
   *   piece_id: string
   *   data_set_id: string
   *   ipfs_root_cid: string
   *   service_provider_id: string
   *   payer_address: string | undefined
   *   with_cdn: number | undefined
   *   with_ipfs_indexing: number | undefined
   *   service_url: string | undefined
   *   is_sanctioned: number | undefined
   * }[]}
   */ (
    /** @type {any[]} */ (
      (await env.DB.prepare(query).bind(ipfsRootCid).all()).results
    )
  )

  return validateQueryResultsAndGetProvider({
    results,
    payerAddress,
    lookupKey: `IPFS Root CID '${ipfsRootCid}'`,
  })
}

/**
 * Retrieves the provider info for a given data set ID and piece ID.
 *
 * @param {Pick<Env, 'DB'>} env - Cloudflare Worker environment with D1 DB
 *   binding
 * @param {string} dataSetId - The data set ID
 * @param {string} pieceId - The piece ID
 * @returns {Promise<{
 *   serviceProviderId: string
 *   serviceUrl: string
 *   dataSetId: string
 *   pieceId: string
 *   ipfsRootCid: string
 * }>}
 */
export async function getStorageProviderAndValidatePayerByDataSetAndPiece(
  env,
  dataSetId,
  pieceId,
) {
  if (dataSetId === '9999' && pieceId === '9999') {
    // Special case for testing purposes only
    return {
      serviceProviderId: '9999',
      serviceUrl: 'https://frisbii.fly.dev/',
      dataSetId,
      pieceId,
      ipfsRootCid:
        'bafybeiagrjpf2rwth5oylc64czsrz2jm7a4fgo67b2luygqjrivjbswuku',
    }
  }

  const query = `
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
   WHERE pieces.id = ? AND pieces.data_set_id = ?
 `

  const results = /**
   * @type {{
   *   piece_id: string
   *   data_set_id: string
   *   ipfs_root_cid: string
   *   service_provider_id: string
   *   payer_address: string | undefined
   *   with_cdn: number | undefined
   *   with_ipfs_indexing: number | undefined
   *   service_url: string | undefined
   *   is_sanctioned: number | undefined
   * }[]}
   */ (
    /** @type {any[]} */ (
      (await env.DB.prepare(query).bind(pieceId, dataSetId).all()).results
    )
  )

  httpAssert(
    results && results.length > 0,
    404,
    `Piece ID '${pieceId}' does not exist in data set ID '${dataSetId}' or may not have been indexed yet.`,
  )

  // Extract the payer address from the first result
  const { payer_address: payerAddress } = results[0]

  httpAssert(
    payerAddress,
    404,
    `Data set ID '${dataSetId}' exists but has no associated payer address.`,
  )

  return validateQueryResultsAndGetProvider({
    results,
    payerAddress,
    lookupKey: `data set ID '${dataSetId}' and piece ID '${pieceId}'`,
  })
}

/**
 * @param {Pick<Env, 'DB'>} env - Worker environment (contains D1 binding).
 * @param {object} params - Parameters for the data set update.
 * @param {string} params.dataSetId - The ID of the data set to update.
 * @param {number} params.egressBytes - The egress bytes used for the response.
 */
export async function updateDataSetStats(env, { dataSetId, egressBytes }) {
  await env.DB.prepare(
    `
    UPDATE data_sets
    SET total_egress_bytes_used = total_egress_bytes_used + ?
    WHERE id = ?
    `,
  )
    .bind(egressBytes, dataSetId)
    .run()
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
  const { dataSetId, pieceId } =
    await getStorageProviderAndValidatePayerByWalletAndCid(
      env,
      payerAddress,
      ipfsRootCid,
    )

  return buildSlug(BigInt(dataSetId), BigInt(pieceId))
}
