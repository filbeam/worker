import { httpAssert } from '@filbeam/retrieval'

/**
 * Retrieves the provider and data set id for a given root CID.
 *
 * @param {{ DB: D1Database }} env - Cloudflare Worker environment with D1 DB
 *   binding
 * @param {string} payerAddress - The address of the client paying for the
 *   request
 * @param {string} pieceCid - The piece CID to look up
 * @param {boolean} [enforceEgressQuota=false] - Whether to enforce egress quota
 *   limits. Default is `false`
 * @returns {Promise<
 *   {
 *     serviceProviderId: string
 *     serviceUrl: string
 *     dataSetId: string
 *     cdnEgressQuota: bigint
 *     cacheMissEgressQuota: bigint
 *   }[]
 * >}
 */
export async function getRetrievalCandidatesAndValidatePayer(
  env,
  payerAddress,
  pieceCid,
  enforceEgressQuota = false,
) {
  const query = `
   SELECT 
    pieces.data_set_id, 
    data_sets.service_provider_id, 
    data_sets.payer_address, 
    data_sets.with_cdn,
    data_set_egress_quotas.cdn_egress_quota, 
    data_set_egress_quotas.cache_miss_egress_quota,
    service_providers.service_url, 
    service_providers.is_deleted as service_provider_is_deleted,
    wallet_details.is_sanctioned
   FROM pieces
   LEFT OUTER JOIN data_sets
     ON pieces.data_set_id = data_sets.id
   LEFT OUTER JOIN data_set_egress_quotas
     ON pieces.data_set_id = data_set_egress_quotas.data_set_id
   LEFT OUTER JOIN service_providers
     ON data_sets.service_provider_id = service_providers.id
   LEFT OUTER JOIN wallet_details
     ON data_sets.payer_address = wallet_details.address
   WHERE
     pieces.cid = ? AND pieces.is_deleted IS FALSE
 `

  const results = /**
   * @type {{
   *   service_provider_id: string
   *   data_set_id: string
   *   payer_address: string | undefined
   *   with_cdn: number | undefined
   *   cdn_egress_quota: string | undefined
   *   cache_miss_egress_quota: string | undefined
   *   service_url: string | undefined
   *   service_provider_is_deleted: boolean | undefined
   *   is_sanctioned: number | undefined
   * }[]}
   */ (
    /** @type {any[]} */ (
      (await env.DB.prepare(query).bind(pieceCid).all()).results
    )
  )
  httpAssert(
    results && results.length > 0,
    404,
    `Piece_cid '${pieceCid}' does not exist or may not have been indexed yet.`,
  )

  const withServiceProvider = results.filter(
    (row) =>
      row &&
      row.service_provider_id != null &&
      !row.service_provider_is_deleted,
  )
  httpAssert(
    withServiceProvider.length > 0,
    404,
    `Piece_cid '${pieceCid}' exists but has no associated service provider.`,
  )

  const withPaymentRail = withServiceProvider.filter(
    (row) =>
      row.payer_address && row.payer_address.toLowerCase() === payerAddress,
  )
  httpAssert(
    withPaymentRail.length > 0,
    402,
    `There is no Filecoin Warm Storage Service deal for payer '${payerAddress}' and piece_cid '${pieceCid}'.`,
  )

  const withCDN = withPaymentRail.filter(
    (row) => row.with_cdn && row.with_cdn === 1,
  )
  httpAssert(
    withCDN.length > 0,
    402,
    `The Filecoin Warm Storage Service deal for payer '${payerAddress}' and piece_cid '${pieceCid}' has withCDN=false.`,
  )

  const withPayerNotSanctioned = withCDN.filter((row) => !row.is_sanctioned)
  httpAssert(
    withPayerNotSanctioned.length > 0,
    403,
    `Wallet '${payerAddress}' is sanctioned and cannot retrieve piece_cid '${pieceCid}'.`,
  )

  const withApprovedProvider = withPayerNotSanctioned.filter(
    (row) => row.service_url,
  )
  httpAssert(
    withApprovedProvider.length > 0,
    404,
    `No approved service provider found for payer '${payerAddress}' and piece_cid '${pieceCid}'.`,
  )

  // Check CDN quota first
  const withSufficientCDNQuota = enforceEgressQuota
    ? withApprovedProvider.filter((row) => {
        return BigInt(row.cdn_egress_quota ?? '0') > 0n
      })
    : withApprovedProvider

  httpAssert(
    withSufficientCDNQuota.length > 0,
    402,
    `CDN egress quota exhausted for payer '${payerAddress}' and data set '${withApprovedProvider[0]?.data_set_id}'. Please top up your CDN egress quota.`,
  )

  // Check cache-miss quota
  const withSufficientCacheMissQuota = enforceEgressQuota
    ? withSufficientCDNQuota.filter((row) => {
        return BigInt(row.cache_miss_egress_quota ?? '0') > 0n
      })
    : withSufficientCDNQuota

  httpAssert(
    withSufficientCacheMissQuota.length > 0,
    402,
    `Cache miss egress quota exhausted for payer '${payerAddress}' and data set '${withSufficientCDNQuota[0]?.data_set_id}'. Please top up your cache miss egress quota.`,
  )

  const retrievalCandidates = withSufficientCacheMissQuota.map((row) => ({
    dataSetId: row.data_set_id,
    serviceProviderId: row.service_provider_id,
    // We need this cast to supress a TypeScript error. The compiler is not able to infer that
    // `withCDN.filter()` above returns only rows with `service_url` defined.
    serviceUrl: /** @type {string} */ (row.service_url),
    cdnEgressQuota: BigInt(row.cdn_egress_quota ?? '0'),
    cacheMissEgressQuota: BigInt(row.cache_miss_egress_quota ?? '0'),
  }))

  console.log(
    `Looked up ${retrievalCandidates.length} retrieval candidates for piece_cid '${pieceCid}' and payer '${payerAddress}'`,
    retrievalCandidates,
  )

  return retrievalCandidates
}
