import {
  filterAuthorizedRetrievalCandidates,
  buildRetrievalCandidateQuery,
} from '@filbeam/retrieval'

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
  const query = buildRetrievalCandidateQuery({
    where: 'pieces.cid = ?',
  })

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
  const authorizedRetrievalCandidates = filterAuthorizedRetrievalCandidates(
    results,
    { payerAddress, enforceEgressQuota },
  )

  const retrievalCandidates = authorizedRetrievalCandidates.map((row) => ({
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
