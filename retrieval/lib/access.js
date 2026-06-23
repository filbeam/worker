import { httpAssert } from './http-assert.js'

/**
 * The columns the authorization cascade reads from a candidate row. Callers may
 * pass rows with additional columns, which are preserved in the return value.
 *
 * @typedef {object} RetrievalCandidateRow
 * @property {string | null} [service_provider_id]
 * @property {boolean | null} [service_provider_is_deleted]
 * @property {string | null} [payer_address]
 * @property {number | null} [with_cdn]
 * @property {number | boolean | null} [is_sanctioned]
 * @property {string | null} [service_url]
 */

/**
 * Runs the shared retrieval authorization cascade over candidate rows joined
 * from pieces, data_sets, service_providers and wallet_details. Each check
 * filters the rows and throws an httpAssert error when no row survives. Returns
 * the rows that pass every check.
 *
 * The checks run in order: indexed, has a (non-deleted) service provider, has a
 * payment rail for the payer, has CDN enabled, payer is not sanctioned, and the
 * service provider is approved.
 *
 * @template {RetrievalCandidateRow} Row
 * @param {Row[]} rows
 * @param {object} options
 * @param {string} options.payerAddress - Lower-cased payer address to match.
 * @returns {Row[]} The rows passing every check.
 */
export function filterAuthorizedRetrievalCandidates(rows, { payerAddress }) {
  httpAssert(
    rows && rows.length > 0,
    404,
    'The requested content does not exist or may not have been indexed yet.',
  )

  const withServiceProvider = rows.filter(
    (row) =>
      row &&
      row.service_provider_id != null &&
      !row.service_provider_is_deleted,
  )
  httpAssert(
    withServiceProvider.length > 0,
    404,
    'The requested content exists but has no associated service provider.',
  )

  const withPaymentRail = withServiceProvider.filter(
    (row) =>
      row.payer_address && row.payer_address.toLowerCase() === payerAddress,
  )
  httpAssert(
    withPaymentRail.length > 0,
    402,
    `There is no Filecoin Warm Storage Service deal for payer '${payerAddress}' and the requested content.`,
  )

  const withCDN = withPaymentRail.filter(
    (row) => row.with_cdn && row.with_cdn === 1,
  )
  httpAssert(
    withCDN.length > 0,
    402,
    `The Filecoin Warm Storage Service deal for payer '${payerAddress}' and the requested content has withCDN=false.`,
  )

  const withPayerNotSanctioned = withCDN.filter((row) => !row.is_sanctioned)
  httpAssert(
    withPayerNotSanctioned.length > 0,
    403,
    `Wallet '${payerAddress}' is sanctioned and cannot retrieve the requested content.`,
  )

  const authorizedRetrievalCandidates = withPayerNotSanctioned.filter(
    (row) => row.service_url,
  )
  httpAssert(
    authorizedRetrievalCandidates.length > 0,
    404,
    `No approved service provider found for payer '${payerAddress}' and the requested content.`,
  )

  return authorizedRetrievalCandidates
}

/**
 * The egress quota columns read from a candidate row. Quotas are stored as
 * integers but D1 may surface them as strings, so both are accepted.
 *
 * @typedef {object} EgressQuotaRow
 * @property {string | number | null} [cdn_egress_quota]
 * @property {string | number | null} [cache_miss_egress_quota]
 */

/**
 * Filters retrieval candidates to those whose data set still has egress quota.
 * When `enforceEgressQuota` is false the rows are returned unchanged. Otherwise
 * rows with no remaining CDN or cache-miss quota are dropped, throwing a 402
 * when none remain.
 *
 * @template {EgressQuotaRow} Row
 * @param {Row[]} rows
 * @param {object} options
 * @param {string} options.payerAddress - Lower-cased payer address, used in
 *   error messages.
 * @param {boolean} [options.enforceEgressQuota]
 * @returns {Row[]} The rows with sufficient quota.
 */
export function filterCandidatesWithSufficientEgressQuota(
  rows,
  { payerAddress, enforceEgressQuota = false },
) {
  if (!enforceEgressQuota) return rows

  const withSufficientCDNQuota = rows.filter(
    (row) => BigInt(row.cdn_egress_quota ?? '0') > 0n,
  )
  httpAssert(
    withSufficientCDNQuota.length > 0,
    402,
    `CDN egress quota exhausted for payer '${payerAddress}' and the requested content. Please top up your CDN egress quota.`,
  )

  const withSufficientCacheMissQuota = withSufficientCDNQuota.filter(
    (row) => BigInt(row.cache_miss_egress_quota ?? '0') > 0n,
  )
  httpAssert(
    withSufficientCacheMissQuota.length > 0,
    402,
    `Cache miss egress quota exhausted for payer '${payerAddress}' and the requested content. Please top up your cache miss egress quota.`,
  )

  return withSufficientCacheMissQuota
}
