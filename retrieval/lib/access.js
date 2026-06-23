import { httpAssert } from './http-assert.js'

/**
 * The columns the authorization cascade reads from a candidate row. Callers may
 * pass rows with additional columns, which are preserved in the return value.
 * Quotas are stored as integers but D1 may surface them as strings, so both are
 * accepted.
 *
 * @typedef {object} RetrievalCandidateRow
 * @property {string | null} [service_provider_id]
 * @property {boolean | null} [service_provider_is_deleted]
 * @property {string | null} [payer_address]
 * @property {number | null} [with_cdn]
 * @property {number | boolean | null} [is_sanctioned]
 * @property {string | null} [service_url]
 * @property {string | number | null} [cdn_egress_quota]
 * @property {string | number | null} [cache_miss_egress_quota]
 */

/**
 * Runs the shared retrieval authorization cascade over candidate rows joined
 * from pieces, data_sets, service_providers and wallet_details. Each check
 * filters the rows and throws an httpAssert error when no row survives. Returns
 * the rows that pass every check.
 *
 * The checks run in order: indexed, has a (non-deleted) service provider, has a
 * payment rail for the payer, has CDN enabled, payer is not sanctioned, the
 * service provider is approved, and (when `enforceEgressQuota` is set) the data
 * set has CDN and cache-miss egress quota remaining.
 *
 * @template {RetrievalCandidateRow} Row
 * @param {Row[]} rows
 * @param {object} options
 * @param {string} options.payerAddress - Lower-cased payer address to match.
 * @param {boolean} [options.enforceEgressQuota] - Also require remaining CDN
 *   and cache-miss egress quota.
 * @returns {Row[]} The rows passing every check.
 */
export function filterAuthorizedRetrievalCandidates(
  rows,
  { payerAddress, enforceEgressQuota = false },
) {
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

  if (!enforceEgressQuota) return authorizedRetrievalCandidates

  const withSufficientCDNQuota = authorizedRetrievalCandidates.filter(
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

/**
 * Builds the SELECT that joins pieces, data_sets, data_set_egress_quotas,
 * service_providers and wallet_details and returns the columns the
 * authorization cascade in {@link filterAuthorizedRetrievalCandidates} reads.
 * Callers supply the lookup-specific `where` clause and any extra columns.
 *
 * @param {object} options
 * @param {string[]} [options.extraColumns] - Extra columns to select, in
 *   addition to the ones the cascade reads.
 * @param {string} options.where - The `WHERE` clause, without the `WHERE`
 *   keyword.
 * @returns {string}
 */
export function buildRetrievalCandidateQuery({ extraColumns = [], where }) {
  const columns = [
    'pieces.data_set_id',
    'data_sets.service_provider_id',
    'data_sets.payer_address',
    'data_sets.with_cdn',
    'data_set_egress_quotas.cdn_egress_quota',
    'data_set_egress_quotas.cache_miss_egress_quota',
    'service_providers.service_url',
    'service_providers.is_deleted AS service_provider_is_deleted',
    'wallet_details.is_sanctioned',
    ...extraColumns,
  ]

  return `
    SELECT ${columns.join(', ')}
    FROM pieces
    LEFT OUTER JOIN data_sets
      ON pieces.data_set_id = data_sets.id
    LEFT OUTER JOIN data_set_egress_quotas
      ON pieces.data_set_id = data_set_egress_quotas.data_set_id
    LEFT OUTER JOIN service_providers
      ON data_sets.service_provider_id = service_providers.id
    LEFT OUTER JOIN wallet_details
      ON data_sets.payer_address = wallet_details.address
    WHERE ${where}
  `
}
