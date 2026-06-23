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
