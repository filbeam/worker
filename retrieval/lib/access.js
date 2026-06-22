import { httpAssert } from './http-assert.js'

/**
 * Runs the shared retrieval authorization cascade over candidate rows joined
 * from pieces, data_sets, service_providers and wallet_details. Each check
 * filters the rows and throws an httpAssert error with the caller-provided
 * message when no row survives. Returns the rows that pass every check.
 *
 * The checks run in order: indexed, has a (non-deleted) service provider, has a
 * payment rail for the payer, has CDN enabled, (optionally) has IPFS indexing
 * enabled, payer is not sanctioned, and the service provider is approved.
 *
 * @param {any[]} rows
 * @param {object} options
 * @param {string} options.payerAddress - Lower-cased payer address to match.
 * @param {object} options.messages - Error messages per failed check.
 * @param {string} options.messages.notIndexed
 * @param {string} options.messages.noServiceProvider
 * @param {string} options.messages.noPaymentRail
 * @param {string} options.messages.cdnDisabled
 * @param {string} options.messages.sanctioned
 * @param {string} options.messages.noApprovedProvider
 * @param {boolean} [options.requireServiceProviderNotDeleted] - Also exclude
 *   rows whose service provider is soft-deleted.
 * @param {string} [options.ipfsIndexingDisabledMessage] - When provided, also
 *   require IPFS indexing to be enabled on the deal, throwing this message
 *   otherwise.
 * @returns {any[]} The rows passing every check.
 */
export function filterAuthorizedRetrievalRows(
  rows,
  {
    payerAddress,
    messages,
    requireServiceProviderNotDeleted = false,
    ipfsIndexingDisabledMessage,
  },
) {
  httpAssert(rows && rows.length > 0, 404, messages.notIndexed)

  const withServiceProvider = rows.filter(
    (row) =>
      row &&
      row.service_provider_id != null &&
      (!requireServiceProviderNotDeleted || !row.service_provider_is_deleted),
  )
  httpAssert(withServiceProvider.length > 0, 404, messages.noServiceProvider)

  const withPaymentRail = withServiceProvider.filter(
    (row) =>
      row.payer_address && row.payer_address.toLowerCase() === payerAddress,
  )
  httpAssert(withPaymentRail.length > 0, 402, messages.noPaymentRail)

  const withCDN = withPaymentRail.filter(
    (row) => row.with_cdn && row.with_cdn === 1,
  )
  httpAssert(withCDN.length > 0, 402, messages.cdnDisabled)

  let rowsAfterCdn = withCDN
  if (ipfsIndexingDisabledMessage) {
    rowsAfterCdn = withCDN.filter((row) => row.with_ipfs_indexing === 1)
    httpAssert(rowsAfterCdn.length > 0, 402, ipfsIndexingDisabledMessage)
  }

  const withPayerNotSanctioned = rowsAfterCdn.filter(
    (row) => !row.is_sanctioned,
  )
  httpAssert(withPayerNotSanctioned.length > 0, 403, messages.sanctioned)

  const withApprovedProvider = withPayerNotSanctioned.filter(
    (row) => row.service_url,
  )
  httpAssert(withApprovedProvider.length > 0, 404, messages.noApprovedProvider)

  return withApprovedProvider
}
