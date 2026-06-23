import { describe, it, expect } from 'vitest'
import { filterAuthorizedRetrievalRows } from '../lib/access.js'

const MESSAGES = {
  notIndexed: 'not indexed',
  noServiceProvider: 'no service provider',
  noPaymentRail: 'no payment rail',
  cdnDisabled: 'cdn disabled',
  sanctioned: 'sanctioned',
  noApprovedProvider: 'no approved provider',
}

const IPFS_INDEXING_DISABLED = 'ipfs indexing disabled'

/** A row that passes every check. payer_address is upper-cased on purpose. */
function authorizedRow(overrides = {}) {
  return {
    service_provider_id: 'sp1',
    service_provider_is_deleted: 0,
    payer_address: '0xABCDEF',
    with_cdn: 1,
    with_ipfs_indexing: 1,
    is_sanctioned: 0,
    service_url: 'https://sp.example/',
    ...overrides,
  }
}

/** @param {() => unknown} fn */
function expectHttpError(fn, status, message) {
  let error
  try {
    fn()
  } catch (err) {
    error = err
  }
  expect(error).toBeInstanceOf(Error)
  expect(error.status).toBe(status)
  expect(error.message).toBe(message)
}

describe('filterAuthorizedRetrievalRows', () => {
  const payerAddress = '0xabcdef'

  it('returns the rows passing every check, matching the payer case-insensitively', () => {
    const rows = [authorizedRow()]
    expect(
      filterAuthorizedRetrievalRows(rows, {
        payerAddress,
        ipfsIndexingDisabledMessage: IPFS_INDEXING_DISABLED,
        messages: MESSAGES,
      }),
    ).toEqual(rows)
  })

  it('throws 404 when there are no rows', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalRows([], { payerAddress, messages: MESSAGES }),
      404,
      MESSAGES.notIndexed,
    )
  })

  it('throws 404 when no row has a service provider', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalRows(
          [authorizedRow({ service_provider_id: null })],
          { payerAddress, messages: MESSAGES },
        ),
      404,
      MESSAGES.noServiceProvider,
    )
  })

  it('always excludes soft-deleted service providers', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalRows(
          [authorizedRow({ service_provider_is_deleted: 1 })],
          { payerAddress, messages: MESSAGES },
        ),
      404,
      MESSAGES.noServiceProvider,
    )
  })

  it('throws 402 when the payer has no payment rail', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalRows(
          [authorizedRow({ payer_address: '0xother' })],
          { payerAddress, messages: MESSAGES },
        ),
      402,
      MESSAGES.noPaymentRail,
    )
  })

  it('throws 402 when CDN is disabled', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalRows([authorizedRow({ with_cdn: 0 })], {
          payerAddress,
          messages: MESSAGES,
        }),
      402,
      MESSAGES.cdnDisabled,
    )
  })

  it('throws 402 when IPFS indexing is required but disabled', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalRows(
          [authorizedRow({ with_ipfs_indexing: 0 })],
          {
            payerAddress,
            ipfsIndexingDisabledMessage: IPFS_INDEXING_DISABLED,
            messages: MESSAGES,
          },
        ),
      402,
      IPFS_INDEXING_DISABLED,
    )
  })

  it('does not require IPFS indexing by default', () => {
    const rows = [authorizedRow({ with_ipfs_indexing: 0 })]
    expect(
      filterAuthorizedRetrievalRows(rows, { payerAddress, messages: MESSAGES }),
    ).toEqual(rows)
  })

  it('throws 403 when the payer is sanctioned', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalRows([authorizedRow({ is_sanctioned: 1 })], {
          payerAddress,
          messages: MESSAGES,
        }),
      403,
      MESSAGES.sanctioned,
    )
  })

  it('throws 404 when no service provider is approved (no service_url)', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalRows([authorizedRow({ service_url: null })], {
          payerAddress,
          messages: MESSAGES,
        }),
      404,
      MESSAGES.noApprovedProvider,
    )
  })
})
