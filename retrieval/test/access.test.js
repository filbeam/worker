import { describe, it, expect } from 'vitest'
import { filterAuthorizedRetrievalCandidates } from '../lib/access.js'

const MESSAGES = {
  notIndexed: 'not indexed',
  noServiceProvider: 'no service provider',
  noPaymentRail: 'no payment rail',
  cdnDisabled: 'cdn disabled',
  sanctioned: 'sanctioned',
  noApprovedProvider: 'no approved provider',
}

/** A row that passes every check. payer_address is upper-cased on purpose. */
function authorizedRow(overrides = {}) {
  return {
    service_provider_id: 'sp1',
    service_provider_is_deleted: 0,
    payer_address: '0xABCDEF',
    with_cdn: 1,
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

describe('filterAuthorizedRetrievalCandidates', () => {
  const payerAddress = '0xabcdef'

  it('returns the rows passing every check, matching the payer case-insensitively', () => {
    const rows = [authorizedRow()]
    expect(
      filterAuthorizedRetrievalCandidates(rows, {
        payerAddress,
        messages: MESSAGES,
      }),
    ).toEqual(rows)
  })

  it('throws 404 when there are no rows', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalCandidates([], {
          payerAddress,
          messages: MESSAGES,
        }),
      404,
      MESSAGES.notIndexed,
    )
  })

  it('throws 404 when no row has a service provider', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalCandidates(
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
        filterAuthorizedRetrievalCandidates(
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
        filterAuthorizedRetrievalCandidates(
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
        filterAuthorizedRetrievalCandidates([authorizedRow({ with_cdn: 0 })], {
          payerAddress,
          messages: MESSAGES,
        }),
      402,
      MESSAGES.cdnDisabled,
    )
  })

  it('throws 403 when the payer is sanctioned', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalCandidates(
          [authorizedRow({ is_sanctioned: 1 })],
          {
            payerAddress,
            messages: MESSAGES,
          },
        ),
      403,
      MESSAGES.sanctioned,
    )
  })

  it('throws 404 when no service provider is approved (no service_url)', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalCandidates(
          [authorizedRow({ service_url: null })],
          {
            payerAddress,
            messages: MESSAGES,
          },
        ),
      404,
      MESSAGES.noApprovedProvider,
    )
  })
})
