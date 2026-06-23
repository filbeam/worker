import { describe, it, expect } from 'vitest'
import {
  filterAuthorizedRetrievalCandidates,
  filterCandidatesWithSufficientEgressQuota,
} from '../lib/access.js'

const payerAddress = '0xabcdef'

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
  it('returns the rows passing every check, matching the payer case-insensitively', () => {
    const rows = [authorizedRow()]
    expect(filterAuthorizedRetrievalCandidates(rows, { payerAddress })).toEqual(
      rows,
    )
  })

  it('throws 404 when there are no rows', () => {
    expectHttpError(
      () => filterAuthorizedRetrievalCandidates([], { payerAddress }),
      404,
      'The requested content does not exist or may not have been indexed yet.',
    )
  })

  it('throws 404 when no row has a service provider', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalCandidates(
          [authorizedRow({ service_provider_id: null })],
          { payerAddress },
        ),
      404,
      'The requested content exists but has no associated service provider.',
    )
  })

  it('always excludes soft-deleted service providers', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalCandidates(
          [authorizedRow({ service_provider_is_deleted: 1 })],
          { payerAddress },
        ),
      404,
      'The requested content exists but has no associated service provider.',
    )
  })

  it('throws 402 when the payer has no payment rail', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalCandidates(
          [authorizedRow({ payer_address: '0xother' })],
          { payerAddress },
        ),
      402,
      `There is no Filecoin Warm Storage Service deal for payer '${payerAddress}' and the requested content.`,
    )
  })

  it('throws 402 when CDN is disabled', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalCandidates([authorizedRow({ with_cdn: 0 })], {
          payerAddress,
        }),
      402,
      `The Filecoin Warm Storage Service deal for payer '${payerAddress}' and the requested content has withCDN=false.`,
    )
  })

  it('throws 403 when the payer is sanctioned', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalCandidates(
          [authorizedRow({ is_sanctioned: 1 })],
          { payerAddress },
        ),
      403,
      `Wallet '${payerAddress}' is sanctioned and cannot retrieve the requested content.`,
    )
  })

  it('throws 404 when no service provider is approved (no service_url)', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalCandidates(
          [authorizedRow({ service_url: null })],
          { payerAddress },
        ),
      404,
      `No approved service provider found for payer '${payerAddress}' and the requested content.`,
    )
  })
})

/** A row with both egress quotas available. */
function quotaRow(overrides = {}) {
  return {
    cdn_egress_quota: '100',
    cache_miss_egress_quota: '100',
    ...overrides,
  }
}

describe('filterCandidatesWithSufficientEgressQuota', () => {
  it('returns the rows unchanged when enforceEgressQuota is false', () => {
    const rows = [
      quotaRow({ cdn_egress_quota: '0', cache_miss_egress_quota: '0' }),
    ]
    expect(
      filterCandidatesWithSufficientEgressQuota(rows, { payerAddress }),
    ).toEqual(rows)
  })

  it('returns the rows when both quotas have budget', () => {
    const rows = [quotaRow()]
    expect(
      filterCandidatesWithSufficientEgressQuota(rows, {
        payerAddress,
        enforceEgressQuota: true,
      }),
    ).toEqual(rows)
  })

  it('throws 402 when the CDN egress quota is exhausted', () => {
    expectHttpError(
      () =>
        filterCandidatesWithSufficientEgressQuota(
          [quotaRow({ cdn_egress_quota: '0' })],
          { payerAddress, enforceEgressQuota: true },
        ),
      402,
      `CDN egress quota exhausted for payer '${payerAddress}' and the requested content. Please top up your CDN egress quota.`,
    )
  })

  it('throws 402 when the cache-miss egress quota is exhausted', () => {
    expectHttpError(
      () =>
        filterCandidatesWithSufficientEgressQuota(
          [quotaRow({ cache_miss_egress_quota: '0' })],
          { payerAddress, enforceEgressQuota: true },
        ),
      402,
      `Cache miss egress quota exhausted for payer '${payerAddress}' and the requested content. Please top up your cache miss egress quota.`,
    )
  })
})
