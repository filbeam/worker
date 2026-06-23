import { describe, it, expect } from 'vitest'
import {
  filterAuthorizedRetrievalCandidates,
  buildRetrievalCandidateQuery,
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
    cdn_egress_quota: '100',
    cache_miss_egress_quota: '100',
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

  it('does not check egress quota when enforceEgressQuota is false', () => {
    const rows = [
      authorizedRow({ cdn_egress_quota: '0', cache_miss_egress_quota: '0' }),
    ]
    expect(filterAuthorizedRetrievalCandidates(rows, { payerAddress })).toEqual(
      rows,
    )
  })

  it('returns the rows when enforceEgressQuota is set and both quotas have budget', () => {
    const rows = [authorizedRow()]
    expect(
      filterAuthorizedRetrievalCandidates(rows, {
        payerAddress,
        enforceEgressQuota: true,
      }),
    ).toEqual(rows)
  })

  it('throws 402 when enforcing and the CDN egress quota is exhausted', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalCandidates(
          [authorizedRow({ cdn_egress_quota: '0' })],
          { payerAddress, enforceEgressQuota: true },
        ),
      402,
      `CDN egress quota exhausted for payer '${payerAddress}' and the requested content. Please top up your CDN egress quota.`,
    )
  })

  it('throws 402 when enforcing and the cache-miss egress quota is exhausted', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalCandidates(
          [authorizedRow({ cache_miss_egress_quota: '0' })],
          { payerAddress, enforceEgressQuota: true },
        ),
      402,
      `Cache miss egress quota exhausted for payer '${payerAddress}' and the requested content. Please top up your cache miss egress quota.`,
    )
  })
})

describe('buildRetrievalCandidateQuery', () => {
  it('selects the cascade columns, the joins, and the given where clause', () => {
    const query = buildRetrievalCandidateQuery({
      where: 'pieces.cid = ?',
    })

    for (const column of [
      'pieces.data_set_id',
      'data_sets.service_provider_id',
      'data_sets.payer_address',
      'data_sets.with_cdn',
      'data_set_egress_quotas.cdn_egress_quota',
      'data_set_egress_quotas.cache_miss_egress_quota',
      'service_providers.service_url',
      'service_providers.is_deleted AS service_provider_is_deleted',
      'wallet_details.is_sanctioned',
    ]) {
      expect(query).toContain(column)
    }
    expect(query).toContain('FROM pieces')
    expect(query).toContain(
      'LEFT OUTER JOIN data_sets\n      ON pieces.data_set_id = data_sets.id',
    )
    expect(query).toContain('WHERE pieces.cid = ?')
  })

  it('appends the extra columns', () => {
    const query = buildRetrievalCandidateQuery({
      extraColumns: ['pieces.id AS piece_id', 'pieces.ipfs_root_cid'],
      where: 'pieces.ipfs_root_cid = ?',
    })

    expect(query).toContain('pieces.id AS piece_id')
    expect(query).toContain('pieces.ipfs_root_cid')
    expect(query).toContain('WHERE pieces.ipfs_root_cid = ?')
  })
})
