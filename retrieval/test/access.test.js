import { describe, it, expect } from 'vitest'
import { filterAuthorizedRetrievalCandidates } from '../lib/access.js'

const payerAddress = '0xabcdef'
const subject = "IPFS Root CID 'bafktest'"

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
    expect(
      filterAuthorizedRetrievalCandidates(rows, { payerAddress, subject }),
    ).toEqual(rows)
  })

  it('throws 404 when there are no rows', () => {
    expectHttpError(
      () => filterAuthorizedRetrievalCandidates([], { payerAddress, subject }),
      404,
      `${subject} does not exist or may not have been indexed yet.`,
    )
  })

  it('throws 404 when no row has a service provider', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalCandidates(
          [authorizedRow({ service_provider_id: null })],
          { payerAddress, subject },
        ),
      404,
      `${subject} exists but has no associated service provider.`,
    )
  })

  it('always excludes soft-deleted service providers', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalCandidates(
          [authorizedRow({ service_provider_is_deleted: 1 })],
          { payerAddress, subject },
        ),
      404,
      `${subject} exists but has no associated service provider.`,
    )
  })

  it('throws 402 when the payer has no payment rail', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalCandidates(
          [authorizedRow({ payer_address: '0xother' })],
          { payerAddress, subject },
        ),
      402,
      `There is no Filecoin Warm Storage Service deal for payer '${payerAddress}' and ${subject}.`,
    )
  })

  it('throws 402 when CDN is disabled', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalCandidates([authorizedRow({ with_cdn: 0 })], {
          payerAddress,
          subject,
        }),
      402,
      `The Filecoin Warm Storage Service deal for payer '${payerAddress}' and ${subject} has withCDN=false.`,
    )
  })

  it('throws 403 when the payer is sanctioned', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalCandidates(
          [authorizedRow({ is_sanctioned: 1 })],
          { payerAddress, subject },
        ),
      403,
      `Wallet '${payerAddress}' is sanctioned and cannot retrieve ${subject}.`,
    )
  })

  it('throws 404 when no service provider is approved (no service_url)', () => {
    expectHttpError(
      () =>
        filterAuthorizedRetrievalCandidates(
          [authorizedRow({ service_url: null })],
          { payerAddress, subject },
        ),
      404,
      `No approved service provider found for payer '${payerAddress}' and ${subject}.`,
    )
  })
})
