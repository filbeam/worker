import { expect } from 'vitest'

/**
 * Generate a random ID for testing
 *
 * @returns {string}
 */
export function randomId() {
  return Math.floor(Math.random() * 1000000000).toString()
}

/**
 * @param {string | null} sqliteDateString
 * @param {string} [message]
 */
export function assertCloseToNow(sqliteDateString, message = 'timestamp') {
  expect(sqliteDateString, message).not.toBeNull()
  // D1 returns dates as UTC without timezone info, append 'Z' to parse as UTC if needed
  const hasTimezone = /([+-]\d{2}:\d{2}|Z)$/i.test(sqliteDateString)
  const date = new Date(hasTimezone ? sqliteDateString : sqliteDateString + 'Z')
  // Assert that the timestamp is within 5 seconds of now
  expect(date, message).toBeCloseTo(new Date(), -4)
}

/**
 * Create a test service provider
 *
 * @param {any} env
 * @param {{
 *   serviceProviderId?: string
 *   serviceUrl?: string
 * }} options
 * @returns {Promise<string>}
 */
export async function withServiceProvider(
  env,
  { serviceProviderId = randomId(), serviceUrl = 'https://example.com' } = {},
) {
  await env.DB.prepare(
    `INSERT OR IGNORE INTO service_providers (id, service_url) VALUES (?, ?)`,
  )
    .bind(String(serviceProviderId), serviceUrl)
    .run()

  return serviceProviderId
}

/**
 * Create a test data set
 *
 * @param {any} env
 * @param {{
 *   dataSetId?: string
 *   withCDN?: boolean
 *   withIPFSIndexing?: boolean
 *   serviceProviderId?: string
 *   payerAddress?: string
 *   cdnEgressQuota?: number
 *   cacheMissEgressQuota?: number
 * }} options
 * @returns {Promise<string>}
 */
export async function withDataSet(
  env,
  {
    dataSetId = randomId(),
    withCDN = true,
    withIPFSIndexing = false,
    serviceProviderId,
    payerAddress,
    cdnEgressQuota = 0,
    cacheMissEgressQuota = 0,
  } = {},
) {
  await env.DB.prepare(
    `
    INSERT INTO data_sets (
      id,
      with_cdn,
      with_ipfs_indexing,
      service_provider_id,
      payer_address
    )
    VALUES (?, ?, ?, ?, ?)`,
  )
    .bind(
      String(dataSetId),
      withCDN,
      withIPFSIndexing,
      serviceProviderId,
      payerAddress,
    )
    .run()

  // Insert quotas into the separate table if they're provided
  if (cdnEgressQuota > 0 || cacheMissEgressQuota > 0) {
    await env.DB.prepare(
      `
      INSERT INTO data_set_egress_quotas (
        data_set_id,
        cdn_egress_quota,
        cache_miss_egress_quota
      )
      VALUES (?, ?, ?)`,
    )
      .bind(String(dataSetId), cdnEgressQuota, cacheMissEgressQuota)
      .run()
  }

  return dataSetId
}

export async function withPieces(
  env,
  dataSetId,
  pieceIds,
  pieceCids,
  ipfsRootCids = [],
) {
  await env.DB.prepare(
    `
    INSERT INTO pieces (
      id,
      data_set_id,
      cid,
      ipfs_root_cid
    )
    VALUES ${new Array(pieceIds.length)
      .fill(null)
      .map(() => '(?, ?, ?, ?)')
      .join(', ')}
    ON CONFLICT DO NOTHING
  `,
  )
    .bind(
      ...pieceIds.flatMap((pieceId, i) => [
        String(pieceId),
        String(dataSetId),
        pieceCids[i],
        ipfsRootCids[i] || null,
      ]),
    )
    .run()
}
