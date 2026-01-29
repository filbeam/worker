import { describe, it, expect, beforeEach } from 'vitest'
import { env } from 'cloudflare:test'
import { randomId } from './test-helpers.js'
import { handleCdnPaymentSettled } from '../lib/filbeam-operator-handlers.js'

const FILECOIN_GENESIS_BLOCK_TIMESTAMP_MS = 1667326380000
const TIMESTAMP_AT_BLOCK_2000 = '2022-11-02T10:53:00.000Z'

beforeEach(async () => {
  await env.DB.exec('DELETE FROM data_sets')
})

describe('handleCdnPaymentSettled', () => {
  const testEnv = { ...env, FILECOIN_GENESIS_BLOCK_TIMESTAMP_MS }

  it('creates a row on first call', async () => {
    const dataSetId = randomId()
    await handleCdnPaymentSettled(testEnv, {
      data_set_id: dataSetId,
      block_number: 1000,
    })

    const row = await testEnv.DB.prepare(
      'SELECT id, with_cdn, usage_reported_until, payments_settled_until FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .first()

    expect(row).toEqual({
      id: dataSetId,
      with_cdn: 1,
      usage_reported_until: '1970-01-01T00:00:00.000Z',
      payments_settled_until: '2022-11-02T02:33:00.000Z',
    })
  })

  it('increases value on subsequent call with higher block_number', async () => {
    const dataSetId = randomId()
    await handleCdnPaymentSettled(testEnv, {
      data_set_id: dataSetId,
      block_number: 1000,
    })
    await handleCdnPaymentSettled(testEnv, {
      data_set_id: dataSetId,
      block_number: 2000,
    })

    const row = await testEnv.DB.prepare(
      'SELECT payments_settled_until FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .first()

    expect(row).toEqual({ payments_settled_until: TIMESTAMP_AT_BLOCK_2000 })
  })

  it('does not decrease value when called with lower block_number', async () => {
    const dataSetId = randomId()
    await handleCdnPaymentSettled(testEnv, {
      data_set_id: dataSetId,
      block_number: 2000,
    })
    await handleCdnPaymentSettled(testEnv, {
      data_set_id: dataSetId,
      block_number: 1000,
    })

    const row = await testEnv.DB.prepare(
      'SELECT payments_settled_until FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .first()

    expect(row).toEqual({ payments_settled_until: TIMESTAMP_AT_BLOCK_2000 })
  })

  it('handles different data_set_ids independently', async () => {
    const id1 = randomId()
    const id2 = randomId()
    await handleCdnPaymentSettled(testEnv, {
      data_set_id: id1,
      block_number: 500,
    })
    await handleCdnPaymentSettled(testEnv, {
      data_set_id: id2,
      block_number: 3000,
    })

    const row1 = await testEnv.DB.prepare(
      'SELECT payments_settled_until FROM data_sets WHERE id = ?',
    )
      .bind(id1)
      .first()
    const row2 = await testEnv.DB.prepare(
      'SELECT payments_settled_until FROM data_sets WHERE id = ?',
    )
      .bind(id2)
      .first()

    expect(row1).toEqual({ payments_settled_until: '2022-11-01T22:23:00.000Z' })
    expect(row2).toEqual({ payments_settled_until: '2022-11-02T19:13:00.000Z' })
  })
})
