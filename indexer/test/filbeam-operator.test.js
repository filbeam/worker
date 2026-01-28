import { describe, it, expect, beforeEach } from 'vitest'
import { env } from 'cloudflare:test'
import { randomId } from './test-helpers.js'
import {
  handleUsageReported,
  handleCdnPaymentSettled,
} from '../lib/filbeam-operator-handlers.js'

beforeEach(async () => {
  await env.DB.exec('DELETE FROM data_sets_settlements')
})

describe('handleUsageReported', () => {
  it('creates a row on first call', async () => {
    const dataSetId = randomId()
    await handleUsageReported(env, { data_set_id: dataSetId, to_epoch: '100' })

    const row = await env.DB.prepare(
      'SELECT * FROM data_sets_settlements WHERE data_set_id = ?',
    )
      .bind(dataSetId)
      .first()

    expect(row).toEqual({
      data_set_id: dataSetId,
      usage_reported_until: 100,
      payments_settled_until: 0,
    })
  })

  it('increases value on subsequent call with higher epoch', async () => {
    const dataSetId = randomId()
    await handleUsageReported(env, { data_set_id: dataSetId, to_epoch: '100' })
    await handleUsageReported(env, { data_set_id: dataSetId, to_epoch: '200' })

    const row = await env.DB.prepare(
      'SELECT usage_reported_until FROM data_sets_settlements WHERE data_set_id = ?',
    )
      .bind(dataSetId)
      .first()

    expect(row).toEqual({ usage_reported_until: 200 })
  })

  it('does not decrease value when called with lower epoch', async () => {
    const dataSetId = randomId()
    await handleUsageReported(env, { data_set_id: dataSetId, to_epoch: '200' })
    await handleUsageReported(env, { data_set_id: dataSetId, to_epoch: '100' })

    const row = await env.DB.prepare(
      'SELECT usage_reported_until FROM data_sets_settlements WHERE data_set_id = ?',
    )
      .bind(dataSetId)
      .first()

    expect(row).toEqual({ usage_reported_until: 200 })
  })

  it('handles different data_set_ids independently', async () => {
    const id1 = randomId()
    const id2 = randomId()
    await handleUsageReported(env, { data_set_id: id1, to_epoch: '50' })
    await handleUsageReported(env, { data_set_id: id2, to_epoch: '300' })

    const row1 = await env.DB.prepare(
      'SELECT usage_reported_until FROM data_sets_settlements WHERE data_set_id = ?',
    )
      .bind(id1)
      .first()
    const row2 = await env.DB.prepare(
      'SELECT usage_reported_until FROM data_sets_settlements WHERE data_set_id = ?',
    )
      .bind(id2)
      .first()

    expect(row1).toEqual({ usage_reported_until: 50 })
    expect(row2).toEqual({ usage_reported_until: 300 })
  })
})

describe('handleCdnPaymentSettled', () => {
  it('creates a row on first call', async () => {
    const dataSetId = randomId()
    await handleCdnPaymentSettled(env, {
      data_set_id: dataSetId,
      block_number: 1000,
    })

    const row = await env.DB.prepare(
      'SELECT * FROM data_sets_settlements WHERE data_set_id = ?',
    )
      .bind(dataSetId)
      .first()

    expect(row).toEqual({
      data_set_id: dataSetId,
      usage_reported_until: 0,
      payments_settled_until: 1000,
    })
  })

  it('increases value on subsequent call with higher block_number', async () => {
    const dataSetId = randomId()
    await handleCdnPaymentSettled(env, {
      data_set_id: dataSetId,
      block_number: 1000,
    })
    await handleCdnPaymentSettled(env, {
      data_set_id: dataSetId,
      block_number: 2000,
    })

    const row = await env.DB.prepare(
      'SELECT payments_settled_until FROM data_sets_settlements WHERE data_set_id = ?',
    )
      .bind(dataSetId)
      .first()

    expect(row).toEqual({ payments_settled_until: 2000 })
  })

  it('does not decrease value when called with lower block_number', async () => {
    const dataSetId = randomId()
    await handleCdnPaymentSettled(env, {
      data_set_id: dataSetId,
      block_number: 2000,
    })
    await handleCdnPaymentSettled(env, {
      data_set_id: dataSetId,
      block_number: 1000,
    })

    const row = await env.DB.prepare(
      'SELECT payments_settled_until FROM data_sets_settlements WHERE data_set_id = ?',
    )
      .bind(dataSetId)
      .first()

    expect(row).toEqual({ payments_settled_until: 2000 })
  })

  it('handles different data_set_ids independently', async () => {
    const id1 = randomId()
    const id2 = randomId()
    await handleCdnPaymentSettled(env, { data_set_id: id1, block_number: 500 })
    await handleCdnPaymentSettled(env, { data_set_id: id2, block_number: 3000 })

    const row1 = await env.DB.prepare(
      'SELECT payments_settled_until FROM data_sets_settlements WHERE data_set_id = ?',
    )
      .bind(id1)
      .first()
    const row2 = await env.DB.prepare(
      'SELECT payments_settled_until FROM data_sets_settlements WHERE data_set_id = ?',
    )
      .bind(id2)
      .first()

    expect(row1).toEqual({ payments_settled_until: 500 })
    expect(row2).toEqual({ payments_settled_until: 3000 })
  })
})
