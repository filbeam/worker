import { describe, it, expect } from 'vitest'
import { updateDataSetStats } from '../lib/stats'
import { withDataSet } from './test-helpers'
import { env } from 'cloudflare:test'

describe('updateDataSetStats', () => {
  it('updates egress stats', async () => {
    const DATA_SET_ID = 'test-data-set-1'
    const EGRESS_BYTES = 123456

    await withDataSet(env, {
      dataSetId: DATA_SET_ID,
      cdnEgressQuota: 100,
      cacheMissEgressQuota: 100,
    })
    await updateDataSetStats(env, {
      dataSetId: DATA_SET_ID,
      egressBytes: EGRESS_BYTES,
    })

    const { results: insertResults } = await env.DB.prepare(
      `SELECT id, total_egress_bytes_used 
       FROM data_sets
       WHERE id = ?`,
    )
      .bind(DATA_SET_ID)
      .all()

    expect(insertResults).toEqual([
      {
        id: DATA_SET_ID,
        total_egress_bytes_used: EGRESS_BYTES,
      },
    ])

    // Update the egress stats
    await updateDataSetStats(env, {
      dataSetId: DATA_SET_ID,
      egressBytes: 1000,
    })

    const { results: updateResults } = await env.DB.prepare(
      `SELECT id, total_egress_bytes_used 
       FROM data_sets 
       WHERE id = ?`,
    )
      .bind(DATA_SET_ID)
      .all()

    expect(updateResults).toEqual([
      {
        id: DATA_SET_ID,
        total_egress_bytes_used: EGRESS_BYTES + 1000,
      },
    ])
  })

  it('does not decrement quotas when enforceEgressQuota is false', async () => {
    const DATA_SET_ID = 'test-data-set-no-enforce'
    const EGRESS_BYTES = 100
    const initialCdnQuota = 500
    const initialCacheMissQuota = 300

    await withDataSet(env, {
      dataSetId: DATA_SET_ID,
      cdnEgressQuota: initialCdnQuota,
      cacheMissEgressQuota: initialCacheMissQuota,
    })

    // Test with cache hit (cacheMiss = false)
    await updateDataSetStats(env, {
      dataSetId: DATA_SET_ID,
      egressBytes: EGRESS_BYTES,
      cacheMiss: false,
      enforceEgressQuota: false,
    })

    const dataSetResult = await env.DB.prepare(
      `SELECT total_egress_bytes_used FROM data_sets WHERE id = ?`,
    )
      .bind(DATA_SET_ID)
      .first()

    const quotaResult = await env.DB.prepare(
      `SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_set_egress_quotas WHERE data_set_id = ?`,
    )
      .bind(DATA_SET_ID)
      .first()

    expect(dataSetResult.total_egress_bytes_used).toBe(EGRESS_BYTES)
    expect(quotaResult.cdn_egress_quota).toBe(initialCdnQuota)
    expect(quotaResult.cache_miss_egress_quota).toBe(initialCacheMissQuota)

    // Test with cache miss (cacheMiss = true)
    await updateDataSetStats(env, {
      dataSetId: DATA_SET_ID,
      egressBytes: EGRESS_BYTES,
      cacheMiss: true,
      enforceEgressQuota: false,
    })

    const dataSetResult2 = await env.DB.prepare(
      `SELECT total_egress_bytes_used FROM data_sets WHERE id = ?`,
    )
      .bind(DATA_SET_ID)
      .first()

    const quotaResult2 = await env.DB.prepare(
      `SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_set_egress_quotas WHERE data_set_id = ?`,
    )
      .bind(DATA_SET_ID)
      .first()

    expect(dataSetResult2.total_egress_bytes_used).toBe(EGRESS_BYTES * 2)
    expect(quotaResult2.cdn_egress_quota).toBe(initialCdnQuota)
    expect(quotaResult2.cache_miss_egress_quota).toBe(initialCacheMissQuota)
  })

  it('decrements quotas when enforceEgressQuota is true', async () => {
    const DATA_SET_ID = 'test-data-set-enforce'
    const EGRESS_BYTES = 100
    const initialCdnQuota = 500
    const initialCacheMissQuota = 300

    await withDataSet(env, {
      dataSetId: DATA_SET_ID,
      cdnEgressQuota: initialCdnQuota,
      cacheMissEgressQuota: initialCacheMissQuota,
    })

    // Test with cache hit (cacheMiss = false)
    await updateDataSetStats(env, {
      dataSetId: DATA_SET_ID,
      egressBytes: EGRESS_BYTES,
      cacheMiss: false,
      enforceEgressQuota: true,
    })

    const dataSetResult = await env.DB.prepare(
      `SELECT total_egress_bytes_used FROM data_sets WHERE id = ?`,
    )
      .bind(DATA_SET_ID)
      .first()

    const quotaResult = await env.DB.prepare(
      `SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_set_egress_quotas WHERE data_set_id = ?`,
    )
      .bind(DATA_SET_ID)
      .first()

    expect(dataSetResult.total_egress_bytes_used).toBe(EGRESS_BYTES)
    expect(quotaResult.cdn_egress_quota).toBe(initialCdnQuota - EGRESS_BYTES)
    expect(quotaResult.cache_miss_egress_quota).toBe(initialCacheMissQuota)

    // Test with cache miss (cacheMiss = true)
    await updateDataSetStats(env, {
      dataSetId: DATA_SET_ID,
      egressBytes: EGRESS_BYTES,
      cacheMiss: true,
      enforceEgressQuota: true,
    })

    const dataSetResult2 = await env.DB.prepare(
      `SELECT total_egress_bytes_used FROM data_sets WHERE id = ?`,
    )
      .bind(DATA_SET_ID)
      .first()

    const quotaResult2 = await env.DB.prepare(
      `SELECT cdn_egress_quota, cache_miss_egress_quota FROM data_set_egress_quotas WHERE data_set_id = ?`,
    )
      .bind(DATA_SET_ID)
      .first()

    expect(dataSetResult2.total_egress_bytes_used).toBe(EGRESS_BYTES * 2)
    expect(quotaResult2.cdn_egress_quota).toBe(
      initialCdnQuota - EGRESS_BYTES * 2,
    )
    expect(quotaResult2.cache_miss_egress_quota).toBe(
      initialCacheMissQuota - EGRESS_BYTES,
    )
  })
})
