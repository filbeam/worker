import { describe, it, expect, beforeEach } from 'vitest'
import worker from '../bin/stats-api.js'
import { env } from 'cloudflare:test'

describe('stats-api.fetch', () => {
  beforeEach(async () => {
    await env.DB.batch([env.DB.prepare('DELETE FROM data_sets')])
  })

  it('returns cdn and cache miss egress quota for existing data set', async () => {
    const dataSetId = '1'
    await env.DB.prepare(
      'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
    )
      .bind(dataSetId, '1', '0xPayerAddress', true, 1000000, 2000000)
      .run()

    const req = new Request(`https://example.com/stats/${dataSetId}`)
    const res = await worker.fetch(req, env)

    expect(res.status).toBe(200)
    expect(res.headers.get('Content-Type')).toBe('application/json')

    const data = await res.json()
    expect(data).toStrictEqual({
      cdn_egress_quota: '1000000',
      cache_miss_egress_quota: '2000000',
    })
  })

  it('returns 404 for non-existent data set', async () => {
    const req = new Request('https://example.com/stats/1337')
    const res = await worker.fetch(req, env)

    expect(res.status).toBe(404)
    const text = await res.text()
    expect(text).toBe('Not Found')
  })

  it('returns 405 for POST method', async () => {
    const req = new Request('https://example.com/stats/1337', {
      method: 'POST',
      body: JSON.stringify({ test: 'data' }),
      headers: { 'Content-Type': 'application/json' },
    })
    const res = await worker.fetch(req, env)

    expect(res.status).toBe(405)
    expect(res.headers.get('Allow')).toBe('GET')
    const text = await res.text()
    expect(text).toBe('Method Not Allowed')
  })

  it('returns 405 for PUT method', async () => {
    const req = new Request('https://example.com/stats/1337', {
      method: 'PUT',
      body: JSON.stringify({ test: 'data' }),
      headers: { 'Content-Type': 'application/json' },
    })
    const res = await worker.fetch(req, env)

    expect(res.status).toBe(405)
    expect(res.headers.get('Allow')).toBe('GET')
    const text = await res.text()
    expect(text).toBe('Method Not Allowed')
  })

  it('returns 405 for DELETE method', async () => {
    const req = new Request('https://example.com/stats/1337', {
      method: 'DELETE',
    })
    const res = await worker.fetch(req, env)

    expect(res.status).toBe(405)
    expect(res.headers.get('Allow')).toBe('GET')
    const text = await res.text()
    expect(text).toBe('Method Not Allowed')
  })

  it('returns 404 for invalid path format', async () => {
    const req = new Request('https://example.com/invalid/path/format')
    const res = await worker.fetch(req, env)

    expect(res.status).toBe(404)
    const text = await res.text()
    expect(text).toBe('Not Found')
  })

  it('returns 404 for root path', async () => {
    const req = new Request('https://example.com/')
    const res = await worker.fetch(req, env)

    expect(res.status).toBe(404)
    const text = await res.text()
    expect(text).toBe('Not Found')
  })

  it('handles null egress quota values as zero', async () => {
    const dataSetId = '2'
    await env.DB.prepare(
      'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
    )
      .bind(dataSetId, '2', '0xPayerAddress2', false, null, null)
      .run()

    const req = new Request(`https://example.com/stats/${dataSetId}`)
    const res = await worker.fetch(req, env)

    expect(res.status).toBe(200)

    const data = await res.json()
    expect(data).toStrictEqual({
      cdn_egress_quota: '0',
      cache_miss_egress_quota: '0',
    })
  })

  it('handles zero egress quota values correctly', async () => {
    const dataSetId = '3'
    await env.DB.prepare(
      'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
    )
      .bind(dataSetId, '3', '0xPayerAddress3', true, 0, 0)
      .run()

    const req = new Request(`https://example.com/stats/${dataSetId}`)
    const res = await worker.fetch(req, env)

    expect(res.status).toBe(200)

    const data = await res.json()
    expect(data).toStrictEqual({
      cdn_egress_quota: '0',
      cache_miss_egress_quota: '0',
    })
  })

  it('handles large integer values correctly', async () => {
    const dataSetId = '4'
    const largeCdnQuota = 999999999999
    const largeCacheMissQuota = 888888888888

    await env.DB.prepare(
      'INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, cdn_egress_quota, cache_miss_egress_quota) VALUES (?, ?, ?, ?, ?, ?)',
    )
      .bind(
        dataSetId,
        '4',
        '0xPayerAddress4',
        true,
        largeCdnQuota,
        largeCacheMissQuota,
      )
      .run()

    const req = new Request(`https://example.com/stats/${dataSetId}`)
    const res = await worker.fetch(req, env)

    expect(res.status).toBe(200)

    const data = await res.json()
    expect(data).toStrictEqual({
      cdn_egress_quota: '999999999999',
      cache_miss_egress_quota: '888888888888',
    })
  })
})
