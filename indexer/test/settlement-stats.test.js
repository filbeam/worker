import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { env } from 'cloudflare:test'
import workerImpl from '../bin/indexer.js'

describe('reportSettlementStats', () => {
  let writeDataPoint
  let testEnv

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2022-11-02T12:00:00.000Z'))
    writeDataPoint = vi.fn()
    testEnv = {
      ...env,
      SETTLEMENT_STATS: { writeDataPoint },
    }
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('writes data point for data set with oldest unsettled usage', async () => {
    await testEnv.DB.prepare(
      `INSERT INTO data_sets (id, with_cdn, usage_reported_until, cdn_payments_settled_until) VALUES ('ds-1', FALSE, '2022-11-02T00:00:00.000Z', '2022-11-01T12:00:00.000Z')`,
    ).run()

    await workerImpl.reportSettlementStats(testEnv)

    const usageReportedUntilMs = new Date('2022-11-02T00:00:00.000Z').getTime()
    const nowMs = new Date('2022-11-02T12:00:00.000Z').getTime()
    expect(writeDataPoint).toHaveBeenCalledWith({
      doubles: [usageReportedUntilMs, nowMs - usageReportedUntilMs],
      blobs: ['ds-1'],
    })
  })

  it('picks the data set with the oldest usage_reported_until', async () => {
    await testEnv.DB.prepare(
      `INSERT INTO data_sets (id, with_cdn, usage_reported_until, cdn_payments_settled_until) VALUES ('ds-old', FALSE, '2022-11-01T00:00:00.000Z', '2022-10-31T00:00:00.000Z')`,
    ).run()
    await testEnv.DB.prepare(
      `INSERT INTO data_sets (id, with_cdn, usage_reported_until, cdn_payments_settled_until) VALUES ('ds-new', FALSE, '2022-11-02T00:00:00.000Z', '2022-11-01T12:00:00.000Z')`,
    ).run()

    await workerImpl.reportSettlementStats(testEnv)

    expect(writeDataPoint).toHaveBeenCalledWith({
      doubles: [
        new Date('2022-11-01T00:00:00.000Z').getTime(),
        expect.any(Number),
      ],
      blobs: ['ds-old'],
    })
  })

  it('calculates lag from usage_reported_until, not cdn_payments_settled_until', async () => {
    vi.setSystemTime(new Date('2026-02-05T15:18:57.908Z'))

    await testEnv.DB.prepare(
      `INSERT INTO data_sets (id, with_cdn, usage_reported_until, cdn_payments_settled_until) VALUES ('ds-11322', FALSE, '2026-02-05T00:29:30.000Z', '1970-01-01T00:00:00.000Z')`,
    ).run()

    await workerImpl.reportSettlementStats(testEnv)

    const usageReportedUntilMs = new Date('2026-02-05T00:29:30.000Z').getTime()
    const nowMs = new Date('2026-02-05T15:18:57.908Z').getTime()
    expect(writeDataPoint).toHaveBeenCalledWith({
      doubles: [usageReportedUntilMs, nowMs - usageReportedUntilMs],
      blobs: ['ds-11322'],
    })
  })

  it('writes "no lag" data point when no unsettled usage exists', async () => {
    await testEnv.DB.prepare(
      `INSERT INTO data_sets (id, with_cdn, usage_reported_until, cdn_payments_settled_until) VALUES ('ds-settled', FALSE, '2022-11-01T12:00:00.000Z', '2022-11-01T12:00:00.000Z')`,
    ).run()

    await workerImpl.reportSettlementStats(testEnv)

    const nowMs = new Date('2022-11-02T12:00:00.000Z').getTime()
    expect(writeDataPoint).toHaveBeenCalledWith({
      doubles: [nowMs, 0],
      blobs: [''],
    })
  })

  it('writes "no lag" data point when table is empty', async () => {
    await workerImpl.reportSettlementStats(testEnv)

    const nowMs = new Date('2022-11-02T12:00:00.000Z').getTime()
    expect(writeDataPoint).toHaveBeenCalledWith({
      doubles: [nowMs, 0],
      blobs: [''],
    })
  })
})
