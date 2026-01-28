import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { env } from 'cloudflare:test'
import workerImpl from '../bin/indexer.js'

describe('reportSettlementStats', () => {
  let writeDataPoint
  let testEnv

  beforeEach(() => {
    writeDataPoint = vi.fn()
    testEnv = {
      ...env,
      SETTLEMENT_STATS: { writeDataPoint },
      FILECOIN_GENESIS_BLOCK_TIMESTAMP_MS: 1667326380000,
    }
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('writes data point for data set with oldest unsettled usage', async () => {
    await testEnv.DB.prepare(
      `INSERT INTO data_sets_settlements (data_set_id, usage_reported_until, payments_settled_until) VALUES ('ds-1', 200, 100)`,
    ).run()

    await workerImpl.reportSettlementStats(testEnv)

    expect(writeDataPoint).toHaveBeenCalledWith({
      doubles: [100 * 30 * 1000 + 1667326380000],
      blobs: ['ds-1'],
    })
  })

  it('picks the data set with the oldest payments_settled_until', async () => {
    await testEnv.DB.prepare(
      `INSERT INTO data_sets_settlements (data_set_id, usage_reported_until, payments_settled_until) VALUES ('ds-old', 300, 50)`,
    ).run()
    await testEnv.DB.prepare(
      `INSERT INTO data_sets_settlements (data_set_id, usage_reported_until, payments_settled_until) VALUES ('ds-new', 300, 200)`,
    ).run()

    await workerImpl.reportSettlementStats(testEnv)

    expect(writeDataPoint).toHaveBeenCalledWith({
      doubles: [50 * 30 * 1000 + 1667326380000],
      blobs: ['ds-old'],
    })
  })

  it('does not write data point when no unsettled usage exists', async () => {
    await testEnv.DB.prepare(
      `INSERT INTO data_sets_settlements (data_set_id, usage_reported_until, payments_settled_until) VALUES ('ds-settled', 100, 100)`,
    ).run()

    await workerImpl.reportSettlementStats(testEnv)

    expect(writeDataPoint).not.toHaveBeenCalled()
  })

  it('does not write data point when table is empty', async () => {
    await workerImpl.reportSettlementStats(testEnv)

    expect(writeDataPoint).not.toHaveBeenCalled()
  })
})
