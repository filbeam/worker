import { BigInt } from '@graphprotocol/graph-ts'
import {
  assert,
  beforeEach,
  clearStore,
  describe,
  test,
} from 'matchstick-as/assembly/index'
import {
  handleUsageReported,
  handleCdnPaymentSettled,
} from '../src/FilBeamOperator'
import {
  createUsageReportedEvent,
  createCdnPaymentSettledEvent,
} from './FilBeamOperator.utils'
import { getEventEntityId } from '../src/utils'

describe('FilBeam Subgraph (FilBeamOperator)', () => {
  beforeEach(() => {
    clearStore()
  })

  test('UsageReported created and stored', () => {
    assert.entityCount('UsageReported', 0)

    const event = createUsageReportedEvent(
      BigInt.fromString('102'),
      BigInt.fromString('1000'),
      BigInt.fromString('2000'),
      BigInt.fromString('5000000'),
      BigInt.fromString('1000000'),
    )

    handleUsageReported(event)

    assert.entityCount('UsageReported', 1)
    const id = getEventEntityId(event)

    assert.fieldEquals('UsageReported', id, 'dataSetId', '102')
    assert.fieldEquals('UsageReported', id, 'fromEpoch', '1000')
    assert.fieldEquals('UsageReported', id, 'toEpoch', '2000')
    assert.fieldEquals('UsageReported', id, 'cdnBytesUsed', '5000000')
    assert.fieldEquals('UsageReported', id, 'cacheMissBytesUsed', '1000000')
    assert.fieldEquals(
      'UsageReported',
      id,
      'transactionHash',
      event.transaction.hash.toHexString(),
    )
  })

  test('CdnPaymentSettled created and stored', () => {
    assert.entityCount('CdnPaymentSettled', 0)

    const event = createCdnPaymentSettledEvent(
      BigInt.fromString('102'),
      BigInt.fromString('999999'),
    )

    handleCdnPaymentSettled(event)

    assert.entityCount('CdnPaymentSettled', 1)
    const id = getEventEntityId(event)

    assert.fieldEquals('CdnPaymentSettled', id, 'dataSetId', '102')
    assert.fieldEquals('CdnPaymentSettled', id, 'cdnAmount', '999999')
    assert.fieldEquals(
      'CdnPaymentSettled',
      id,
      'transactionHash',
      event.transaction.hash.toHexString(),
    )
  })
})
