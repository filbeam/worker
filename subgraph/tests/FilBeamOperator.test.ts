import { BigInt } from '@graphprotocol/graph-ts'
import {
  assert,
  beforeEach,
  clearStore,
  describe,
  test,
} from 'matchstick-as/assembly/index'
import { handleCdnPaymentSettled } from '../src/FilBeamOperator'
import { createCdnPaymentSettledEvent } from './FilBeamOperator.utils'
import { getEventEntityId } from '../src/utils'

describe('FilBeam Subgraph (FilBeamOperator)', () => {
  beforeEach(() => {
    clearStore()
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
