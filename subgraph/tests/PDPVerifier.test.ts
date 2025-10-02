import { BigInt } from '@graphprotocol/graph-ts'
import {
  assert,
  beforeEach,
  clearStore,
  describe,
  test,
} from 'matchstick-as/assembly/index'
import { handlePiecesRemoved } from '../src/PDPVerifier'
import { getEventEntityId } from '../src/utils'
import { createPiecesRemovedEvent } from './PDPVerifier.utils'

// Tests structure (matchstick-as >=0.5.0)
// https://thegraph.com/docs/en/subgraphs/developing/creating/unit-testing-framework/#tests-structure
// For more test scenarios, see:
// https://thegraph.com/docs/en/subgraphs/developing/creating/unit-testing-framework/#write-a-unit-test
// More assert options:
// https://thegraph.com/docs/en/subgraphs/developing/creating/unit-testing-framework/#asserts

describe('FilBeam Subgraph (PDPVerifier)', () => {
  beforeEach(() => {
    clearStore()
  })

  test('PiecesRemoved created and stored', () => {
    assert.entityCount('PiecesRemoved', 0)

    const event = createPiecesRemovedEvent(BigInt.fromString('103'), [
      // Piece IDs
      BigInt.fromString('9003'),
      BigInt.fromString('9004'),
    ])

    handlePiecesRemoved(event)

    assert.entityCount('PiecesRemoved', 1)
    const id = getEventEntityId(event)

    assert.fieldEquals('PiecesRemoved', id, 'dataSetId', '103')
    assert.fieldEquals('PiecesRemoved', id, 'pieceIds', '[9003, 9004]')
    assert.fieldEquals(
      'PiecesRemoved',
      id,
      'transactionHash',
      event.transaction.hash.toHexString(),
    )
  })
})
