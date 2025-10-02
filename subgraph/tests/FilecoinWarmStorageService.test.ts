import { BigInt, Bytes, Address } from '@graphprotocol/graph-ts'
import {
  assert,
  beforeEach,
  clearStore,
  describe,
  test,
} from 'matchstick-as/assembly/index'
import {
  handlePieceAdded,
  handleServiceTerminated,
  handleCdnServiceTerminated,
  handleDataSetCreated,
} from '../src/FilecoinWarmStorageService'
import {
  createPieceAddedEvent,
  createServiceTerminatedEvent,
  createCdnServiceTerminatedEvent,
  createDataSetCreatedEvent,
} from './FilecoinWarmStorageService.utils'
import { getEventEntityId } from '../src/utils'

// Tests structure (matchstick-as >=0.5.0)
// https://thegraph.com/docs/en/subgraphs/developing/creating/unit-testing-framework/#tests-structure
// For more test scenarios, see:
// https://thegraph.com/docs/en/subgraphs/developing/creating/unit-testing-framework/#write-a-unit-test
// More assert options:
// https://thegraph.com/docs/en/subgraphs/developing/creating/unit-testing-framework/#asserts

describe('FilBeam Subgraph (FilecoinWarmStorageService)', () => {
  beforeEach(() => {
    clearStore()
  })

  test('PieceAdded created and stored', () => {
    assert.entityCount('PieceAdded', 0)

    const event = createPieceAddedEvent(
      // DataSet ID
      BigInt.fromString('102'),
      // Piece ID
      BigInt.fromString('9001'),
      // Piece CIDs
      Bytes.fromHexString(
        '0x01559120225a03b9ee2f06796d442590e54a4c8a0ac87c2eea0f443a3ca2b87644e1273956e41b',
      ),
      ['metadata-key'],
      ['metadata-value'],
    )

    handlePieceAdded(event)

    assert.entityCount('PieceAdded', 1)
    const id = getEventEntityId(event)

    assert.fieldEquals('PieceAdded', id, 'dataSetId', '102')
    assert.fieldEquals('PieceAdded', id, 'pieceId', '9001')
    assert.fieldEquals(
      'PieceAdded',
      id,
      'pieceCid',
      '0x01559120225a03b9ee2f06796d442590e54a4c8a0ac87c2eea0f443a3ca2b87644e1273956e41b',
    )
    assert.fieldEquals('PieceAdded', id, 'metadataKeys', '[metadata-key]')
    assert.fieldEquals('PieceAdded', id, 'metadataValues', '[metadata-value]')
    assert.fieldEquals(
      'PieceAdded',
      id,
      'transactionHash',
      event.transaction.hash.toHexString(),
    )
  })

  test('ServiceTerminated created and stored', () => {
    assert.entityCount('ServiceTerminated', 0)

    const event = createServiceTerminatedEvent(
      // caller
      Address.fromString('0x1234567890123456789012345678901234567890'),
      // dataSetId
      BigInt.fromString('102'),
      // pdpRailId
      BigInt.fromString('201'),
      // cacheMissRailId
      BigInt.fromString('202'),
      // cdnRailId
      BigInt.fromString('203'),
    )

    handleServiceTerminated(event)

    assert.entityCount('ServiceTerminated', 1)
    const id = getEventEntityId(event)

    assert.fieldEquals(
      'ServiceTerminated',
      id,
      'caller',
      '0x1234567890123456789012345678901234567890',
    )
    assert.fieldEquals('ServiceTerminated', id, 'dataSetId', '102')
    assert.fieldEquals('ServiceTerminated', id, 'pdpRailId', '201')
    assert.fieldEquals('ServiceTerminated', id, 'cacheMissRailId', '202')
    assert.fieldEquals('ServiceTerminated', id, 'cdnRailId', '203')
    assert.fieldEquals(
      'ServiceTerminated',
      id,
      'transactionHash',
      event.transaction.hash.toHexString(),
    )
  })

  test('CdnServiceTerminated created and stored', () => {
    assert.entityCount('CdnServiceTerminated', 0)

    const event = createCdnServiceTerminatedEvent(
      // caller
      Address.fromString('0x1234567890123456789012345678901234567890'),
      // dataSetId
      BigInt.fromString('102'),
      // cacheMissRailId
      BigInt.fromString('302'),
      // cdnRailId
      BigInt.fromString('303'),
    )

    handleCdnServiceTerminated(event)

    assert.entityCount('CdnServiceTerminated', 1)
    const id = getEventEntityId(event)

    assert.fieldEquals(
      'CdnServiceTerminated',
      id,
      'caller',
      '0x1234567890123456789012345678901234567890',
    )
    assert.fieldEquals('CdnServiceTerminated', id, 'dataSetId', '102')
    assert.fieldEquals('CdnServiceTerminated', id, 'cacheMissRailId', '302')
    assert.fieldEquals('CdnServiceTerminated', id, 'cdnRailId', '303')
    assert.fieldEquals(
      'CdnServiceTerminated',
      id,
      'transactionHash',
      event.transaction.hash.toHexString(),
    )
  })

  test('DataSetCreated created and stored', () => {
    assert.entityCount('DataSetCreated', 0)

    const event = createDataSetCreatedEvent(
      // dataSetId
      BigInt.fromString('104'),
      // providerId
      BigInt.fromString('501'),
      // pdpRailId
      BigInt.fromString('301'),
      // cacheMissRailId
      BigInt.fromString('302'),
      // cdnRailId
      BigInt.fromString('303'),
      // payer
      Address.fromString('0x1111111111111111111111111111111111111111'),
      // serviceProvider
      Address.fromString('0x2222222222222222222222222222222222222222'),
      // payee
      Address.fromString('0x3333333333333333333333333333333333333333'),
      // metadataKeys
      ['dataset-key1', 'dataset-key2'],
      // metadataValues
      ['dataset-value1', 'dataset-value2'],
    )

    handleDataSetCreated(event)

    assert.entityCount('DataSetCreated', 1)
    const id = getEventEntityId(event)

    assert.fieldEquals('DataSetCreated', id, 'dataSetId', '104')
    assert.fieldEquals('DataSetCreated', id, 'providerId', '501')
    assert.fieldEquals('DataSetCreated', id, 'pdpRailId', '301')
    assert.fieldEquals('DataSetCreated', id, 'cacheMissRailId', '302')
    assert.fieldEquals('DataSetCreated', id, 'cdnRailId', '303')
    assert.fieldEquals(
      'DataSetCreated',
      id,
      'payer',
      '0x1111111111111111111111111111111111111111',
    )
    assert.fieldEquals(
      'DataSetCreated',
      id,
      'serviceProvider',
      '0x2222222222222222222222222222222222222222',
    )
    assert.fieldEquals(
      'DataSetCreated',
      id,
      'payee',
      '0x3333333333333333333333333333333333333333',
    )
    assert.fieldEquals(
      'DataSetCreated',
      id,
      'metadataKeys',
      '[dataset-key1, dataset-key2]',
    )
    assert.fieldEquals(
      'DataSetCreated',
      id,
      'metadataValues',
      '[dataset-value1, dataset-value2]',
    )
    assert.fieldEquals(
      'DataSetCreated',
      id,
      'transactionHash',
      event.transaction.hash.toHexString(),
    )
  })
})
