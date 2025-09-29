import { newMockEvent } from 'matchstick-as'
import { ethereum, BigInt, Bytes, Address } from '@graphprotocol/graph-ts'
import {
  PieceAdded,
  ServiceTerminated,
  CDNServiceTerminated,
  DataSetCreated,
} from '../generated/FilecoinWarmStorageService/FilecoinWarmStorageService'

export function createPieceAddedEvent(
  setId: BigInt,
  pieceId: BigInt,
  pieceCid: Bytes,
  keys: Array<string>,
  values: Array<string>,
): PieceAdded {
  const pieceAddedEvent = changetype<PieceAdded>(newMockEvent())

  pieceAddedEvent.parameters = []

  pieceAddedEvent.parameters.push(
    new ethereum.EventParam('setId', ethereum.Value.fromUnsignedBigInt(setId)),
  )
  pieceAddedEvent.parameters.push(
    new ethereum.EventParam(
      'pieceId',
      ethereum.Value.fromUnsignedBigInt(pieceId),
    ),
  )

  const cidTuple = new ethereum.Tuple(1)
  cidTuple[0] = ethereum.Value.fromBytes(pieceCid)

  pieceAddedEvent.parameters.push(
    new ethereum.EventParam('pieceCid', ethereum.Value.fromTuple(cidTuple)),
  )

  pieceAddedEvent.parameters.push(
    new ethereum.EventParam('keys', ethereum.Value.fromStringArray(keys)),
  )

  pieceAddedEvent.parameters.push(
    new ethereum.EventParam('values', ethereum.Value.fromStringArray(values)),
  )

  return pieceAddedEvent
}

export function createServiceTerminatedEvent(
  caller: Address,
  dataSetId: BigInt,
  pdpRailId: BigInt,
  cacheMissRailId: BigInt,
  cdnRailId: BigInt,
): ServiceTerminated {
  const serviceTerminatedEvent = changetype<ServiceTerminated>(newMockEvent())

  serviceTerminatedEvent.parameters = []

  serviceTerminatedEvent.parameters.push(
    new ethereum.EventParam('caller', ethereum.Value.fromAddress(caller)),
  )
  serviceTerminatedEvent.parameters.push(
    new ethereum.EventParam(
      'dataSetId',
      ethereum.Value.fromUnsignedBigInt(dataSetId),
    ),
  )
  serviceTerminatedEvent.parameters.push(
    new ethereum.EventParam(
      'pdpRailId',
      ethereum.Value.fromUnsignedBigInt(pdpRailId),
    ),
  )
  serviceTerminatedEvent.parameters.push(
    new ethereum.EventParam(
      'cacheMissRailId',
      ethereum.Value.fromUnsignedBigInt(cacheMissRailId),
    ),
  )
  serviceTerminatedEvent.parameters.push(
    new ethereum.EventParam(
      'cdnRailId',
      ethereum.Value.fromUnsignedBigInt(cdnRailId),
    ),
  )

  return serviceTerminatedEvent
}

export function createCdnServiceTerminatedEvent(
  caller: Address,
  dataSetId: BigInt,
  cacheMissRailId: BigInt,
  cdnRailId: BigInt,
): CDNServiceTerminated {
  const cdnServiceTerminatedEvent =
    changetype<CDNServiceTerminated>(newMockEvent())

  cdnServiceTerminatedEvent.parameters = []

  cdnServiceTerminatedEvent.parameters.push(
    new ethereum.EventParam('caller', ethereum.Value.fromAddress(caller)),
  )
  cdnServiceTerminatedEvent.parameters.push(
    new ethereum.EventParam(
      'dataSetId',
      ethereum.Value.fromUnsignedBigInt(dataSetId),
    ),
  )
  cdnServiceTerminatedEvent.parameters.push(
    new ethereum.EventParam(
      'cacheMissRailId',
      ethereum.Value.fromUnsignedBigInt(cacheMissRailId),
    ),
  )
  cdnServiceTerminatedEvent.parameters.push(
    new ethereum.EventParam(
      'cdnRailId',
      ethereum.Value.fromUnsignedBigInt(cdnRailId),
    ),
  )

  return cdnServiceTerminatedEvent
}

export function createDataSetCreatedEvent(
  dataSetId: BigInt,
  providerId: BigInt,
  pdpRailId: BigInt,
  cacheMissRailId: BigInt,
  cdnRailId: BigInt,
  payer: Address,
  serviceProvider: Address,
  payee: Address,
  metadataKeys: Array<string>,
  metadataValues: Array<string>,
): DataSetCreated {
  const dataSetCreatedEvent = changetype<DataSetCreated>(newMockEvent())

  dataSetCreatedEvent.parameters = []

  dataSetCreatedEvent.parameters.push(
    new ethereum.EventParam(
      'dataSetId',
      ethereum.Value.fromUnsignedBigInt(dataSetId),
    ),
  )
  dataSetCreatedEvent.parameters.push(
    new ethereum.EventParam(
      'providerId',
      ethereum.Value.fromUnsignedBigInt(providerId),
    ),
  )
  dataSetCreatedEvent.parameters.push(
    new ethereum.EventParam(
      'pdpRailId',
      ethereum.Value.fromUnsignedBigInt(pdpRailId),
    ),
  )
  dataSetCreatedEvent.parameters.push(
    new ethereum.EventParam(
      'cacheMissRailId',
      ethereum.Value.fromUnsignedBigInt(cacheMissRailId),
    ),
  )
  dataSetCreatedEvent.parameters.push(
    new ethereum.EventParam(
      'cdnRailId',
      ethereum.Value.fromUnsignedBigInt(cdnRailId),
    ),
  )
  dataSetCreatedEvent.parameters.push(
    new ethereum.EventParam('payer', ethereum.Value.fromAddress(payer)),
  )
  dataSetCreatedEvent.parameters.push(
    new ethereum.EventParam(
      'serviceProvider',
      ethereum.Value.fromAddress(serviceProvider),
    ),
  )
  dataSetCreatedEvent.parameters.push(
    new ethereum.EventParam('payee', ethereum.Value.fromAddress(payee)),
  )
  dataSetCreatedEvent.parameters.push(
    new ethereum.EventParam(
      'metadataKeys',
      ethereum.Value.fromStringArray(metadataKeys),
    ),
  )
  dataSetCreatedEvent.parameters.push(
    new ethereum.EventParam(
      'metadataValues',
      ethereum.Value.fromStringArray(metadataValues),
    ),
  )

  return dataSetCreatedEvent
}
