import {
  PieceAdded as PieceAddedEvent,
  ServiceTerminated as ServiceTerminatedEvent,
  CDNServiceTerminated as CDNServiceTerminatedEvent,
  DataSetCreated as DataSetCreatedEvent,
  CDNPaymentRailsToppedUp as CDNPaymentRailsToppedUpEvent,
} from '../generated/FilecoinWarmStorageService/FilecoinWarmStorageService'
import {
  PieceAdded,
  ServiceTerminated,
  CdnServiceTerminated,
  DataSetCreated,
  CdnPaymentRailsToppedUp,
} from '../generated/schema'
import { getEventEntityId } from './utils'

export function handlePieceAdded(event: PieceAddedEvent): void {
  const entity = new PieceAdded(getEventEntityId(event))
  entity.dataSetId = event.params.dataSetId.toString()
  entity.pieceId = event.params.pieceId.toString()
  entity.pieceCid = event.params.pieceCid.data.toHexString()
  entity.metadataKeys = event.params.keys
  entity.metadataValues = event.params.values

  entity.blockNumber = event.block.number
  entity.blockTimestamp = event.block.timestamp
  entity.transactionHash = event.transaction.hash.toHexString()

  entity.save()
}

export function handleServiceTerminated(event: ServiceTerminatedEvent): void {
  const entity = new ServiceTerminated(getEventEntityId(event))
  entity.caller = event.params.caller.toHexString()
  entity.dataSetId = event.params.dataSetId.toString()
  entity.pdpRailId = event.params.pdpRailId.toString()
  entity.cacheMissRailId = event.params.cacheMissRailId.toString()
  entity.cdnRailId = event.params.cdnRailId.toString()

  entity.blockNumber = event.block.number
  entity.timestamp = event.block.timestamp
  entity.transactionHash = event.transaction.hash.toHexString()

  entity.save()
}

export function handleCdnServiceTerminated(
  event: CDNServiceTerminatedEvent,
): void {
  const entity = new CdnServiceTerminated(getEventEntityId(event))
  entity.caller = event.params.caller.toHexString()
  entity.dataSetId = event.params.dataSetId.toString()
  entity.cacheMissRailId = event.params.cacheMissRailId.toString()
  entity.cdnRailId = event.params.cdnRailId.toString()

  entity.blockNumber = event.block.number
  entity.timestamp = event.block.timestamp
  entity.transactionHash = event.transaction.hash.toHexString()

  entity.save()
}

export function handleDataSetCreated(event: DataSetCreatedEvent): void {
  const entity = new DataSetCreated(getEventEntityId(event))
  entity.dataSetId = event.params.dataSetId.toString()
  entity.providerId = event.params.providerId.toString()
  entity.pdpRailId = event.params.pdpRailId.toString()
  entity.cacheMissRailId = event.params.cacheMissRailId.toString()
  entity.cdnRailId = event.params.cdnRailId.toString()
  entity.payer = event.params.payer.toHexString()
  entity.serviceProvider = event.params.serviceProvider.toHexString()
  entity.payee = event.params.payee.toHexString()
  entity.metadataKeys = event.params.metadataKeys
  entity.metadataValues = event.params.metadataValues

  entity.blockNumber = event.block.number
  entity.blockTimestamp = event.block.timestamp
  entity.transactionHash = event.transaction.hash.toHexString()

  entity.save()
}

export function handleCdnPaymentRailsToppedUp(
  event: CDNPaymentRailsToppedUpEvent,
): void {
  const entity = new CdnPaymentRailsToppedUp(getEventEntityId(event))
  entity.dataSetId = event.params.dataSetId.toString()
  entity.totalCdnLockup = event.params.totalCdnLockup.toString()
  entity.totalCacheMissLockup = event.params.totalCacheMissLockup.toString()

  entity.blockNumber = event.block.number
  entity.blockTimestamp = event.block.timestamp
  entity.transactionHash = event.transaction.hash.toHexString()

  entity.save()
}
