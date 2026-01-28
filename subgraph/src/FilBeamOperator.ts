import {
  UsageReported as UsageReportedEvent,
  CDNSettlement as CDNSettlementEvent,
} from '../generated/FilBeamOperator/FilBeamOperator'
import { UsageReported, CDNSettlement } from '../generated/schema'
import { getEventEntityId } from './utils'

export function handleUsageReported(event: UsageReportedEvent): void {
  const entity = new UsageReported(getEventEntityId(event))
  entity.dataSetId = event.params.dataSetId.toString()
  entity.fromEpoch = event.params.fromEpoch.toString()
  entity.toEpoch = event.params.toEpoch.toString()
  entity.cdnBytesUsed = event.params.cdnBytesUsed.toString()
  entity.cacheMissBytesUsed = event.params.cacheMissBytesUsed.toString()

  entity.blockNumber = event.block.number
  entity.blockTimestamp = event.block.timestamp
  entity.transactionHash = event.transaction.hash.toHexString()

  entity.save()
}

export function handleCDNSettlement(event: CDNSettlementEvent): void {
  const entity = new CDNSettlement(getEventEntityId(event))
  entity.dataSetId = event.params.dataSetId.toString()
  entity.cdnAmount = event.params.cdnAmount.toString()

  entity.blockNumber = event.block.number
  entity.blockTimestamp = event.block.timestamp
  entity.transactionHash = event.transaction.hash.toHexString()

  entity.save()
}
