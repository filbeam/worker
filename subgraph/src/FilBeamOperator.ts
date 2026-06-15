import { CDNSettlement as CDNSettlementEvent } from '../generated/FilBeamOperator/FilBeamOperator'
import { CdnPaymentSettled } from '../generated/schema'
import { getEventEntityId } from './utils'

export function handleCdnPaymentSettled(event: CDNSettlementEvent): void {
  const entity = new CdnPaymentSettled(getEventEntityId(event))
  entity.dataSetId = event.params.dataSetId.toString()
  entity.cdnAmount = event.params.cdnAmount.toString()

  entity.blockNumber = event.block.number
  entity.blockTimestamp = event.block.timestamp
  entity.transactionHash = event.transaction.hash.toHexString()

  entity.save()
}
