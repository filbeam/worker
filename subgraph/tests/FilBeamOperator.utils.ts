import { newMockEvent } from 'matchstick-as'
import { ethereum, BigInt } from '@graphprotocol/graph-ts'
import { CDNSettlement } from '../generated/FilBeamOperator/FilBeamOperator'

export function createCdnPaymentSettledEvent(
  dataSetId: BigInt,
  cdnAmount: BigInt,
): CDNSettlement {
  const event = changetype<CDNSettlement>(newMockEvent())

  event.parameters = []

  event.parameters.push(
    new ethereum.EventParam(
      'dataSetId',
      ethereum.Value.fromUnsignedBigInt(dataSetId),
    ),
  )
  event.parameters.push(
    new ethereum.EventParam(
      'cdnAmount',
      ethereum.Value.fromUnsignedBigInt(cdnAmount),
    ),
  )

  return event
}
