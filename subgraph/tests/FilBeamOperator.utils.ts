import { newMockEvent } from 'matchstick-as'
import { ethereum, BigInt } from '@graphprotocol/graph-ts'
import {
  UsageReported,
  CDNSettlement,
} from '../generated/FilBeamOperator/FilBeamOperator'

export function createUsageReportedEvent(
  dataSetId: BigInt,
  fromEpoch: BigInt,
  toEpoch: BigInt,
  cdnBytesUsed: BigInt,
  cacheMissBytesUsed: BigInt,
): UsageReported {
  const event = changetype<UsageReported>(newMockEvent())

  event.parameters = []

  event.parameters.push(
    new ethereum.EventParam(
      'dataSetId',
      ethereum.Value.fromUnsignedBigInt(dataSetId),
    ),
  )
  event.parameters.push(
    new ethereum.EventParam(
      'fromEpoch',
      ethereum.Value.fromUnsignedBigInt(fromEpoch),
    ),
  )
  event.parameters.push(
    new ethereum.EventParam(
      'toEpoch',
      ethereum.Value.fromUnsignedBigInt(toEpoch),
    ),
  )
  event.parameters.push(
    new ethereum.EventParam(
      'cdnBytesUsed',
      ethereum.Value.fromUnsignedBigInt(cdnBytesUsed),
    ),
  )
  event.parameters.push(
    new ethereum.EventParam(
      'cacheMissBytesUsed',
      ethereum.Value.fromUnsignedBigInt(cacheMissBytesUsed),
    ),
  )

  return event
}

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
