import { ethereum } from '@graphprotocol/graph-ts'

export function getEventEntityId(event: ethereum.Event): string {
  return event.transaction.hash.concatI32(event.logIndex.toI32()).toHexString()
}
