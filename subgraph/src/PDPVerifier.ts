import { PiecesRemoved as PiecesRemovedEvent } from '../generated/PDPVerifier/PDPVerifier'
import { PiecesRemoved } from '../generated/schema'
import { getEventEntityId } from './utils'

export function handlePiecesRemoved(event: PiecesRemovedEvent): void {
  const entity = new PiecesRemoved(getEventEntityId(event))
  entity.dataSetId = event.params.setId.toString()
  entity.pieceIds = event.params.pieceIds.map<string>((id) => id.toString())

  entity.blockNumber = event.block.number
  entity.blockTimestamp = event.block.timestamp
  entity.transactionHash = event.transaction.hash.toHexString()

  entity.save()
}
