import { newMockEvent } from 'matchstick-as'
import { ethereum, BigInt } from '@graphprotocol/graph-ts'
import { PiecesRemoved } from '../generated/PDPVerifier/PDPVerifier'

export function createPiecesRemovedEvent(
  setId: BigInt,
  pieceIds: Array<BigInt>,
): PiecesRemoved {
  const piecesRemovedEvent = changetype<PiecesRemoved>(newMockEvent())

  piecesRemovedEvent.parameters = []

  piecesRemovedEvent.parameters.push(
    new ethereum.EventParam('setId', ethereum.Value.fromUnsignedBigInt(setId)),
  )
  piecesRemovedEvent.parameters.push(
    new ethereum.EventParam(
      'pieceIds',
      ethereum.Value.fromUnsignedBigIntArray(pieceIds),
    ),
  )

  return piecesRemovedEvent
}
