import * as Hasher from '@web3-storage/data-segment/multihash'
import * as Raw from 'multiformats/codecs/raw'
import * as Link from 'multiformats/link'

/** @typedef {import('@web3-storage/data-segment').PieceLink} PieceCID */

/**
 * @returns {{
 *   stream: TransformStream<Uint8Array, Uint8Array>
 *   getPieceCID: () => PieceCID | null
 * }}
 */
export function createPieceCIDStream() {
  const hasher = Hasher.create()
  let finished = false
  /** @type {PieceCID | null} */
  let pieceCid = null

  /** @type {TransformStream<Uint8Array, Uint8Array>} */
  const stream = new TransformStream({
    /**
     * @param {Uint8Array} chunk
     * @param {TransformStreamDefaultController<Uint8Array>} controller
     */
    transform(chunk, controller) {
      hasher.write(chunk)
      controller.enqueue(chunk)
    },

    flush() {
      // Calculate final PieceCID when stream ends
      const digest = hasher.digest()
      /** @type {PieceCID} */
      pieceCid = Link.create(Raw.code, digest)
      finished = true
    },
  })

  return {
    stream,
    getPieceCID: () => {
      if (!finished) {
        return null
      }
      return pieceCid
    },
  }
}
