import * as Hasher from 'fr32-sha2-256-trunc254-padded-binary-tree-multihash/wasm-import'
import * as Raw from 'multiformats/codecs/raw'
import * as Digest from 'multiformats/hashes/digest'
import { CID } from 'multiformats/cid'

/** @typedef {CID} PieceCID */

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

      // Allocate buffer to hold the multihash
      const digest = new Uint8Array(hasher.multihashByteLength())
      // Write digest and capture end offset
      hasher.digestInto(
        // into provided buffer
        digest,
        // at 0 byte offset
        0,
        // and include multihash prefix
        true,
      )
      // There's no GC (yet) in WASM so you should free up
      // memory manually once you're done.
      hasher.free()

      pieceCid = CID.createV1(Raw.code, Digest.decode(digest))
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
