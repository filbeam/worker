import { createPieceCIDStream } from '../lib/piece.js'
import { describe, it, expect } from 'vitest'

describe('createPieceCIDStream', () => {
  it('calculates correct PieceCID for a small file', async () => {
    const chunks = [new Uint8Array([1, 2, 3, 4]), new Uint8Array([5, 6, 7])]
    const { pieceCid, content } = await calculatePieceCid(
      givenReadableStream(chunks),
    )

    expect(pieceCid).toBe(
      'bafkzcibcpabmh6kcmzljo4uuu2uwaojzoaw4xj2svxzojv7oh5s7bq6sl532wia',
    )
    expect(content).toEqual(chunks)
  })

  it('calculates correct PieceCID for a large file', async () => {
    const chunks = []
    for (let i = 0; i < 1000; i++) {
      const chunk = new Uint8Array(1024)
      for (let j = 0; j < chunk.length; j++) {
        chunk[j] = (i + j) % 256
      }
      chunks.push(chunk)
    }

    const { pieceCid, content } = await calculatePieceCid(
      givenReadableStream(chunks),
    )

    expect(pieceCid).toBe(
      'bafkzcibeqcaacd7u5a2ha3cx55svuwfqjrgmscz57ebackb44bgvqrhuhdbjnouteu',
    )
    expect(content).toEqual(chunks)
  })
})

/** @param {Uint8Array[]} chunks */
function givenReadableStream(chunks) {
  return new ReadableStream({
    start(controller) {
      for (const c of chunks) {
        controller.enqueue(c)
      }
      controller.close()
    },
  })
}

/**
 * @param {ReadableStream<Uint8Array>} readableStream
 * @returns
 */
async function calculatePieceCid(readableStream) {
  const { stream: pieceCidStream, getPieceCID } = createPieceCIDStream()
  const transformedStream = readableStream.pipeThrough(pieceCidStream)

  // Consume the stream to trigger the hashing
  /** @type {Uint8Array[]} */
  const content = []
  const reader = transformedStream.getReader()
  while (true) {
    const { value, done } = await reader.read()
    if (done) break
    content.push(value)
  }

  return { pieceCid: getPieceCID().toString(), content }
}
