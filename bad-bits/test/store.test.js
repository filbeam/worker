import { describe, it, beforeAll, expect } from 'vitest'
import { updateBadBitsDatabase, getAllBadBitHashes } from '../lib/store.js'
import { env } from 'cloudflare:test'

describe('updateBadBitsDatabase', () => {
  beforeAll(async () => {
    // Clear the database before running tests
    const { keys } = await env.KV.list()
    for (const key of keys) {
      if (key.startsWith('bad-bits:')) {
        await env.KV.delete(key)
      }
    }
  })

  it('adds new hashes to the database', async () => {
    const currentHashes = new Set(['hash1', 'hash2', 'hash3'])

    await updateBadBitsDatabase(env, currentHashes)
    const storedHashes = new Set(await getAllBadBitHashes(env))

    // Verify the database contains the new hashes
    expect(storedHashes).toEqual(currentHashes)
  })

  it('removes hashes not in the current set', async () => {
    // Insert some initial hashes into the database
    const initialHashes = ['hash1', 'hash2', 'hash3']
    await initialHashes.map((hash) => env.KV.put(`bad-bits:${hash}`, 'true'))
    await env.KV.put(`bad-bits:_latest-hashes:0`, initialHashes.join(','))

    const currentHashes = new Set(['hash2', 'hash4'])

    await updateBadBitsDatabase(env, currentHashes)
    const storedHashes = new Set(await getAllBadBitHashes(env))

    // Verify the database contains only the current hashes
    expect(storedHashes).toEqual(currentHashes)
    expect(storedHashes.has('hash1')).toBe(false)
    expect(storedHashes.has('hash3')).toBe(false)
    expect(storedHashes.has('hash2')).toBe(true)
    expect(storedHashes.has('hash4')).toBe(true)
  })

  it('does not modify the database if hashes are unchanged', async () => {
    const currentHashes = ['hash1', 'hash2', 'hash3']

    // Insert the same hashes into the database
    await Promise.all(
      currentHashes.map((hash) => env.KV.put(`bad-bits:${hash}`, 'true')),
    )
    await env.KV.put(`bad-bits:_latest-hashes:0`, currentHashes.join(','))

    await updateBadBitsDatabase(env, new Set(currentHashes))
    const storedHashes = await getAllBadBitHashes(env)

    // Verify the database remains unchanged
    expect(storedHashes).toEqual(currentHashes)
  })
})
