const KV_VALUE_MAX_SIZE = 26214400
const BAD_BITS_CID_LENGTH = 64
const KV_SEGMENT_MAX_HASH_COUNT = Math.floor(
  KV_VALUE_MAX_SIZE / (BAD_BITS_CID_LENGTH + 1) /* , */,
)
const MAX_TOTAL_CHANGES = 10_000
const MAX_KV_BATCH_SIZE = 1_000

/**
 * Updates the bad bits database with new hashes
 *
 * @param {{ BAD_BITS_KV: KVNamespace }} env - Environment containing database
 *   connection
 * @param {Set<string>} currentHashes - Set of current valid hashes from
 *   denylist
 * @param {string | null} etag - ETag for the current denylist
 */
export async function updateBadBitsDatabase(env, currentHashes, etag) {
  const startedAt = Date.now()

  console.log('getting latest hashes')
  const oldHashes = new Set(await getAllBadBitHashes(env))

  console.log('comparing current hashes')
  const addedHashes = [...currentHashes.difference(oldHashes)]
  const removedHashes = [...oldHashes.difference(currentHashes)]
  addedHashes.length = Math.min(addedHashes.length, MAX_TOTAL_CHANGES)
  removedHashes.length = Math.min(
    removedHashes.length,
    MAX_TOTAL_CHANGES - addedHashes.length,
  )

  await persistUpdates(env, oldHashes, addedHashes, removedHashes)

  const wasCapped =
    addedHashes.length + removedHashes.length >= MAX_TOTAL_CHANGES
  if (etag && !wasCapped) {
    await env.BAD_BITS_KV.put('latest-etag', etag)
  }

  console.log(
    `+${addedHashes.length} -${removedHashes.length} hashes in ${Date.now() - startedAt}ms`,
  )
}

/** @param {{ BAD_BITS_KV: KVNamespace }} env */
export async function getLastEtag(env) {
  return await env.BAD_BITS_KV.get('latest-etag')
}

/**
 * Gets all bad bit hashes from the database
 *
 * @param {{ BAD_BITS_KV: KVNamespace }} env - Environment containing database
 *   connection
 * @returns {Promise<string[]>} - Array of hash strings
 */
export async function getAllBadBitHashes(env) {
  const hashes = []
  const { keys } = await env.BAD_BITS_KV.list({ prefix: 'latest-hashes' })
  for (const key of keys) {
    const segment = await env.BAD_BITS_KV.get(key.name)
    if (segment) {
      for (const hash of segment.split(',')) {
        hashes.push(hash)
      }
    }
  }
  return hashes
}

/**
 * @param {{ BAD_BITS_KV: KVNamespace }} env
 * @param {Set<string>} oldHashes
 * @param {string[]} addedHashes
 * @param {string[]} removedHashes
 * @returns {Promise<void>}
 */
async function persistUpdates(env, oldHashes, addedHashes, removedHashes) {
  console.log('writing added hashes')
  for (let i = 0; i < Math.ceil(addedHashes.length / MAX_KV_BATCH_SIZE); i++) {
    const batch = addedHashes.slice(
      i * MAX_KV_BATCH_SIZE,
      (i + 1) * MAX_KV_BATCH_SIZE ,
    )
    await Promise.all(
      batch.map((hash) => env.BAD_BITS_KV.put(`bad-bits:${hash}`, 'true')),
    )
  }

  console.log('deleting removed hashes')
  for (
    let i = 0;
    i < Math.ceil(removedHashes.length / MAX_KV_BATCH_SIZE);
    i++
  ) {
    const batch = removedHashes.slice(
      i * MAX_KV_BATCH_SIZE,
      (i + 1) * MAX_KV_BATCH_SIZE,
    )
    await Promise.all(
      batch.map((hash) => env.BAD_BITS_KV.delete(`bad-bits:${hash}`)),
    )
  }

  console.log('storing latest hashes')
  const latestHashes = new Set(oldHashes)
  for (const hash of addedHashes) {
    latestHashes.add(hash)
  }
  for (const hash of removedHashes) {
    latestHashes.delete(hash)
  }
  const segmentCount = Math.ceil(latestHashes.size / KV_SEGMENT_MAX_HASH_COUNT)
  for (let i = 0; i < segmentCount; i++) {
    await env.BAD_BITS_KV.put(
      `latest-hashes:${i}`,
      [...latestHashes]
        .slice(
          i * KV_SEGMENT_MAX_HASH_COUNT,
          (i + 1) * KV_SEGMENT_MAX_HASH_COUNT,
        )
        .join(','),
    )
  }
  const { keys } = await env.BAD_BITS_KV.list({ prefix: 'latest-hashes:' })
  for (const key of keys) {
    const i = Number(key.name.split(':')[1])
    if (i > segmentCount - 1) {
      await env.BAD_BITS_KV.delete(key.name)
    }
  }
}
