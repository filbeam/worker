const MAX_TOTAL_CHANGES = 10_000
const MAX_KV_BATCH_SIZE = 1_000

/**
 * Updates the bad bits database with new hashes
 *
 * @param {{ BAD_BITS_KV: KVNamespace; BAD_BITS_R2: R2Bucket }} env -
 *   Environment containing database connection
 * @param {Set<string>} currentHashes - Set of current valid hashes from
 *   denylist
 * @param {string | null} etag - ETag for the current denylist
 */
export async function updateBadBitsDatabase(env, currentHashes, etag) {
  const startedAt = Date.now()

  console.log('getting latest hashes')
  const oldHashes = new Set(await getAllBadBitHashes(env))

  console.log('comparing current hashes')
  let wasCapped = false
  const addedHashes = [...currentHashes.difference(oldHashes)]
  const removedHashes = [...oldHashes.difference(currentHashes)]
  if (addedHashes.length > MAX_TOTAL_CHANGES) {
    addedHashes.splice(MAX_TOTAL_CHANGES)
    wasCapped = true
  }
  if (removedHashes.length > MAX_TOTAL_CHANGES - addedHashes.length) {
    removedHashes.splice(MAX_TOTAL_CHANGES - addedHashes.length)
    wasCapped = true
  }

  await persistUpdates(env, oldHashes, addedHashes, removedHashes)

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
 * @param {{ BAD_BITS_R2: R2Bucket }} env - Environment containing database
 *   connection
 * @returns {Promise<string[]>} - Array of hash strings
 */
export async function getAllBadBitHashes(env) {
  const object = await env.BAD_BITS_R2.get('latest-hashes')
  return object ? await object.json() : []
}

/**
 * @param {{ BAD_BITS_KV: KVNamespace; BAD_BITS_R2: R2Bucket }} env
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
      (i + 1) * MAX_KV_BATCH_SIZE,
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
  await env.BAD_BITS_R2.put('latest-hashes', JSON.stringify([...latestHashes]))
}
