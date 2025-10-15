/**
 * Gets all bad bit hashes from the database
 *
 * @param {{ KV: KVNamespace }} env - Environment containing database connection
 * @returns {Promise<string[]>} - Array of hash strings
 */
export async function getAllBadBitHashes(env) {
  const hashes = []
  const { keys } = await env.KV.list({ prefix: 'bad-bits:_latest-hashes'})
  for (const key of keys) {
    const segment = await env.KV.get(key.name)
    if (segment) {
      for (const hash of segment.split(',')) {
        hashes.push(hash)
      }
    }
  }
  return hashes
}
