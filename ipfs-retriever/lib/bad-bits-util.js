/**
 * @param {string} cid
 * @returns {Promise<string>} Bad Bits entry in the legacy double-hash format
 */
export async function getBadBitsEntry(cid) {
  const cidBytes = new TextEncoder().encode(`${cid}/`)
  const hash = await crypto.subtle.digest('SHA-256', cidBytes)
  const hashHex = Array.from(new Uint8Array(hash))
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('')
  return hashHex
}

/**
 * @param {Pick<Env, 'DB'>} env
 * @param {string} cid
 * @returns {Promise<boolean>}
 */
export async function findInBadBits(env, cid) {
  const badBitsEntry = await getBadBitsEntry(cid)

  const result = await env.DB.prepare('SELECT * FROM bad_bits WHERE hash = ?')
    .bind(badBitsEntry)
    .all()

  return result.results.length > 0
}
