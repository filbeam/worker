import { httpAssert } from './http-assert.js'

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

export const BAD_BITS_DENIED_MESSAGE =
  'The requested CID was flagged by the Bad Bits Denylist at https://badbits.dwebops.pub'

/**
 * Looks up whether a CID is on the Bad Bits denylist stored in KV.
 *
 * @param {{ BAD_BITS_KV: KVNamespace }} env
 * @param {string} cid
 * @returns {Promise<boolean>}
 */
export async function isCidDenied(env, cid) {
  const entry = await env.BAD_BITS_KV.get(
    `bad-bits:${await getBadBitsEntry(cid)}`,
    { type: 'json' },
  )
  return Boolean(entry)
}

/**
 * Throws a `404` when the CID is on the Bad Bits denylist.
 *
 * @param {{ BAD_BITS_KV: KVNamespace }} env
 * @param {string} cid
 * @returns {Promise<void>}
 */
export async function assertCidNotDenied(env, cid) {
  httpAssert(!(await isCidDenied(env, cid)), 404, BAD_BITS_DENIED_MESSAGE)
}
