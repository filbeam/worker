import { assertOkResponse } from 'assert-ok-response'
import pRetry from 'p-retry'
import { getLastEtag, updateBadBitsDatabase } from './store.js'

export const BAD_BITS_URL = 'https://badbits.dwebops.pub/badbits.deny'

/**
 * @param {{ KV: KVNamespace }} env
 * @param {object} options
 * @param {typeof globalThis.fetch} options.fetch
 * @returns
 */
export async function fetchAndStoreBadBits(
  env,
  { fetch } = { fetch: globalThis.fetch },
) {
  const lastEtag = await getLastEtag(env)

  const result = await pRetry(() => fetchBadBits({ lastEtag, fetch }), {
    retries: 5,
    shouldRetry: ({ error }) => isServerError(error),
    onFailedAttempt: ({ error }) => {
      if (isServerError(error)) return
      console.error(error)
      console.error('Bad-bits query failed, retrying...')
    },
  })

  if (!result.hasChanged) return
  const { etag, text } = result

  const lines = text.split('\n')
  const currentBadHashes = new Set()

  for (const line of lines) {
    if (line.startsWith('//')) {
      const hash = line.substring(2).trim()
      if (hash) {
        currentBadHashes.add(hash)
      }
    }
  }

  console.log({
    message: 'New bad bits version',
    etag,
    lineCount: lines.length,
    hashCount: currentBadHashes.size,
  })

  try {
    await updateBadBitsDatabase(env, currentBadHashes, etag)
  } catch (error) {
    console.error('Error updating bad bits:', error)
    throw error
  }
}

/**
 * @param {object} options
 * @param {string | null} options.lastEtag
 * @param {typeof globalThis.fetch} options.fetch
 * @returns {Promise<
 *   | { hasChanged: false }
 *   | { hasChanged: true; text: string; etag: string | null }
 * >}
 */
async function fetchBadBits({ lastEtag, fetch }) {
  const req = new Request(BAD_BITS_URL)
  if (lastEtag) {
    console.log(`Requesting version different from etag ${lastEtag}`)
    req.headers.set('if-none-match', lastEtag)
  }

  const response = await fetch(req)

  if (response.status === 304) {
    console.log(
      'Bad bits were not modified since the last check, skipping update.',
    )
    return { hasChanged: false }
  }
  await assertOkResponse(response, 'Failed to fetch bad bits')

  const text = await response.text()
  const etag = response.headers.get('etag')
  return { hasChanged: true, text, etag }
}

/**
 * @param {Error} error
 * @returns {boolean}
 */
function isServerError(error) {
  return (
    'statusCode' in error &&
    typeof error.statusCode === 'number' &&
    error.statusCode >= 500
  )
}
