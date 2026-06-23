import { logRetrievalResult } from './stats.js'
import { setContentSecurityPolicy } from './content-security-policy.js'
import { httpAssert } from './http-assert.js'

/** @typedef {{ response: Response; cacheMiss: boolean }} RetrievalResult */

/**
 * Attempt retrieval from the given candidates in random order until one returns
 * a usable response (status `< 500`). Candidates whose retrieval throws or
 * returns a `5xx` response are skipped.
 *
 * On success, returns the selected candidate and its result. When every
 * candidate fails (throws or returns a `5xx`), logs the failure and returns the
 * `502` "No available service provider found" response instead.
 *
 * The input array is not mutated.
 *
 * @template {{
 *   serviceUrl: string
 *   serviceProviderId: string
 *   dataSetId: string
 * }} Candidate
 * @template {RetrievalResult} Result
 * @param {Candidate[]} candidates - The candidates to attempt, in any order.
 * @param {(candidate: Candidate) => Promise<Result>} attemptRetrieval -
 *   Performs the retrieval for a single candidate.
 * @param {object} options - Context for logging a failed retrieval.
 * @param {{ DB: D1Database }} options.env
 * @param {ExecutionContext} options.ctx
 * @param {string | null} options.requestCountryCode
 * @param {string} options.timestamp
 * @param {string | undefined} options.botName
 * @returns {Promise<
 *   | { candidate: Candidate; result: Result; failureResponse?: undefined }
 *   | { candidate?: undefined; result?: undefined; failureResponse: Response }
 * >}
 *   - On success, `candidate` and its `result`.
 *   - On failure, the `502` `failureResponse`.
 */
export async function selectRetrievalCandidate(
  candidates,
  attemptRetrieval,
  { env, ctx, requestCountryCode, timestamp, botName },
) {
  const remaining = [...candidates]
  /** @type {Candidate | undefined} */
  let candidate
  /** @type {Result | undefined} */
  let result
  /** @type {Candidate[]} */
  const attempts = []

  while (remaining.length > 0) {
    const index = Math.floor(Math.random() * remaining.length)
    candidate = remaining[index]
    attempts.push(candidate)
    remaining.splice(index, 1)
    console.log(`Attempting retrieval via ${candidate.serviceUrl}`)
    try {
      result = await attemptRetrieval(candidate)
      if (result.response.ok) {
        console.log(
          `Retrieval attempt succeeded (cache ${result.cacheMiss ? 'miss' : 'hit'})`,
        )
        break
      }
      console.log(`Retrieval attempt failed: HTTP ${result.response.status}`, {
        candidate,
        willRetry: remaining.length > 0,
      })
    } catch (err) {
      const msg =
        typeof err === 'object' && err !== null && 'message' in err
          ? err.message
          : String(err)
      console.log(`Retrieval attempt failed: ${msg}`, {
        candidate,
        willRetry: remaining.length > 0,
      })
    }
  }

  httpAssert(candidate, 500, 'should never happen')

  if (result && result.response.status < 500) {
    return { candidate, result }
  }

  ctx.waitUntil(
    logRetrievalResult(env, {
      cacheMiss: result?.cacheMiss ?? null,
      cacheMissResponseValid: null,
      responseStatus: 502,
      egressBytes: 0,
      cacheMissEgressBytes: 0,
      requestCountryCode,
      timestamp,
      dataSetId: candidate.dataSetId,
      botName,
    }),
  )

  const failureResponse = new Response(
    `No available service provider found. Attempted: ${attempts
      .map((a) => `ID=${a.serviceProviderId} (Service URL=${a.serviceUrl})`)
      .join(', ')}`,
    {
      status: 502,
      headers: new Headers({
        'X-Data-Set-ID': attempts.map((a) => a.dataSetId).join(','),
      }),
    },
  )
  setContentSecurityPolicy(failureResponse)
  return { failureResponse }
}
