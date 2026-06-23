/**
 * Attempt retrieval from the given candidates in random order until one returns
 * an OK response or all candidates have been tried. Candidates whose retrieval
 * throws or returns a non-OK response are skipped.
 *
 * The input array is not mutated.
 *
 * @template {{ serviceUrl: string }} Candidate
 * @template {{ response: Response; cacheMiss: boolean }} Result
 * @param {Candidate[]} candidates - The candidates to attempt, in any order.
 * @param {(candidate: Candidate) => Promise<Result>} attemptRetrieval -
 *   Performs the retrieval for a single candidate.
 * @returns {Promise<{
 *   candidate: Candidate | undefined
 *   result: Result | undefined
 *   attempts: Candidate[]
 * }>}
 *   - `candidate` is the last attempted candidate (the successful one when a
 *       retrieval returned an OK response).
 *   - `result` is that candidate's retrieval result, or `undefined` when every
 *       attempt threw.
 *   - `attempts` lists every candidate that was attempted, in attempt order.
 */
export async function selectRetrievalCandidate(candidates, attemptRetrieval) {
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

  return { candidate, result, attempts }
}
