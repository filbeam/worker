import { logRetrievalResult } from './stats.js'
import { setContentSecurityPolicy } from './content-security-policy.js'

/** @typedef {{ response: Response; cacheMiss: boolean }} RetrievalResult */

/**
 * @typedef {{
 *   serviceProviderId: string
 *   serviceUrl: string
 *   dataSetId: string
 * }} RetrievalAttempt
 */

/**
 * Decides whether a retrieval failed at the service-provider level (no result,
 * or a `5xx` response from the origin). When it did, logs the failure and
 * returns the `502` "No available service provider found" response. Otherwise
 * returns `null`, leaving the caller to serve the successful response.
 *
 * @param {{ DB: D1Database }} env - Worker environment (contains D1 binding).
 * @param {ExecutionContext} ctx
 * @param {object} params
 * @param {RetrievalResult | undefined} params.retrievalResult - The last
 *   retrieval result, or `undefined` when every attempt threw.
 * @param {RetrievalAttempt[]} params.attempts - Every service provider that was
 *   attempted.
 * @param {string} params.dataSetId - The data set ID to log the failure
 *   against.
 * @param {string | null} params.requestCountryCode
 * @param {string} params.timestamp
 * @param {string | undefined} params.botName
 * @returns {Response | null}
 */
export function maybeHandleNoServiceProvider(
  env,
  ctx,
  {
    retrievalResult,
    attempts,
    dataSetId,
    requestCountryCode,
    timestamp,
    botName,
  },
) {
  if (retrievalResult && retrievalResult.response.status < 500) {
    return null
  }

  ctx.waitUntil(
    logRetrievalResult(env, {
      cacheMiss: retrievalResult?.cacheMiss ?? null,
      cacheMissResponseValid: null,
      responseStatus: 502,
      egressBytes: 0,
      cacheMissEgressBytes: 0,
      requestCountryCode,
      timestamp,
      dataSetId,
      botName,
    }),
  )

  const response = new Response(
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
  setContentSecurityPolicy(response)
  return response
}
