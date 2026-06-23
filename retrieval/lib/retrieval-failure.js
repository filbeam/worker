import { logRetrievalResult } from './stats.js'
import { setContentSecurityPolicy } from './content-security-policy.js'

/**
 * Logs a failed retrieval and builds the `502` response returned when none of
 * the attempted service providers could serve the content (every attempt either
 * threw or returned a `5xx` response).
 *
 * @param {{ DB: D1Database }} env - Worker environment (contains D1 binding).
 * @param {ExecutionContext} ctx
 * @param {object} params
 * @param {{ cacheMiss: boolean } | undefined} params.retrievalResult - The last
 *   retrieval result, or `undefined` when every attempt threw.
 * @param {{
 *   serviceProviderId: string
 *   serviceUrl: string
 *   dataSetId: string
 * }[]} params.attempts
 *   - Every service provider that was attempted.
 *
 * @param {string} params.dataSetId - The data set ID to log the failure
 *   against.
 * @param {string | null} params.requestCountryCode
 * @param {string} params.timestamp
 * @param {string | undefined} params.botName
 * @returns {Response}
 */
export function respondNoServiceProviderAvailable(
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
