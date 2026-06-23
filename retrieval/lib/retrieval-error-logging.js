import { logRetrievalError } from './stats.js'

/**
 * Sets up the per-request retrieval telemetry, runs the handler with it, and
 * logs a retrieval error (then rethrows) when the handler throws.
 *
 * @template T
 * @param {Request} request
 * @param {{ DB: D1Database }} env
 * @param {ExecutionContext} ctx
 * @param {{ botName: string | undefined }} options
 * @param {(context: {
 *   requestTimestamp: string
 *   workerStartedAt: number
 *   requestCountryCode: string | null
 * }) => Promise<T>} handler
 * @returns {Promise<T>}
 */
export async function withRetrievalErrorLogging(
  request,
  env,
  ctx,
  { botName },
  handler,
) {
  const requestTimestamp = new Date().toISOString()
  const workerStartedAt = performance.now()
  const requestCountryCode = request.headers.get('CF-IPCountry')

  try {
    return await handler({
      requestTimestamp,
      workerStartedAt,
      requestCountryCode,
    })
  } catch (error) {
    logRetrievalError(env, ctx, error, {
      requestCountryCode,
      timestamp: requestTimestamp,
      botName,
    })

    throw error
  }
}
