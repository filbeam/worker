import { getErrorHttpStatusMessage } from './http-error.js'

/**
 * @param {{ DB: D1Database }} env - Worker environment (contains D1 binding).
 * @param {object} params - Parameters for the data set update.
 * @param {string} params.dataSetId - The ID of the data set to update.
 * @param {number} params.egressBytes - The egress bytes sent to the client.
 *   This is what the CDN egress quota is charged for.
 * @param {number} [params.cacheMissEgressBytes] - The egress bytes fetched from
 *   the service provider on a cache miss. This is what the cache-miss egress
 *   quota is charged for. Defaults to `egressBytes`, which is correct whenever
 *   the bytes served to the client equal the bytes fetched from the origin
 *   (e.g. raw piece retrievals). For IPFS retrievals the origin response is a
 *   CAR that is larger than the raw bytes served to the client.
 * @param {boolean} params.cacheMiss - Whether this was a cache miss (true) or
 *   cache hit (false).
 * @param {boolean | null} [params.cacheMissResponseValid]
 * @param {boolean} [params.enforceEgressQuota=false] - Whether to decrement
 *   egress quotas. Default is `false`
 * @param {boolean} [params.isBotTraffic=false] - Whether the egress traffic
 *   originated from the bot. Default is `false`
 */
export async function updateDataSetStats(
  env,
  {
    dataSetId,
    egressBytes,
    cacheMissEgressBytes = egressBytes,
    cacheMiss,
    cacheMissResponseValid,
    enforceEgressQuota = false,
  },
) {
  await env.DB.prepare(
    `
    UPDATE data_sets
    SET total_egress_bytes_used = total_egress_bytes_used + ?
    WHERE id = ?
    `,
  )
    .bind(egressBytes, dataSetId)
    .run()

  if (enforceEgressQuota) {
    await env.DB.prepare(
      `
      UPDATE data_set_egress_quotas
      SET cdn_egress_quota = cdn_egress_quota - ?,
          cache_miss_egress_quota = cache_miss_egress_quota - ?
      WHERE data_set_id = ?
      `,
    )
      .bind(
        egressBytes,
        cacheMiss && cacheMissResponseValid ? cacheMissEgressBytes : 0,
        dataSetId,
      )
      .run()
  }
}

/**
 * Logs the result of a file retrieval attempt to the D1 database.
 *
 * @param {{ DB: D1Database }} env - Worker environment (contains D1 binding).
 * @param {object} params - Parameters for the retrieval log.
 * @param {number | null} params.egressBytes - The egress bytes sent to the
 *   client.
 * @param {number | null} [params.cacheMissEgressBytes] - The egress bytes
 *   fetched from the service provider on a cache miss. Defaults to
 *   `egressBytes`, which is correct whenever the bytes served to the client
 *   equal the bytes fetched from the origin (e.g. raw piece retrievals). For
 *   IPFS retrievals the origin CAR is larger than the raw bytes served.
 * @param {number} params.responseStatus - The HTTP response status code.
 * @param {boolean | null} params.cacheMiss - Whether the retrieval was a cache
 *   miss.
 * @param {boolean | null} params.cacheMissResponseValid
 * @param {{
 *   fetchTtfb: number
 *   fetchTtlb: number
 *   workerTtfb: number
 * } | null} [params.performanceStats]
 *   - Performance statistics.
 *
 * @param {string} params.timestamp - The timestamp of the retrieval.
 * @param {string | null} params.requestCountryCode - The country code where the
 *   request originated from
 * @param {string | null} params.dataSetId - The data set ID associated with the
 *   retrieval
 * @param {string | undefined} params.botName - The name of the bot making the
 *   request, or null for anonymous requests
 * @returns {Promise<void>} - A promise that resolves when the log is inserted.
 */
export async function logRetrievalResult(env, params) {
  const {
    cacheMiss,
    cacheMissResponseValid,
    egressBytes,
    cacheMissEgressBytes = egressBytes,
    responseStatus,
    timestamp,
    performanceStats,
    requestCountryCode,
    dataSetId,
    botName,
  } = params

  try {
    await env.DB.prepare(
      `
      INSERT INTO retrieval_logs (
        timestamp,
        response_status,
        egress_bytes,
        cache_miss_egress_bytes,
        cache_miss,
        cache_miss_response_valid,
        fetch_ttfb,
        fetch_ttlb,
        worker_ttfb,
        request_country_code,
        data_set_id,
        bot_name
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
    )
      .bind(
        timestamp,
        responseStatus,
        egressBytes,
        cacheMissEgressBytes,
        cacheMiss,
        cacheMissResponseValid,
        performanceStats?.fetchTtfb ?? null,
        performanceStats?.fetchTtlb ?? null,
        performanceStats?.workerTtfb ?? null,
        requestCountryCode,
        dataSetId,
        botName ?? null,
      )
      .run()
  } catch (error) {
    console.error(`Error inserting log: ${error}`)
    // TODO: Handle specific SQL error codes if needed
    throw error
  }
}

/**
 * Records a failed retrieval: logs the resolved HTTP status with no egress and
 * no data set, scheduled on the execution context. Intended for a worker's
 * request error handler.
 *
 * @param {{ DB: D1Database }} env - Worker environment (contains D1 binding).
 * @param {ExecutionContext} ctx
 * @param {unknown} error - The error thrown while handling the request.
 * @param {object} context
 * @param {string | null} context.requestCountryCode
 * @param {string} context.timestamp
 * @param {string | undefined} context.botName
 */
export function logRetrievalError(
  env,
  ctx,
  error,
  { requestCountryCode, timestamp, botName },
) {
  const { status } = getErrorHttpStatusMessage(error)

  ctx.waitUntil(
    logRetrievalResult(env, {
      cacheMiss: null,
      cacheMissResponseValid: null,
      responseStatus: status,
      egressBytes: null,
      requestCountryCode,
      timestamp,
      dataSetId: null,
      botName,
    }),
  )
}

/**
 * Records a completed retrieval: writes the retrieval log and updates the data
 * set egress stats and quotas. These are always performed together for a
 * successful streamed response.
 *
 * @param {{ DB: D1Database }} env - Worker environment (contains D1 binding).
 * @param {object} params - Combined parameters for {@link logRetrievalResult}
 *   and {@link updateDataSetStats}.
 * @param {string} params.dataSetId
 * @param {number} params.egressBytes
 * @param {number} [params.cacheMissEgressBytes]
 * @param {boolean} params.cacheMiss
 * @param {boolean | null} params.cacheMissResponseValid
 * @param {number} params.responseStatus
 * @param {string | null} params.requestCountryCode
 * @param {string} params.timestamp
 * @param {{
 *   fetchTtfb: number
 *   fetchTtlb: number
 *   workerTtfb: number
 * }} [params.performanceStats]
 * @param {string | undefined} params.botName
 * @param {boolean} [params.enforceEgressQuota]
 */
export async function recordRetrieval(env, params) {
  await logRetrievalResult(env, params)
  await updateDataSetStats(env, {
    dataSetId: params.dataSetId,
    egressBytes: params.egressBytes,
    cacheMissEgressBytes: params.cacheMissEgressBytes,
    cacheMiss: params.cacheMiss,
    cacheMissResponseValid: params.cacheMissResponseValid,
    enforceEgressQuota: params.enforceEgressQuota,
  })
}
