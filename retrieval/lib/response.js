import { logRetrievalResult, updateDataSetStats } from './stats.js'
import { setContentSecurityPolicy } from './content-security-policy.js'
import { httpAssert } from './http-assert.js'

/**
 * @param {Env} env
 * @param {ExecutionContext} ctx
 * @param {Response} response
 * @param {boolean | null} cacheMiss
 * @param {string | null} requestCountryCode
 * @param {string} requestTimestamp
 * @param {string} dataSetId
 * @param {string | undefined} botName
 * @param {number} fetchStartedAt
 * @param {number} workerStartedAt
 * @param {number} clientCacheTTL
 * @param {boolean} enforceEgressQuota
 * @returns {Response}
 */
export function handleResponse(
  env,
  ctx,
  response,
  cacheMiss,
  requestCountryCode,
  requestTimestamp,
  dataSetId,
  botName,
  fetchStartedAt,
  workerStartedAt,
  clientCacheTTL,
  enforceEgressQuota,
) {
  if (response.status >= 500) {
    ctx.waitUntil(
      logRetrievalResult(env, {
        cacheMiss: cacheMiss || null,
        responseStatus: 502,
        egressBytes: 0,
        requestCountryCode,
        timestamp: requestTimestamp,
        dataSetId,
        botName,
      }),
    )
    setContentSecurityPolicy(response)
    return response
  }

  if (!response.body) {
    // The upstream response does not have any readable body
    // There is no need to measure response body size, we can
    // return the original response object.
    ctx.waitUntil(
      logRetrievalResult(env, {
        cacheMiss,
        responseStatus: response.status,
        egressBytes: 0,
        requestCountryCode,
        timestamp: requestTimestamp,
        dataSetId,
        botName,
      }),
    )
    const res = new Response(response.body, response)
    setContentSecurityPolicy(res)
    response.headers.set('X-Data-Set-ID', dataSetId)
    response.headers.set('Cache-Control', `public, max-age=${clientCacheTTL}`)
    return res
  }

  httpAssert(typeof cacheMiss === 'boolean', 500, 'should never happen')

  // Stream and count bytes
  // We create two identical streams, one for the egress measurement and the other for returning the response as soon as possible
  const [returnedStream, egressMeasurementStream] = response.body.tee()
  const reader = egressMeasurementStream.getReader()
  const firstByteAt = performance.now()

  ctx.waitUntil(
    (async () => {
      const egressBytes = await measureStreamedEgress(reader)
      const lastByteFetchedAt = performance.now()

      await logRetrievalResult(env, {
        cacheMiss,
        responseStatus: response.status,
        egressBytes,
        requestCountryCode,
        timestamp: requestTimestamp,
        performanceStats: {
          fetchTtfb: firstByteAt - fetchStartedAt,
          fetchTtlb: lastByteFetchedAt - fetchStartedAt,
          workerTtfb: firstByteAt - workerStartedAt,
        },
        dataSetId,
        botName,
      })

      await updateDataSetStats(env, {
        dataSetId,
        egressBytes,
        cacheMiss,
        enforceEgressQuota,
      })
    })(),
  )

  // Return immediately, proxying the transformed response
  const res = new Response(returnedStream, {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
  })
  setContentSecurityPolicy(res)
  response.headers.set('X-Data-Set-ID', dataSetId)
  response.headers.set('Cache-Control', `public, max-age=${clientCacheTTL}`)
  return res
}

/**
 * Measures the egress of a request by reading from a readable stream and return
 * the total number of bytes transferred.
 *
 * @param {ReadableStreamDefaultReader<Uint8Array>} reader - The reader for the
 *   readable stream.
 * @returns {Promise<number>} - A promise that resolves to the total number of
 *   bytes transferred.
 */
export async function measureStreamedEgress(reader) {
  let total = 0
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    total += value.length
  }
  return total
}
