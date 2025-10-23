import { isValidEthereumAddress } from '../lib/address.js'
import { parseRequest } from '../lib/request.js'
import {
  retrieveFile as defaultRetrieveFile,
  measureStreamedEgress,
} from '../lib/retrieval.js'
import {
  getStorageProviderAndValidatePayer,
  logRetrievalResult,
  updateDataSetStats,
} from '../lib/store.js'
import { httpAssert } from '../lib/http-assert.js'
import { setContentSecurityPolicy } from '../lib/content-security-policy.js'
import { getBadBitsEntry } from '../lib/bad-bits-util.js'

export default {
  /**
   * @param {Request} request
   * @param {Env} env
   * @param {ExecutionContext} ctx
   * @param {object} options
   * @param {typeof defaultRetrieveFile} [options.retrieveFile]
   * @returns
   */
  async fetch(request, env, ctx, { retrieveFile = defaultRetrieveFile } = {}) {
    try {
      return await this._fetch(request, env, ctx, { retrieveFile })
    } catch (error) {
      return this._handleError(error)
    }
  },

  /**
   * @param {Request} request
   * @param {Env} env
   * @param {ExecutionContext} ctx
   * @param {object} options
   * @param {typeof defaultRetrieveFile} [options.retrieveFile]
   * @returns
   */
  async _fetch(request, env, ctx, { retrieveFile = defaultRetrieveFile } = {}) {
    httpAssert(
      ['GET', 'HEAD'].includes(request.method),
      405,
      'Method Not Allowed',
    )
    if (URL.parse(request.url)?.pathname === '/') {
      return Response.redirect('https://filbeam.com/', 302)
    }
    if (URL.parse(request.url)?.hostname.endsWith('filcdn.io')) {
      return Response.redirect(
        request.url.replace('filcdn.io', 'filbeam.io'),
        301,
      )
    }

    const requestTimestamp = new Date().toISOString()
    const workerStartedAt = performance.now()
    const requestCountryCode = request.headers.get('CF-IPCountry')

    const { payerWalletAddress, pieceCid } = parseRequest(request, env)

    httpAssert(payerWalletAddress && pieceCid, 400, 'Missing required fields')
    httpAssert(
      isValidEthereumAddress(payerWalletAddress),
      400,
      `Invalid address: ${payerWalletAddress}. Address must be a valid ethereum address.`,
    )

    // Step 4a: Instantiate Durable Object Stub
    // Using 'singleton' ensures we interact with a single, global quota manager instance.
    const quotaManagerStub = env.QUOTA_MANAGER_DO.idFromName('singleton').getStub();

    try {
      // Timestamp to measure file retrieval performance (from cache and from SP)
      const fetchStartedAt = performance.now()

      const [{ serviceProviderId, serviceUrl, dataSetId }, isBadBit] =
        await Promise.all([
          getStorageProviderAndValidatePayer(env, payerWalletAddress, pieceCid),
          env.BAD_BITS_KV.get(`bad-bits:${getBadBitsEntry(pieceCid)}`, {
            type: 'json',
          }),
        ])

      httpAssert(
        !isBadBit,
        404,
        'The requested CID was flagged by the Bad Bits Denylist at https://badbits.dwebops.pub',
      )

      httpAssert(
        serviceProviderId,
        404,
        `Unsupported Service Provider: ${serviceProviderId}`,
      )

      // Step 4b: Retrieve Piece and Egress Info
      // The `retrieveFile` function (from ../lib/retrieval.js) is expected to be updated
      // to return `egressType` and `egressSize` along with the response.
      const {
        response: originResponse,
        cacheMiss,
        url,
        egressType, // Expected from updated retrieval.js
        egressSize, // Expected from updated retrieval.js
      } = await retrieveFile(
        ctx,
        serviceUrl,
        pieceCid,
        request,
        env.ORIGIN_CACHE_TTL,
        { signal: request.signal },
      )

      // Step 4c: Fetch Current Quotas from the Durable Object
      const currentQuotasResponse = await quotaManagerStub.fetch('/getQuotas');
      httpAssert(currentQuotasResponse.ok, 500, 'Failed to fetch current quotas from QuotaManager');
      const { cdnRemaining: currentCdnRemaining, cacheMissRemaining: currentCacheMissRemaining } =
        await currentQuotasResponse.json();

      // Step 4d: Calculate Reported Quotas for the response headers
      let reportedCdnRemaining = currentCdnRemaining;
      let reportedCacheMissRemaining = currentCacheMissRemaining;
      const actualEgressAmount = egressSize || 0; // Use egressSize from retrieval, default to 0 if not provided

      if (egressType === 'cdn') {
        reportedCdnRemaining = Math.max(0, currentCdnRemaining - actualEgressAmount);
      } else if (egressType === 'cache-miss') {
        reportedCacheMissRemaining = Math.max(0, currentCacheMissRemaining - actualEgressAmount);
      }
      // If `egressType` is not 'cdn' or 'cache-miss', the reported quotas will not be
      // adjusted by the current request's egress amount in the response headers.
      // The persistent decrement will still occur based on the `egressType` provided.

      let finalResponse;
      let egressBytesForLogging = 0; // Actual bytes measured from the stream for logging purposes

      if (originResponse.status >= 500) {
        finalResponse = new Response(
          `Service provider ${serviceProviderId} is unavailable at ${url}`,
          {
            status: 502,
            headers: new Headers({
              'X-Data-Set-ID': dataSetId,
            }),
          },
        )
        // Log retrieval result for error path as per original logic
        ctx.waitUntil(
          logRetrievalResult(env, {
            cacheMiss,
            responseStatus: originResponse.status,
            egressBytes: 0, // No egress for error response
            requestCountryCode,
            timestamp: requestTimestamp,
            dataSetId,
          }),
        )
      } else if (!originResponse.body) {
        // The upstream response does not have any readable body
        finalResponse = new Response(originResponse.body, originResponse)
        finalResponse.headers.set('X-Data-Set-ID', dataSetId)
        finalResponse.headers.set(
          'Cache-Control',
          `public, max-age=${env.CLIENT_CACHE_TTL}`,
        )
        // Log retrieval result for no-body path as per original logic
        ctx.waitUntil(
          logRetrievalResult(env, {
            cacheMiss,
            responseStatus: originResponse.status,
            egressBytes: 0, // No egress for no-body response
            requestCountryCode,
            timestamp: requestTimestamp,
            dataSetId,
          }),
        )
      } else {
        // Stream and count bytes
        // We create two identical streams, one for the egress measurement and the other for returning the response as soon as possible
        const [returnedStream, egressMeasurementStream] =
          originResponse.body.tee()
        const reader = egressMeasurementStream.getReader()
        const firstByteAt = performance.now()

        ctx.waitUntil(
          (async () => {
            egressBytesForLogging = await measureStreamedEgress(reader)
            const lastByteFetchedAt = performance.now()

            await logRetrievalResult(env, {
              cacheMiss,
              responseStatus: originResponse.status,
              egressBytes: egressBytesForLogging, // Log actual measured egress
              requestCountryCode,
              timestamp: requestTimestamp,
              performanceStats: {
                fetchTtfb: firstByteAt - fetchStartedAt,
                fetchTtlb: lastByteFetchedAt - fetchStartedAt,
                workerTtfb: firstByteAt - workerStartedAt,
              },
              dataSetId,
            })

            await updateDataSetStats(env, { dataSetId, egressBytes: egressBytesForLogging })
          })(),
        )

        // Return immediately, proxying the transformed response
        finalResponse = new Response(returnedStream, {
          status: originResponse.status,
          statusText: originResponse.statusText,
          headers: originResponse.headers,
        })
        finalResponse.headers.set('X-Data-Set-ID', dataSetId)
        finalResponse.headers.set(
          'Cache-Control',
          `public, max-age=${env.CLIENT_CACHE_TTL}`,
        )
      }

      // Apply common headers and security policy
      setContentSecurityPolicy(finalResponse);

      // Step 4e: Add Response Headers with calculated remaining quotas
      finalResponse.headers.set('X-Egress-Quota-CDN-Remaining', reportedCdnRemaining.toString());
      finalResponse.headers.set('X-Egress-Quota-Cache-Miss-Remaining', reportedCacheMissRemaining.toString());

      // Step 4f: Decrement Persistent Quotas Asynchronously in the Durable Object
      // Use event.waitUntil to ensure the Durable Object update finishes even if the response is sent.
      ctx.waitUntil(
        quotaManagerStub.fetch('/decrementQuotas', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          // Send the egressType and actualEgressAmount determined by retrieveFile for DO decrement
          body: JSON.stringify({ type: egressType, amount: actualEgressAmount }),
        }).then(response => {
          if (!response.ok) {
            console.error(`Failed to decrement quota for ${egressType} by ${actualEgressAmount} bytes: ${response.status} ${response.statusText}`);
          }
        }).catch(error => {
          console.error(`Error decrementing quota for ${egressType} by ${actualEgressAmount} bytes:`, error);
        })
      );

      // Step 4g: Return Modified Response
      return finalResponse;

    } catch (error) {
      const { status } = getErrorHttpStatusMessage(error)

      ctx.waitUntil(
        logRetrievalResult(env, {
          cacheMiss: null,
          responseStatus: status,
          egressBytes: null, // Null egress for request failures before response streaming
          requestCountryCode,
          timestamp: requestTimestamp,
          dataSetId: null,
        }),
      )

      throw error
    }
  },

  /**
   * @param {unknown} error
   * @returns
   */
  _handleError(error) {
    const { status, message } = getErrorHttpStatusMessage(error)

    if (status >= 500) {
      console.error(error)
    }
    return new Response(message, { status })
  },
}

/**
 * Extracts status and message from an error object.
 *
 * - If the error has a numeric `status`, it is used; otherwise, defaults to 500.
 * - If the status is < 500 and a string `message` exists, it's used; otherwise, a
 *   generic message is returned.
 *
 * @param {unknown} error - The error object to extract from.
 * @returns {{ status: number; message: string }}
 */
function getErrorHttpStatusMessage(error) {
  const isObject = typeof error === 'object' && error !== null
  const status =
    isObject && 'status' in error && typeof error.status === 'number'
      ? error.status
      : 500

  const message =
    isObject &&
    status < 500 &&
    'message' in error &&
    typeof error.message === 'string'
      ? error.message
      : 'Internal Server Error'

  return { status, message }
}