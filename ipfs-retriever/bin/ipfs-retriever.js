import { isValidEthereumAddress } from '../lib/address.js'
import { parseRequest } from '../lib/request.js'
import {
  retrieveIpfsContent as defaultRetrieveIpfsContent,
  measureStreamedEgress,
} from '../lib/retrieval.js'
import {
  getStorageProviderAndValidatePayer,
  logRetrievalResult,
  updateDataSetStats,
  getSlugForWalletAndCid,
} from '../lib/store.js'
import { httpAssert } from '../lib/http-assert.js'
import { setContentSecurityPolicy } from '../lib/content-security-policy.js'
import { findInBadBits } from '../lib/bad-bits-util.js'

// We need to keep an explicit definition of RetrieverEnv because our monorepo has multiple
// worker-configuration.d.ts files, each file (re)defining the global Env interface, causing the
// final Env interface to contain only properties available to all workers.
/**
 * @typedef {{
 *   ENVIRONMENT: 'dev' | 'calibration ' | 'mainnet'
 *   ORIGIN_CACHE_TTL: 86400
 *   CLIENT_CACHE_TTL: 31536000
 *   DNS_ROOT: '.localhost' | '.calibration.filbeam.io' | '.filbeam.io'
 *   DB: D1Database
 * }} RetrieverEnv
 */
export default {
  /**
   * @param {Request} request
   * @param {RetrieverEnv} env
   * @param {ExecutionContext} ctx
   * @param {object} options
   * @param {typeof defaultRetrieveIpfsContent} [options.retrieveIpfsContent]
   * @returns
   */
  async fetch(
    request,
    env,
    ctx,
    { retrieveIpfsContent = defaultRetrieveIpfsContent } = {},
  ) {
    try {
      return await this._fetch(request, env, ctx, {
        retrieveIpfsContent,
      })
    } catch (error) {
      return this._handleError(error)
    }
  },

  /**
   * @param {Request} request
   * @param {RetrieverEnv} env
   * @param {ExecutionContext} ctx
   * @param {object} options
   * @param {typeof defaultRetrieveIpfsContent} [options.retrieveIpfsContent:]
   * @returns
   */
  async _fetch(
    request,
    env,
    ctx,
    { retrieveIpfsContent = defaultRetrieveIpfsContent } = {},
  ) {
    httpAssert(
      ['GET', 'HEAD'].includes(request.method),
      405,
      'Method Not Allowed',
    )

    if (URL.parse(request.url)?.hostname === env.DNS_ROOT.slice(1)) {
      return handleDnsRootRequest(request, env)
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

    const { payerWalletAddress, ipfsRootCid, ipfsSubpath } = parseRequest(
      request,
      env,
    )

    httpAssert(
      isValidEthereumAddress(payerWalletAddress),
      400,
      `Invalid address: ${payerWalletAddress}. Address must be a valid ethereum address.`,
    )

    try {
      // Timestamp to measure file retrieval performance (from cache and from SP)
      const fetchStartedAt = performance.now()

      const [{ serviceProviderId, serviceUrl, dataSetId }, isBadBit] =
        await Promise.all([
          getStorageProviderAndValidatePayer(
            env,
            payerWalletAddress,
            ipfsRootCid,
          ),
          findInBadBits(env, ipfsRootCid),
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

      const { response: originResponse, cacheMiss } = await retrieveIpfsContent(
        serviceUrl,
        ipfsRootCid,
        ipfsSubpath,
        env.ORIGIN_CACHE_TTL,
        { signal: request.signal },
      )

      if (!originResponse.body) {
        // The upstream response does not have any readable body
        // There is no need to measure response body size, we can
        // return the original response object.
        ctx.waitUntil(
          logRetrievalResult(env, {
            cacheMiss,
            responseStatus: originResponse.status,
            egressBytes: 0,
            requestCountryCode,
            timestamp: requestTimestamp,
            dataSetId,
          }),
        )
        const response = new Response(originResponse.body, originResponse)
        setContentSecurityPolicy(response)
        response.headers.set('X-Data-Set-ID', dataSetId)
        response.headers.set(
          'Cache-Control',
          `public, max-age=${env.CLIENT_CACHE_TTL}`,
        )
        return response
      }

      // Stream and count bytes
      // We create two identical streams, one for the egress measurement and the other for returning the response as soon as possible
      const [returnedStream, egressMeasurementStream] =
        originResponse.body.tee()
      const reader = egressMeasurementStream.getReader()
      const firstByteAt = performance.now()

      ctx.waitUntil(
        (async () => {
          const egressBytes = await measureStreamedEgress(reader)
          const lastByteFetchedAt = performance.now()

          await logRetrievalResult(env, {
            cacheMiss,
            responseStatus: originResponse.status,
            egressBytes,
            requestCountryCode,
            timestamp: requestTimestamp,
            performanceStats: {
              fetchTtfb: firstByteAt - fetchStartedAt,
              fetchTtlb: lastByteFetchedAt - fetchStartedAt,
              workerTtfb: firstByteAt - workerStartedAt,
            },
            dataSetId,
          })

          await updateDataSetStats(env, { dataSetId, egressBytes })
        })(),
      )

      // Return immediately, proxying the transformed response
      const response = new Response(returnedStream, {
        status: originResponse.status,
        statusText: originResponse.statusText,
        headers: originResponse.headers,
      })
      setContentSecurityPolicy(response)
      response.headers.set('X-Data-Set-ID', dataSetId)
      response.headers.set(
        'Cache-Control',
        `public, max-age=${env.CLIENT_CACHE_TTL}`,
      )
      return response
    } catch (error) {
      const { status } = getErrorHttpStatusMessage(error)

      ctx.waitUntil(
        logRetrievalResult(env, {
          cacheMiss: null,
          responseStatus: status,
          egressBytes: null,
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

/**
 * Handles requests to the bare DNS_ROOT domain (e.g., ipfs.filbeam.io).
 *
 * - If no path is provided, redirects to https://filbeam.com
 * - If path is /wallet/cid or /wallet/cid/pathname, generates a slug and
 *   redirects to the subdomain-based URL
 *
 * @param {Request} request - The incoming request
 * @param {RetrieverEnv} env - Worker environment
 * @returns {Promise<Response>} Redirect response
 */
async function handleDnsRootRequest(request, env) {
  // Parse the URL path to extract wallet, cid, and optional pathname
  const parsedUrl = URL.parse(request.url)
  const pathname = parsedUrl?.pathname || '/'

  // If no path, redirect to filbeam.com
  if (pathname === '/' || pathname === '') {
    return Response.redirect('https://filbeam.com/', 302)
  }

  // Parse path as /wallet/cid/pathname
  const pathParts = pathname.slice(1).split('/') // Remove leading slash and split

  if (pathParts.length < 2) {
    httpAssert(
      false,
      404,
      'Invalid path format. Expected: /wallet/cid or /wallet/cid/pathname',
    )
  }

  const wallet = pathParts[0]
  const cid = pathParts[1]
  const subpath = pathParts.slice(2).join('/')

  // Validate wallet address
  httpAssert(
    isValidEthereumAddress(wallet),
    404,
    `Invalid wallet address: ${wallet}. Address must be a valid ethereum address.`,
  )

  // Get slug for the wallet and CID
  const slug = await getSlugForWalletAndCid(env, wallet, cid)

  // Build redirect URL
  const redirectPath = subpath ? `/${subpath}` : ''
  const redirectUrl = `https://${slug}${env.DNS_ROOT}${redirectPath}`

  return Response.redirect(redirectUrl, 302)
}
