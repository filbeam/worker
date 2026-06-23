import {
  isValidEthereumAddress,
  httpAssert,
  setRetrievalResponseHeaders,
  isCidDenied,
  BAD_BITS_DENIED_MESSAGE,
  logRetrievalResult,
  recordRetrieval,
  logRetrievalError,
  handleFetchRequest,
  selectRetrievalCandidate,
  handleNoServiceProviderResponse,
} from '@filbeam/retrieval'

import { parseRequest } from '../lib/request.js'
import {
  retrieveIpfsContent as defaultRetrieveIpfsContent,
  measureStreamedEgress,
  processIpfsResponse,
} from '../lib/retrieval.js'
import {
  getRetrievalCandidatesByDataSetAndPiece,
  getSlugForWalletAndCid,
} from '../lib/store.js'

export default {
  /**
   * @param {Request} request
   * @param {Env} env
   * @param {ExecutionContext} ctx
   * @param {object} options
   * @param {typeof defaultRetrieveIpfsContent} [options.retrieveIpfsContent]
   * @returns
   */
  async fetch(request, env, ctx, options) {
    return handleFetchRequest(request, () =>
      this._fetch(request, env, ctx, options),
    )
  },

  /**
   * @param {Request} request
   * @param {Env} env
   * @param {ExecutionContext} ctx
   * @param {object} options
   * @param {typeof defaultRetrieveIpfsContent} [options.retrieveIpfsContent]
   * @returns
   */
  async _fetch(
    request,
    env,
    ctx,
    { retrieveIpfsContent = defaultRetrieveIpfsContent } = {},
  ) {
    if (
      URL.parse(request.url)?.hostname === env.DNS_ROOT.slice(1) ||
      URL.parse(request.url)?.hostname === `link${env.DNS_ROOT}`
    ) {
      return handleDnsRootRequest(request, env)
    }

    const requestTimestamp = new Date().toISOString()
    const workerStartedAt = performance.now()
    const requestCountryCode = request.headers.get('CF-IPCountry')

    const { dataSetId, pieceId, ipfsSubpath, ipfsFormat, botName } =
      parseRequest(request, env)

    try {
      // Timestamp to measure file retrieval performance (from cache and from SP)
      const fetchStartedAt = performance.now()

      const candidates = await getRetrievalCandidatesByDataSetAndPiece(
        env,
        dataSetId,
        pieceId,
        env.ENFORCE_EGRESS_QUOTA,
      )
      // Every candidate serves the same content, so they share the root CID.
      const ipfsRootCid = candidates[0].ipfsRootCid

      // Now check Bad Bits with the ipfsRootCid we got from the database
      const isBadBit = await isCidDenied(env, ipfsRootCid)
      httpAssert(!isBadBit, 404, BAD_BITS_DENIED_MESSAGE)

      const {
        candidate,
        result: retrievalResult,
        attempts: retrievalAttempts,
      } = await selectRetrievalCandidate(candidates, (candidate) =>
        retrieveIpfsContent(
          candidate.serviceUrl,
          ipfsRootCid,
          ipfsSubpath,
          env.ORIGIN_CACHE_TTL,
          { signal: request.signal },
        ),
      )

      httpAssert(candidate, 500, 'should never happen')

      const noServiceProviderResponse = handleNoServiceProviderResponse(
        env,
        ctx,
        {
          retrievalResult,
          attempts: retrievalAttempts,
          dataSetId: candidate.dataSetId,
          requestCountryCode,
          timestamp: requestTimestamp,
          botName,
        },
      )
      if (noServiceProviderResponse) return noServiceProviderResponse

      httpAssert(retrievalResult, 500, 'should never happen')

      const originResponse = retrievalResult.response
      const cacheMiss = retrievalResult.cacheMiss

      const {
        body: responseBody,
        originEgressBytes,
        headers: responseHeaders,
      } = await processIpfsResponse(originResponse, {
        ipfsRootCid,
        ipfsSubpath,
        ipfsFormat,
        signal: request.signal,
      })

      if (!responseBody) {
        // The upstream response does not have any readable body
        // There is no need to measure response body size, we can
        // return the original response object.
        ctx.waitUntil(
          logRetrievalResult(env, {
            cacheMiss,
            cacheMissResponseValid: null,
            responseStatus: originResponse.status,
            egressBytes: 0,
            cacheMissEgressBytes: 0,
            requestCountryCode,
            timestamp: requestTimestamp,
            dataSetId: candidate.dataSetId,
            botName,
          }),
        )
        const response = new Response(originResponse.body, originResponse)
        setRetrievalResponseHeaders(response, {
          dataSetId: candidate.dataSetId,
          clientCacheTtl: env.CLIENT_CACHE_TTL,
        })
        return response
      }

      // Stream and count bytes
      // We create two identical streams, one for the egress measurement and the other for returning the response as soon as possible
      const [returnedStream, egressMeasurementStream] = responseBody.tee()
      const reader = egressMeasurementStream.getReader()
      const firstByteAt = performance.now()

      ctx.waitUntil(
        (async () => {
          const egressBytes = await measureStreamedEgress(reader)
          const lastByteFetchedAt = performance.now()

          // The client is served the raw bytes (`egressBytes`). On a cache miss
          // the worker fetched a CAR from the service provider, which is larger
          // than the raw bytes when converting from CAR to raw. The cache-miss
          // egress is charged for that CAR size. When the body is passed through
          // unchanged (e.g. `?format=car`), the two values are equal.
          const cacheMissEgressBytes = originEgressBytes ?? egressBytes

          await recordRetrieval(env, {
            cacheMiss,
            cacheMissResponseValid: null,
            responseStatus: originResponse.status,
            egressBytes,
            cacheMissEgressBytes,
            requestCountryCode,
            timestamp: requestTimestamp,
            performanceStats: {
              fetchTtfb: firstByteAt - fetchStartedAt,
              fetchTtlb: lastByteFetchedAt - fetchStartedAt,
              workerTtfb: firstByteAt - workerStartedAt,
            },
            dataSetId: candidate.dataSetId,
            botName,
            enforceEgressQuota: env.ENFORCE_EGRESS_QUOTA,
          })
        })(),
      )

      // Return immediately, proxying the transformed response. The headers
      // already carry the CAR-to-raw adjustments from processIpfsResponse.
      const response = new Response(returnedStream, {
        status: originResponse.status,
        statusText: originResponse.statusText,
        headers: responseHeaders,
      })
      setRetrievalResponseHeaders(response, {
        dataSetId: candidate.dataSetId,
        clientCacheTtl: env.CLIENT_CACHE_TTL,
      })

      return response
    } catch (error) {
      logRetrievalError(env, ctx, error, {
        requestCountryCode,
        timestamp: requestTimestamp,
        botName,
      })

      throw error
    }
  },
}

/**
 * Handles requests to the bare DNS_ROOT domain (e.g., ipfs.filbeam.io).
 *
 * - If no path is provided, redirects to https://filbeam.com
 * - If path is /wallet/cid or /wallet/cid/pathname, generates a slug and
 *   redirects to the subdomain-based URL
 *
 * @param {Request} request - The incoming request
 * @param {Env} env - Worker environment
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

  const wallet = pathParts[0].toLowerCase()
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
