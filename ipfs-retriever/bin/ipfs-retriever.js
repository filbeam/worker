import {
  isValidEthereumAddress,
  httpAssert,
  assertCidNotDenied,
  logRetrievalError,
  handleFetchRequest,
  selectRetrievalCandidate,
} from '@filbeam/retrieval'

import { parseRequest } from '../lib/request.js'
import {
  retrieveIpfsContent as defaultRetrieveIpfsContent,
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
    return handleFetchRequest(request, env, ctx, () =>
      this._fetch(request, env, ctx, options),
    )
  },

  /**
   * @param {Request} request
   * @param {Env} env
   * @param {ExecutionContext} ctx
   * @param {object} options
   * @param {typeof defaultRetrieveIpfsContent} [options.retrieveIpfsContent]
   * @returns {Promise<
   *   Response | import('@filbeam/retrieval').RetrievalOutcome
   * >}
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
      await assertCidNotDenied(env, ipfsRootCid)

      const {
        failureResponse,
        candidate,
        result: retrievalResult,
      } = await selectRetrievalCandidate(
        candidates,
        (candidate) =>
          retrieveIpfsContent(
            candidate.serviceUrl,
            ipfsRootCid,
            ipfsSubpath,
            env.ORIGIN_CACHE_TTL,
            { signal: request.signal },
          ),
        { env, ctx, requestCountryCode, timestamp: requestTimestamp, botName },
      )
      if (failureResponse) return failureResponse
      httpAssert(candidate && retrievalResult, 500, 'should never happen')

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

      // When converting CAR to raw, the headers already carry the CAR-to-raw
      // adjustments. A null body (e.g. a HEAD request) is served as-is.
      const response = responseBody
        ? new Response(responseBody, {
            status: originResponse.status,
            statusText: originResponse.statusText,
            headers: responseHeaders,
          })
        : originResponse

      return {
        response,
        cacheMiss,
        dataSetId: candidate.dataSetId,
        botName,
        requestCountryCode,
        timestamp: requestTimestamp,
        workerStartedAt,
        fetchStartedAt,
        // The client is served the raw bytes. On a cache miss the worker
        // fetched a (larger) CAR from the service provider, which the cache-miss
        // quota is charged for; for a passed-through CAR (`?format=car`) the two
        // are equal. Reaching here means the response streamed successfully (a
        // converted CAR is validated during conversion), so charge every cache
        // miss.
        finalizeCacheMiss: async (egressBytes) => ({
          cacheMissEgressBytes: originEgressBytes ?? egressBytes,
          cacheMissResponseValid: cacheMiss ? true : null,
        }),
      }
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
