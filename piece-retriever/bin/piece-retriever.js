import {
  httpAssert,
  assertCidNotDenied,
  handleFetchRequest,
  selectRetrievalCandidate,
} from '@filbeam/retrieval'

import { parseRequest } from '../lib/request.js'
import {
  retrieveFile as defaultRetrieveFile,
  getRetrievalUrl,
} from '../lib/retrieval.js'
import { getRetrievalCandidatesAndValidatePayer } from '../lib/store.js'

export default {
  /**
   * @param {Request} request
   * @param {Env} env
   * @param {ExecutionContext} ctx
   * @param {object} options
   * @param {typeof defaultRetrieveFile} [options.retrieveFile]
   * @returns
   */
  async fetch(request, env, ctx, options) {
    return handleFetchRequest(request, env, ctx, (context) =>
      this._fetch(request, env, ctx, options, context),
    )
  },

  /**
   * @param {Request} request
   * @param {Env} env
   * @param {ExecutionContext} ctx
   * @param {object} options
   * @param {typeof defaultRetrieveFile} [options.retrieveFile]
   * @param {import('@filbeam/retrieval').RequestContext} context
   * @returns {Promise<Response | import('@filbeam/retrieval').Retrieve>}
   */
  async _fetch(
    request,
    env,
    ctx,
    { retrieveFile = defaultRetrieveFile } = {},
    context,
  ) {
    if (URL.parse(request.url)?.pathname === '/') {
      return Response.redirect('https://filbeam.com/', 302)
    }

    const { payerWalletAddress, pieceCid, botName, validateCacheMissResponse } =
      parseRequest(request, env)
    context.botName = botName

    return async () => {
      // Timestamp to measure file retrieval performance (from cache and from SP)
      const fetchStartedAt = performance.now()

      const [retrievalCandidates] = await Promise.all([
        getRetrievalCandidatesAndValidatePayer(
          env,
          payerWalletAddress,
          pieceCid,
          env.ENFORCE_EGRESS_QUOTA,
        ),
        assertCidNotDenied(env, pieceCid),
      ])

      httpAssert(
        retrievalCandidates.length > 0,
        500,
        'Service provider lookup failed',
      )

      const {
        failureResponse,
        candidate: retrievalCandidate,
        result: retrievalResult,
      } = await selectRetrievalCandidate(
        retrievalCandidates,
        (candidate) =>
          retrieveFile(
            ctx,
            candidate.serviceUrl,
            pieceCid,
            request,
            env.ORIGIN_CACHE_TTL,
            {
              signal: request.signal,
              addCacheMissResponseValidation: validateCacheMissResponse,
            },
          ),
        {
          env,
          ctx,
          requestCountryCode: context.requestCountryCode,
          timestamp: context.requestTimestamp,
          botName,
        },
      )
      if (failureResponse) return failureResponse
      httpAssert(
        retrievalCandidate && retrievalResult,
        500,
        'should never happen',
      )

      return {
        response: retrievalResult.response,
        cacheMiss: retrievalResult.cacheMiss,
        dataSetId: retrievalCandidate.dataSetId,
        fetchStartedAt,
        // Validate the cache-miss response (a `?validate` request) once it has
        // streamed, and drop the cache entry when it fails validation.
        finalizeCacheMiss: async () => {
          const cacheMissResponseValid =
            typeof retrievalResult.validate === 'function'
              ? retrievalResult.validate()
              : null
          if (cacheMissResponseValid === false) {
            await caches.default.delete(
              getRetrievalUrl(retrievalCandidate.serviceUrl, pieceCid),
            )
          }
          return { cacheMissResponseValid }
        },
      }
    }
  },
}
