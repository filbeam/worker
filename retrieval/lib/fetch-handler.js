import { handleError } from './http-error.js'
import { httpAssert } from './http-assert.js'
import { redirectLegacyDomain } from './redirect.js'

/**
 * Runs a worker's fetch implementation with the shared request lifecycle: log
 * when the request is aborted, reject non-GET/HEAD methods with a 405, redirect
 * legacy `*.filcdn.io` requests to `*.filbeam.io`, and turn thrown errors into
 * HTTP responses via {@link handleError}.
 *
 * @param {Request} request
 * @param {() => Promise<Response>} run - Invokes the worker's request handler.
 * @returns {Promise<Response>}
 */
export async function handleFetchRequest(request, run) {
  request.signal.addEventListener('abort', () => {
    console.log('The request was aborted!', { url: request.url })
  })
  try {
    httpAssert(
      ['GET', 'HEAD'].includes(request.method),
      405,
      'Method Not Allowed',
    )
    const legacyRedirect = redirectLegacyDomain(request)
    if (legacyRedirect) return legacyRedirect
    return await run()
  } catch (error) {
    return handleError(error)
  }
}
