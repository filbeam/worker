import { handleError } from './http-error.js'

/**
 * Runs a worker's fetch implementation with the shared request lifecycle: log
 * when the request is aborted, and turn thrown errors into HTTP responses via
 * {@link handleError}.
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
    return await run()
  } catch (error) {
    return handleError(error)
  }
}
