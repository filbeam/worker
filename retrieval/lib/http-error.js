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
export function getErrorHttpStatusMessage(error) {
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
