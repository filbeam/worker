/**
 * @param {any} condition
 * @param {number} status
 * @param {string} message
 * @returns {asserts condition}
 */
export const httpAssert = (condition, status, message) => {
  if (!condition) {
    const error = new Error(message)
    Object.assign(error, { status })
    throw error
  }
}
