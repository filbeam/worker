export * from './lib/access.js'
export * from './lib/address.js'
export * from './lib/bad-bits-util.js'
export * from './lib/bot-auth.js'
export * from './lib/candidate-selection.js'
export * from './lib/content-security-policy.js'
export * from './lib/fetch-handler.js'
export * from './lib/http-assert.js'
export * from './lib/http-error.js'
export * from './lib/origin-cache.js'
export * from './lib/redirect.js'
export * from './lib/response-headers.js'
export * from './lib/retrieval-failure.js'
export * from './lib/stats.js'

export default {
  /**
   * @param {Request} _request
   * @param {Env} _env
   * @param {ExecutionContext} _ctx
   */
  async fetch(_request, _env, _ctx) {
    return new Response('Not Implemented', { status: 501 })
  },
}
