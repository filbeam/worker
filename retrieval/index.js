export * from './lib/address.js'
export * from './lib/bad-bits-util.js'
export * from './lib/content-security-policy.js'
export * from './lib/http-assert.js'
export * from './lib/stats.js'
export * from './lib/response.js'

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
