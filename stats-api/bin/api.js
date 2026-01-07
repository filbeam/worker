import { handleGetDataSetStats, handleGetPayerStats } from '../lib/handlers.js'

export default {
  /**
   * Handles incoming HTTP requests to the stats API
   *
   * @param {Request} request - The incoming request
   * @param {Env} env - Environment bindings including database
   * @param {ExecutionContext} ctx - Execution context
   * @returns {Promise<Response>} JSON response with egress quotas or error
   */
  async fetch(request, env, ctx) {
    const response = await this._fetch(request, env, ctx)
    response.headers.set('Access-Control-Allow-Origin', '*')
    response.headers.set('Access-Control-Allow-Methods', 'GET, OPTIONS')
    return response
  },

  /**
   * @param {Request} request - The incoming request
   * @param {Env} env - Environment bindings including database
   * @param {ExecutionContext} ctx - Execution context
   * @returns {Promise<Response>} JSON response with egress quotas or error
   */
  async _fetch(request, env, ctx) {
    try {
      if (request.method === 'OPTIONS') {
        // CORS pre-flight request
        // Access-Control-* headers are added in the outer fetch method
        return new Response(null, { status: 204 })
      }

      if (request.method !== 'GET') {
        return new Response('Method Not Allowed', {
          status: 405,
          headers: { Allow: 'GET' },
        })
      }

      const url = new URL(request.url)
      const pathSegments = url.pathname.split('/').filter(Boolean)

      if (pathSegments.length === 2 && pathSegments[0] === 'data-set') {
        const dataSetId = pathSegments[1]
        return await handleGetDataSetStats(env, dataSetId)
      } else if (pathSegments.length === 2 && pathSegments[0] === 'payer') {
        const payerAddress = pathSegments[1].toLowerCase()
        return await handleGetPayerStats(env, payerAddress)
      }

      return new Response('Not Found', { status: 404 })
    } catch (error) {
      console.error('Stats API error:', error)

      return new Response('Internal Server Error', { status: 500 })
    }
  },
}
