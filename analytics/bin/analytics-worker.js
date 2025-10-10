// We need to keep an explicit definition of AnalyticsEnv because our monorepo has multiple
// worker-configuration.d.ts files, each file (re)defining the global Env interface, causing the
// final Env interface to contain only properties available to all workers.
/**
 * @typedef {{
 *   ENVIRONMENT: 'dev' | 'calibration' | 'mainnet'
 *   ANALYTICS_ENGINE: AnalyticsEngineDataset
 * }} AnalyticsEnv
 */

export default {
  /**
   * @param {Request} request
   * @param {AnalyticsEnv} env
   * @param {ExecutionContext} ctx
   * @returns {Promise<Response>}
   */
  async fetch(request, env, ctx) {
    try {
      return await this._fetch(request, env, ctx)
    } catch (error) {
      return this._handleError(error)
    }
  },

  /**
   * @param {Request} request
   * @param {AnalyticsEnv} env
   * @param {ExecutionContext} ctx
   * @returns {Promise<Response>}
   */
  async _fetch(request, env, ctx) {
    if (request.method !== "POST") {
      return new Response(JSON.stringify({
        success: false,
        error: "Send POST with TTFB data"
      }), {
        status: 405,
        headers: { "Content-Type": "application/json" }
      })
    }

    try {
      const data = await request.json()
      
      // Validate the data structure
      if (!data.blobs || !data.doubles) {
        return new Response(JSON.stringify({
          success: false,
          error: "Missing required fields: blobs and doubles are required"
        }), {
          status: 400,
          headers: { "Content-Type": "application/json" }
        })
      }

      // Validate blobs array (should contain [url, location, client, cid])
      if (!Array.isArray(data.blobs) || data.blobs.length !== 4) {
        return new Response(JSON.stringify({
          success: false,
          error: "blobs must be an array with exactly 4 elements: [url, location, client, cid]"
        }), {
          status: 400,
          headers: { "Content-Type": "application/json" }
        })
      }

      // Validate doubles array (should contain [ttfb, status, bytes])
      if (!Array.isArray(data.doubles) || data.doubles.length !== 3) {
        return new Response(JSON.stringify({
          success: false,
          error: "doubles must be an array with exactly 3 elements: [ttfb, status, bytes]"
        }), {
          status: 400,
          headers: { "Content-Type": "application/json" }
        })
      }

      // Write data to Analytics Engine
      env.ANALYTICS_ENGINE.writeDataPoint({
        blobs: data.blobs,   // [url, location, client, cid]
        doubles: data.doubles // [ttfb, status, bytes]
      })

      return new Response(JSON.stringify({ success: true }), {
        headers: { "Content-Type": "application/json" }
      })
    } catch (error) {
      return new Response(JSON.stringify({
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error'
      }), {
        status: 500,
        headers: { "Content-Type": "application/json" }
      })
    }
  },

  /**
   * @param {unknown} error
   * @returns {Response}
   */
  _handleError(error) {
    const { status, message } = this._getErrorHttpStatusMessage(error)

    if (status >= 500) {
      console.error(error)
    }
    
    return new Response(JSON.stringify({
      success: false,
      error: message
    }), {
      status,
      headers: { "Content-Type": "application/json" }
    })
  },

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
  _getErrorHttpStatusMessage(error) {
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
}
