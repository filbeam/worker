export default {
  /**
   * @param {Request} request
   * @param {{ analytics_engine: AnalyticsEngineDataset, ANALYTICS_AUTH_KEY: string }} env
   */
  async fetch(request, env) {
    /**
     * @param {string} AUTH_HEADER_KEY Custom header to check for authentication
     * @param {string} authKey Pre-shared authentication key from environment
     */
    const AUTH_HEADER_KEY = 'X-Analytics-Auth'
    const authKey = request.headers.get(AUTH_HEADER_KEY)

    if (authKey !== env.ANALYTICS_AUTH_KEY) {
      return new Response(JSON.stringify({ success: false, error: 'Unauthorized' }), {
        status: 401,
        headers: { 'Content-Type': 'application/json' }
      })
    }

    if (request.method !== 'POST') {
      return new Response(JSON.stringify({ success: false, error: 'Method not allowed' }), {
        status: 405,
        headers: { 'Content-Type': 'application/json' }
      })
    }

    try {
      const data = await request.json()

      // Validate data structure
      if (!data.blobs || !data.doubles) {
        return new Response(JSON.stringify({ success: false, error: 'Missing required fields' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' }
        })
      }

      if (!Array.isArray(data.blobs) || data.blobs.length !== 4) {
        return new Response(JSON.stringify({ success: false, error: 'Invalid blobs array' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' }
        })
      }

      if (!Array.isArray(data.doubles) || data.doubles.length !== 3) {
        return new Response(JSON.stringify({ success: false, error: 'Invalid doubles array' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' }
        })
      }

      // Write to Analytics Engine
      env.analytics_engine.writeDataPoint({
        blobs: data.blobs,   // [url, location, client, cid]
        doubles: data.doubles // [ttfb, status, bytes]
      })

      return new Response(JSON.stringify({ success: true }), {
        headers: { 'Content-Type': 'application/json' }
      })
    } catch (error) {
      return new Response(JSON.stringify({
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error'
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      })
    }
  }
}
