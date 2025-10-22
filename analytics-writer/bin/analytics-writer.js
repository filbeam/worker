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

      // Validate Analytics Engine limits
      // Blobs: max 20 items, max 16KB total size
      if (data.blobs.length > 20) {
        return new Response(JSON.stringify({ success: false, error: 'Too many blobs (max 20)' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' }
        })
      }

      const blobsSize = data.blobs.reduce((/** @type {number} */ total, /** @type {string} */ blob) => total + new TextEncoder().encode(blob).length, 0)
      if (blobsSize > 16 * 1024) { // 16KB
        return new Response(JSON.stringify({ success: false, error: 'Blobs too large (max 16KB)' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' }
        })
      }

      // Doubles: max 20 items
      if (data.doubles.length > 20) {
        return new Response(JSON.stringify({ success: false, error: 'Too many doubles (max 20)' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' }
        })
      }

      // Validate indexes array if provided
      if (data.indexes !== undefined) {
        if (!Array.isArray(data.indexes) || data.indexes.length > 1) {
          return new Response(JSON.stringify({ 
            success: false, 
            error: 'indexes must be an array with at most 1 item' 
          }), {
            status: 400,
            headers: { 'Content-Type': 'application/json' }
          })
        }

        // Index: max 96 bytes
        if (data.indexes.length > 0) {
          const indexSize = new TextEncoder().encode(data.indexes[0]).length
          if (indexSize > 96) {
            return new Response(JSON.stringify({ success: false, error: 'Index too large (max 96 bytes)' }), {
              status: 400,
              headers: { 'Content-Type': 'application/json' }
            })
          }
        }
      }

      // Write to Analytics Engine
      // Blobs: dimensions for grouping and filtering [url, location, client, cid]
      // Doubles: numeric values [ttfb, status, bytes]  
      // Indexes: sampling key (single index only)
      const dataPoint = {
        blobs: data.blobs,
        doubles: data.doubles,
        ...(data.indexes && data.indexes.length > 0 && { indexes: data.indexes })
      }

      env.analytics_engine.writeDataPoint(dataPoint)

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
