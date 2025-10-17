import { describe, it, expect } from 'vitest'
import analyticsWorker from '../bin/analytics-worker.js'

const TEST_AUTH_KEY = 'test-auth-key-123'

describe('Analytics Worker', () => {
  it('should reject requests without authentication', async () => {
    const request = new Request('https://example.com', { method: 'POST' })
    const env = { 
      analytics_engine: { writeDataPoint: () => {} },
      ANALYTICS_AUTH_KEY: TEST_AUTH_KEY
    }
    
    const response = await analyticsWorker.fetch(request, env)
    
    expect(response.status).toBe(401)
    const body = await response.json()
    expect(body.success).toBe(false)
    expect(body.error).toBe('Unauthorized')
  })

  it('should reject requests with invalid authentication', async () => {
    const request = new Request('https://example.com', {
      method: 'POST',
      headers: { 'X-Analytics-Auth': 'wrong-key' }
    })
    const env = { 
      analytics_engine: { writeDataPoint: () => {} },
      ANALYTICS_AUTH_KEY: TEST_AUTH_KEY
    }
    
    const response = await analyticsWorker.fetch(request, env)
    
    expect(response.status).toBe(401)
    const body = await response.json()
    expect(body.success).toBe(false)
    expect(body.error).toBe('Unauthorized')
  })

  it('should reject non-POST requests', async () => {
    const request = new Request('https://example.com', {
      method: 'GET',
      headers: { 'X-Analytics-Auth': TEST_AUTH_KEY }
    })
    const env = { 
      analytics_engine: { writeDataPoint: () => {} },
      ANALYTICS_AUTH_KEY: TEST_AUTH_KEY
    }
    
    const response = await analyticsWorker.fetch(request, env)
    
    expect(response.status).toBe(405)
    const body = await response.json()
    expect(body.success).toBe(false)
    expect(body.error).toBe('Method not allowed')
  })

  it('should reject requests with invalid data structure', async () => {
    const request = new Request('https://example.com', {
      method: 'POST',
      headers: { 'X-Analytics-Auth': TEST_AUTH_KEY },
      body: JSON.stringify({ invalid: 'data' })
    })
    const env = { 
      analytics_engine: { writeDataPoint: () => {} },
      ANALYTICS_AUTH_KEY: TEST_AUTH_KEY
    }
    
    const response = await analyticsWorker.fetch(request, env)
    
    expect(response.status).toBe(400)
    const body = await response.json()
    expect(body.success).toBe(false)
    expect(body.error).toBe('Missing required fields')
  })

  it('should accept valid TTFB data', async () => {
    const validData = {
      blobs: ['https://example.com', 'US', 'bot', 'QmTest123'],
      doubles: [150, 200, 1024]
    }
    
    const request = new Request('https://example.com', {
      method: 'POST',
      headers: { 'X-Analytics-Auth': TEST_AUTH_KEY },
      body: JSON.stringify(validData)
    })
    
    let dataPointWritten = false
    const env = { 
      analytics_engine: { 
        writeDataPoint: (data) => {
          dataPointWritten = true
          expect(data.blobs).toEqual(validData.blobs)
          expect(data.doubles).toEqual(validData.doubles)
        } 
      },
      ANALYTICS_AUTH_KEY: TEST_AUTH_KEY
    }
    
    const response = await analyticsWorker.fetch(request, env)
    
    expect(response.status).toBe(200)
    const body = await response.json()
    expect(body.success).toBe(true)
    expect(dataPointWritten).toBe(true)
  })
})