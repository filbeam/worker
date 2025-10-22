import { describe, it, expect } from 'vitest'
import analyticsWriter from '../bin/analytics-writer.js'

const TEST_AUTH_KEY = 'test-auth-key-123'

describe('Analytics Writer', () => {
  it('should reject requests without authentication', async () => {
    const request = new Request('https://example.com', { method: 'POST' })
    const env = { 
      analytics_engine: { writeDataPoint: () => {} },
      ANALYTICS_AUTH_KEY: TEST_AUTH_KEY
    }
    
    const response = await analyticsWriter.fetch(request, env)
    
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
    
    const response = await analyticsWriter.fetch(request, env)
    
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
    
    const response = await analyticsWriter.fetch(request, env)
    
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
    
    const response = await analyticsWriter.fetch(request, env)
    
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
    
    const response = await analyticsWriter.fetch(request, env)
    
    expect(response.status).toBe(200)
    const body = await response.json()
    expect(body.success).toBe(true)
    expect(dataPointWritten).toBe(true)
  })

  it('should accept valid TTFB data with indexes array', async () => {
    const validData = {
      blobs: ['https://example.com', 'US', 'bot', 'QmTest123'],
      doubles: [150, 200, 1024],
      indexes: ['filbeam-bot-002']
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
          expect(data.indexes).toEqual(validData.indexes)
        } 
      },
      ANALYTICS_AUTH_KEY: TEST_AUTH_KEY
    }
    
    const response = await analyticsWriter.fetch(request, env)
    
    expect(response.status).toBe(200)
    const body = await response.json()
    expect(body.success).toBe(true)
    expect(dataPointWritten).toBe(true)
  })

  it('should reject requests with more than one index', async () => {
    const invalidData = {
      blobs: ['https://example.com', 'US', 'bot', 'QmTest123'],
      doubles: [150, 200, 1024],
      indexes: ['index1', 'index2']
    }
    
    const request = new Request('https://example.com', {
      method: 'POST',
      headers: { 'X-Analytics-Auth': TEST_AUTH_KEY },
      body: JSON.stringify(invalidData)
    })
    
    const env = { 
      analytics_engine: { writeDataPoint: () => {} },
      ANALYTICS_AUTH_KEY: TEST_AUTH_KEY
    }
    
    const response = await analyticsWriter.fetch(request, env)
    
    expect(response.status).toBe(400)
    const body = await response.json()
    expect(body.success).toBe(false)
    expect(body.error).toBe('indexes must be an array with at most 1 item')
  })

  it('should accept empty indexes array', async () => {
    const validData = {
      blobs: ['https://example.com', 'US', 'bot', 'QmTest123'],
      doubles: [150, 200, 1024],
      indexes: []
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
          expect(data.indexes).toBeUndefined()
        } 
      },
      ANALYTICS_AUTH_KEY: TEST_AUTH_KEY
    }
    
    const response = await analyticsWriter.fetch(request, env)
    
    expect(response.status).toBe(200)
    const body = await response.json()
    expect(body.success).toBe(true)
    expect(dataPointWritten).toBe(true)
  })

  // Analytics Engine Limits Tests
  it('should reject requests with too many blobs', async () => {
    const invalidData = {
      blobs: Array(21).fill('test'), // 21 blobs (exceeds limit of 20)
      doubles: [150, 200, 1024]
    }
    
    const request = new Request('https://example.com', {
      method: 'POST',
      headers: { 'X-Analytics-Auth': TEST_AUTH_KEY },
      body: JSON.stringify(invalidData)
    })
    
    const env = { 
      analytics_engine: { writeDataPoint: () => {} },
      ANALYTICS_AUTH_KEY: TEST_AUTH_KEY
    }
    
    const response = await analyticsWriter.fetch(request, env)
    
    expect(response.status).toBe(400)
    const body = await response.json()
    expect(body.success).toBe(false)
    expect(body.error).toBe('Too many blobs (max 20)')
  })

  it('should reject requests with blobs too large', async () => {
    const largeBlob = 'x'.repeat(17 * 1024) // 17KB (exceeds 16KB limit)
    const invalidData = {
      blobs: [largeBlob, 'US', 'bot', 'QmTest123'],
      doubles: [150, 200, 1024]
    }
    
    const request = new Request('https://example.com', {
      method: 'POST',
      headers: { 'X-Analytics-Auth': TEST_AUTH_KEY },
      body: JSON.stringify(invalidData)
    })
    
    const env = { 
      analytics_engine: { writeDataPoint: () => {} },
      ANALYTICS_AUTH_KEY: TEST_AUTH_KEY
    }
    
    const response = await analyticsWriter.fetch(request, env)
    
    expect(response.status).toBe(400)
    const body = await response.json()
    expect(body.success).toBe(false)
    expect(body.error).toBe('Blobs too large (max 16KB)')
  })

  it('should reject requests with too many doubles', async () => {
    const invalidData = {
      blobs: ['https://example.com', 'US', 'bot', 'QmTest123'],
      doubles: Array(21).fill(100) // 21 doubles (exceeds limit of 20)
    }
    
    const request = new Request('https://example.com', {
      method: 'POST',
      headers: { 'X-Analytics-Auth': TEST_AUTH_KEY },
      body: JSON.stringify(invalidData)
    })
    
    const env = { 
      analytics_engine: { writeDataPoint: () => {} },
      ANALYTICS_AUTH_KEY: TEST_AUTH_KEY
    }
    
    const response = await analyticsWriter.fetch(request, env)
    
    expect(response.status).toBe(400)
    const body = await response.json()
    expect(body.success).toBe(false)
    expect(body.error).toBe('Too many doubles (max 20)')
  })

  it('should reject requests with index too large', async () => {
    const largeIndex = 'x'.repeat(97) // 97 bytes (exceeds 96 byte limit)
    const invalidData = {
      blobs: ['https://example.com', 'US', 'bot', 'QmTest123'],
      doubles: [150, 200, 1024],
      indexes: [largeIndex]
    }
    
    const request = new Request('https://example.com', {
      method: 'POST',
      headers: { 'X-Analytics-Auth': TEST_AUTH_KEY },
      body: JSON.stringify(invalidData)
    })
    
    const env = { 
      analytics_engine: { writeDataPoint: () => {} },
      ANALYTICS_AUTH_KEY: TEST_AUTH_KEY
    }
    
    const response = await analyticsWriter.fetch(request, env)
    
    expect(response.status).toBe(400)
    const body = await response.json()
    expect(body.success).toBe(false)
    expect(body.error).toBe('Index too large (max 96 bytes)')
  })
})