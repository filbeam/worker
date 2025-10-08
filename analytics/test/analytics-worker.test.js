import { describe, it, expect } from 'vitest'
import analyticsWorker from '../bin/analytics-worker.js'

describe('Analytics Worker', () => {
  it('should reject non-POST requests', async () => {
    const request = new Request('https://example.com', { method: 'GET' })
    const env = { ANALYTICS_ENGINE: { writeDataPoint: () => {} } }
    
    const response = await analyticsWorker.fetch(request, env, {})
    
    expect(response.status).toBe(405)
    const body = await response.json()
    expect(body.success).toBe(false)
    expect(body.error).toBe('Send POST with TTFB data')
  })

  it('should reject requests with invalid data structure', async () => {
    const request = new Request('https://example.com', {
      method: 'POST',
      body: JSON.stringify({ invalid: 'data' })
    })
    const env = { ANALYTICS_ENGINE: { writeDataPoint: () => {} } }
    
    const response = await analyticsWorker.fetch(request, env, {})
    
    expect(response.status).toBe(400)
    const body = await response.json()
    expect(body.success).toBe(false)
    expect(body.error).toContain('Missing required fields')
  })

  it('should accept valid TTFB data', async () => {
    const validData = {
      blobs: ['https://example.com', 'US', 'bot', 'QmTest123'],
      doubles: [150, 200, 1024]
    }
    
    const request = new Request('https://example.com', {
      method: 'POST',
      body: JSON.stringify(validData)
    })
    
    let dataPointWritten = false
    const env = { 
      ANALYTICS_ENGINE: { 
        writeDataPoint: (data) => {
          dataPointWritten = true
          expect(data.blobs).toEqual(validData.blobs)
          expect(data.doubles).toEqual(validData.doubles)
        } 
      } 
    }
    
    const response = await analyticsWorker.fetch(request, env, {})
    
    expect(response.status).toBe(200)
    const body = await response.json()
    expect(body.success).toBe(true)
    expect(dataPointWritten).toBe(true)
  })
})