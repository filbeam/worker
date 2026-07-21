import { describe, it, expect } from 'vitest'
import { getErrorHttpStatusMessage, handleError } from '../lib/http-error.js'

describe('getErrorHttpStatusMessage', () => {
  it('uses the numeric status and message for client errors', () => {
    expect(
      getErrorHttpStatusMessage(
        Object.assign(new Error('Not Found'), {
          status: 404,
        }),
      ),
    ).toEqual({ status: 404, message: 'Not Found' })
  })

  it('hides the message for server errors', () => {
    expect(
      getErrorHttpStatusMessage(
        Object.assign(new Error('boom'), {
          status: 500,
        }),
      ),
    ).toEqual({ status: 500, message: 'Internal Server Error' })
  })

  it('defaults to 500 when the error has no numeric status', () => {
    expect(getErrorHttpStatusMessage(new Error('boom'))).toEqual({
      status: 500,
      message: 'Internal Server Error',
    })
  })

  it('defaults to 500 for non-object errors', () => {
    expect(getErrorHttpStatusMessage('boom')).toEqual({
      status: 500,
      message: 'Internal Server Error',
    })
  })
})

describe('handleError', () => {
  it('returns the status and message for client errors', async () => {
    const res = handleError(
      Object.assign(new Error('Bad Request'), { status: 400 }),
    )
    expect(res.status).toBe(400)
    expect(await res.text()).toBe('Bad Request')
  })

  it('returns a generic 500 response for server errors', async () => {
    const res = handleError(new Error('boom'))
    expect(res.status).toBe(500)
    expect(await res.text()).toBe('Internal Server Error')
  })
})
