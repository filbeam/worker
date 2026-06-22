import { describe, it, expect } from 'vitest'
import { getErrorHttpStatusMessage } from '../lib/http-error.js'

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
