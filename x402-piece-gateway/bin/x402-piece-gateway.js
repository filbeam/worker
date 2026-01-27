import { httpAssert } from '@filbeam/retrieval'
import { buildForwardUrl, parseRequest } from '../lib/request.js'
import { useFacilitator as defaultUseFacilitator } from 'x402/verify'
import { settleResponseHeader } from 'x402/types'
import {
  buildPaymentRequirements,
  buildPaymentRequiredResponse,
} from '../lib/x402.js'
import { getPieceX402Metadata } from '../lib/store.js'
import { exact } from 'x402/schemes'

const x402Version = 1

export default {
  /**
   * @param {Request} request
   * @param {Env} env
   * @param {ExecutionContext} ctx
   * @param {object} options
   * @param {typeof defaultUseFacilitator} [options.useFacilitator]
   * @returns {Promise<Response>}
   */
  async fetch(
    request,
    env,
    ctx,
    { useFacilitator = defaultUseFacilitator } = {},
  ) {
    request.signal.addEventListener('abort', () => {
      console.log('The request was aborted!', { url: request.url })
    })
    try {
      return await this._fetch(request, env, ctx, { useFacilitator })
    } catch (error) {
      return this._handleError(error)
    }
  },

  /**
   * @param {Request} request
   * @param {Env} env
   * @param {ExecutionContext} ctx
   * @param {object} options
   * @param {Function} options.useFacilitator
   * @returns {Promise<Response>}
   */
  async _fetch(request, env, ctx, { useFacilitator }) {
    httpAssert(
      ['GET', 'HEAD'].includes(request.method),
      405,
      'Method Not Allowed',
    )
    const { verify, settle } = useFacilitator({ url: env.FACILITATOR_URL })

    const { payeeAddress, pieceCid, payment, isWebBrowser } = parseRequest(
      request,
      env,
    )

    const x402Metadata = await getPieceX402Metadata(env, pieceCid, payeeAddress)

    httpAssert(
      !x402Metadata?.is_sanctioned,
      403,
      `The wallet ${payeeAddress} paying for storage of Piece CID ${pieceCid} is sanctioned.`,
    )

    const forwardUrl = buildForwardUrl(env, payeeAddress, pieceCid)
    const forwardRequest = new Request(forwardUrl, request)
    if (!x402Metadata?.price) {
      return env.PIECE_RETRIEVER.fetch(forwardRequest)
    }

    const requirements = buildPaymentRequirements(
      payeeAddress,
      x402Metadata.price,
      request,
      env,
    )

    if (!payment) {
      return buildPaymentRequiredResponse(
        env,
        isWebBrowser,
        requirements,
        'Missing X-PAYMENT header',
      )
    }

    /** @type {import('x402/types').PaymentPayload} */
    let decodedPayment
    try {
      decodedPayment = exact.evm.decodePayment(payment)
      decodedPayment.x402Version = x402Version
    } catch (err) {
      console.error('Cannot decode x402 payment:', err)
      return buildPaymentRequiredResponse(
        env,
        false,
        requirements,
        'Invalid or malformed payment header',
      )
    }

    console.log('Verifying payment...')
    try {
      const verifyResult = await verify(decodedPayment, requirements)

      if (!verifyResult.isValid) {
        console.log('Payment verification failed:', verifyResult)
        return buildPaymentRequiredResponse(
          env,
          false,
          requirements,
          verifyResult.invalidReason,
        )
      }
    } catch (error) {
      console.error('Payment verification failed:', error)
      return buildPaymentRequiredResponse(
        env,
        false,
        requirements,
        error instanceof Error ? error.message : 'Payment verification failed',
      )
    }

    console.log('Payment verified, proxying to piece-retriever')

    const response = await env.PIECE_RETRIEVER.fetch(forwardRequest)

    if (!response.ok) {
      return response
    }

    console.log('Response OK, settling payment...')
    try {
      const settleResult = await settle(decodedPayment, requirements)

      if (settleResult.success) {
        console.log('Payment settled:', settleResult.transaction)
        const newResponse = new Response(response.body, response)
        newResponse.headers.set(
          'X-PAYMENT-RESPONSE',
          settleResponseHeader(settleResult),
        )
        return newResponse
      } else {
        console.error('Payment settlement failed:', settleResult)
        return buildPaymentRequiredResponse(
          env,
          false,
          requirements,
          `Payment settlement failed: ${settleResult.errorReason}`,
        )
      }
    } catch (error) {
      console.error('Payment settlement failed:', error)
      return buildPaymentRequiredResponse(
        env,
        false,
        requirements,
        error instanceof Error ? error.message : 'Failed to settle payment',
      )
    }
  },

  /**
   * @param {unknown} error
   * @returns {Response}
   */
  _handleError(error) {
    const { status, message } = getErrorHttpStatusMessage(error)

    if (status >= 500) {
      console.error(error)
    }
    return new Response(message, { status })
  },
}

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
function getErrorHttpStatusMessage(error) {
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
