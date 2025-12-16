import { httpAssert } from '@filbeam/retrieval'
import { parseRequest } from '../lib/request.js'
import { useFacilitator as defaultUseFacilitator } from 'x402/verify'
import {
  buildPaymentRequirements,
  verifyPayment,
  settlePayment,
  encodeSettleResponse,
  buildPaymentRequiredResponse,
  decodePayment,
} from '../lib/x402.js'

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

    const x402Metadata =
      /** @type {{ price: string; blockNumber: string } | null} */
      (
        await env.X402_METADATA_KV.get(`${payeeAddress}:${pieceCid}`, {
          type: 'json',
        })
      )

    // No metadata = free content, proxy directly to piece-retriever
    if (!x402Metadata) {
      console.log('No x402 metadata found, proxying to piece-retriever')
      return env.PIECE_RETRIEVER.fetch(request)
    }

    console.log('x402 metadata found:', x402Metadata)

    // Build payment requirements from metadata
    const requirements = buildPaymentRequirements(
      payeeAddress,
      x402Metadata,
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

    let decodedPayment
    try {
      decodedPayment = decodePayment(payment, x402Version)
    } catch (err) {
      return buildPaymentRequiredResponse(
        env,
        false,
        requirements,
        'Invalid or malformed payment header',
      )
    }

    console.log('Verifying payment...')
    const verifyResult = await verifyPayment(
      decodedPayment,
      requirements,
      verify,
    )

    if (!verifyResult.isValid) {
      console.log('Payment invalid:', verifyResult.invalidReason)
      return buildPaymentRequiredResponse(
        env,
        false,
        requirements,
        verifyResult.invalidReason,
      )
    }

    console.log('Payment verified, proxying to piece-retriever')

    const response = await env.PIECE_RETRIEVER.fetch(request)

    // Only settle payment on successful responses
    if (response.ok) {
      console.log('Response OK, settling payment...')
      const settleResult = await settlePayment(
        decodedPayment,
        requirements,
        settle,
      )

      if (settleResult.success) {
        console.log('Payment settled:', settleResult.transaction)
        // Create new response with settlement header
        const newResponse = new Response(response.body, response)
        newResponse.headers.set(
          'X-PAYMENT-RESPONSE',
          encodeSettleResponse(settleResult),
        )
        return newResponse
      } else {
        return buildPaymentRequiredResponse(
          env,
          false,
          requirements,
          `Payment settlement failed: ${settleResult.errorReason}`,
        )
      }
    }

    return response
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
