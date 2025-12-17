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

    /** @type {{ price: string; is_sanctioned: boolean } | null} */
    const x402Metadata = await env.DB.prepare(
      `SELECT
        MAX(pieces.x402_price) price,
        wallet_details.is_sanctioned
      FROM pieces
      LEFT JOIN data_sets ON pieces.data_set_id = data_sets.id
      LEFT JOIN wallet_details ON data_sets.payer_address = wallet_details.address
      WHERE
        pieces.cid = ? AND
        pieces.is_deleted IS FALSE AND
        data_sets.payer_address = ?`,
    )
      .bind(pieceCid, payeeAddress)
      .first()

    if (!x402Metadata?.price) {
      return env.PIECE_RETRIEVER.fetch(request)
    }

    httpAssert(
      !x402Metadata.is_sanctioned,
      403,
      `Wallet '${payeeAddress}' is sanctioned and cannot retrieve piece_cid '${pieceCid}'.`,
    )

    // Build payment requirements from metadata
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
