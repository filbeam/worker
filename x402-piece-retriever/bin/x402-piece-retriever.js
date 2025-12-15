import { httpAssert } from '@filbeam/retrieval'
import { parseRequest } from '../lib/request.js'
// import { useFacilitator } from 'x402/verify'
import {
  buildPaymentRequirements,
  // extractPaymentFromRequest,
  verifyPayment,
  settlePayment,
  encodeSettleResponse,
  buildPaymentRequiredResponse,
} from '../lib/x402.js'
import { exact } from 'x402/schemes'

const x402Version = 1

export default {
  /**
   * @param {Request} request
   * @param {Env} env
   * @param {ExecutionContext} ctx
   * @returns {Promise<Response>}
   */
  async fetch(request, env, ctx) {
    try {
      return await this._fetch(request, env, ctx)
    } catch (error) {
      return this._handleError(error)
    }
  },

  /**
   * @param {Request} request
   * @param {Env} env
   * @param {ExecutionContext} ctx
   * @returns {Promise<Response>}
   */
  async _fetch(request, env, ctx) {
    // Only support GET and HEAD methods
    httpAssert(
      ['GET', 'HEAD'].includes(request.method),
      405,
      'Method Not Allowed',
    )
    // const { verify, settle } = useFacilitator({ url: env.FACILITATOR_URL })

    // Parse request to extract payee address and piece CID
    const { payeeAddress, pieceCid, payment, isWebBrowser } = parseRequest(
      request,
      env,
    )

    // Lookup x402 metadata in KV store
    const x402Metadata =
      /** @type {{ price: string; block: string } | null} */
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
        isWebBrowser,
        requirements,
        'Missing X-PAYMENT header',
      )
    }

    // Extract payment from request headers
    // const payment = extractPaymentFromRequest(request)

    // Verify payment
    /** @type {import('x402/types').PaymentPayload} */
    let decodedPayment
    try {
      decodedPayment = exact.evm.decodePayment(payment)
      decodedPayment.x402Version = x402Version
    } catch (err) {
      const response = {
        error:
          err instanceof Error
            ? err.message
            : 'Invalid or malformed payment header',
        accepts: requirements,
        x402Version,
      }
      return new Response(JSON.stringify(response), {
        status: 402,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    // Verify payment with facilitator
    console.log('Verifying payment...')
    const verifyResult = await verifyPayment(decodedPayment, requirements, env)

    if (!verifyResult.isValid) {
      console.log('Payment invalid:', verifyResult.invalidReason)
      return buildPaymentRequiredResponse(
        isWebBrowser,
        requirements,
        verifyResult.invalidReason,
      )
    }

    console.log('Payment verified, proxying to piece-retriever')

    // Payment verified - proxy to piece-retriever
    const response = await env.PIECE_RETRIEVER.fetch(request)

    // Only settle payment on successful responses
    if (response.ok) {
      console.log('Response OK, settling payment...')
      const settleResult = await settlePayment(
        decodedPayment,
        requirements,
        env,
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
        console.error('Settlement failed:', settleResult.errorReason)
        // Still return the response even if settlement failed
        // The payment was verified, so the user should get the content
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
