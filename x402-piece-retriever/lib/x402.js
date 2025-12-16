import { toJsonSafe } from 'x402/shared'
import { getPaywallHtml } from 'x402/paywall'
import { exact } from 'x402/schemes'
/** @import {PaymentRequirements, PaymentPayload, SettleResponse, VerifyResponse} from 'x402/types' */

/**
 * @typedef {Object} X402Metadata
 * @property {string} price - Price in smallest token units
 * @property {string} blockNumber - Block number when price was set
 */

/**
 * Build payment requirements from x402 metadata
 *
 * @param {string} payeeAddress - Ethereum address to receive payment
 * @param {X402Metadata} metadata - Price and block info from KV
 * @param {Request} request - Original request for resource URL
 * @param {Env} env - Worker environment
 * @returns {PaymentRequirements}
 */
export function buildPaymentRequirements(payeeAddress, metadata, request, env) {
  const url = new URL(request.url)

  return {
    scheme: 'exact',
    network: env.NETWORK || 'base-sepolia',
    maxAmountRequired: metadata.price,
    resource: url.href,
    description: '',
    mimeType: '',
    asset: env.TOKEN_ADDRESS,
    payTo: payeeAddress,
    maxTimeoutSeconds: 60, // default timeout
    extra: {},
  }
}

/**
 * Decode payment from X-PAYMENT header
 *
 * @param {string} payment - Base64-encoded payment string
 * @param {number} [x402Version=1] - x402 version
 * @returns {PaymentPayload}
 */
export function decodePayment(payment, x402Version = 1) {
  const decoded = exact.evm.decodePayment(payment)
  decoded.x402Version = x402Version
  return decoded
}

/**
 * Verify a payment with the facilitator
 *
 * @param {PaymentPayload} paymentPayload - Decoded payment from X-PAYMENT
 *   header
 * @param {PaymentRequirements} requirements - Payment requirements
 * @param {Function} verify - Function to verify payment
 * @returns {Promise<VerifyResponse>}
 */
export async function verifyPayment(paymentPayload, requirements, verify) {
  try {
    return await verify(paymentPayload, [requirements])
  } catch (error) {
    console.error('Payment verification error:', error)
    return {
      isValid: false,
      invalidReason: 'unexpected_verify_error',
    }
  }
}

/**
 * Settle a verified payment with the facilitator
 *
 * @param {object} paymentPayload - Decoded payment from X-PAYMENT header
 * @param {PaymentRequirements} requirements - Payment requirements
 * @param {Function} settle - Function to settle payment
 * @returns {Promise<SettleResponse>}
 */
export async function settlePayment(paymentPayload, requirements, settle) {
  try {
    return await settle(paymentPayload, [requirements])
  } catch (error) {
    console.error('Payment settlement error:', error)
    return {
      transaction: '',
      network: requirements.network,
      success: false,
      errorReason: 'unexpected_settle_error',
    }
  }
}

/**
 * Encode settlement response for X-PAYMENT-RESPONSE header
 *
 * @param {SettleResponse} settleResult
 * @returns {string}
 */
export function encodeSettleResponse(settleResult) {
  const payload = toJsonSafe(settleResult)
  return btoa(JSON.stringify(payload))
}

/**
 * Build HTTP 402 Payment Required response
 *
 * @param {Env} env
 * @param {boolean} isWebBrowser
 * @param {PaymentRequirements} requirements
 * @param {string} [errorMessage]
 * @returns {Response}
 */
export function buildPaymentRequiredResponse(
  env,
  isWebBrowser,
  requirements,
  errorMessage,
  x402Version = 1,
) {
  const responseBody = {
    x402Version,
    error: errorMessage || 'Payment Required',
    accepts: [requirements],
  }

  // For browser requests, return HTML paywall
  if (isWebBrowser) {
    const displayAmount =
      parseInt(requirements.maxAmountRequired) /
      Math.pow(10, env.TOKEN_DECIMALS)

    const html = getPaywallHtml({
      amount: displayAmount,
      paymentRequirements: /** @type {any} */ (toJsonSafe([requirements])),
      currentUrl: requirements.resource,
      testnet: requirements.network === 'base-sepolia',
    })
    return new Response(html, {
      status: 402,
      headers: {
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-store',
      },
    })
  }

  // For API clients, return JSON
  return new Response(JSON.stringify(toJsonSafe(responseBody)), {
    status: 402,
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'no-store',
    },
  })
}
