import { toJsonSafe } from 'x402/shared'
import { getPaywallHtml } from 'x402/paywall'
import { exact } from 'x402/schemes'
import { getAddress } from 'viem'
/** @import {PaymentRequirements, PaymentPayload, SettleResponse, VerifyResponse} from 'x402/types' */

/**
 * Build payment requirements from x402 metadata
 *
 * @param {string} payeeAddress - Ethereum address to receive payment
 * @param {string} price - Price and sanctioned status from database
 * @param {Request} request - Original request for resource URL
 * @param {Env} env - Worker environment
 * @returns {PaymentRequirements}
 */
export function buildPaymentRequirements(payeeAddress, price, request, env) {
  const url = new URL(request.url)

  return {
    scheme: 'exact',
    network: env.NETWORK || 'base-sepolia',
    maxAmountRequired: price,
    resource: url.href,
    description: '',
    mimeType: '',
    asset: getAddress(env.TOKEN?.ADDRESS),
    payTo: getAddress(payeeAddress),
    maxTimeoutSeconds: 300,
    extra: {
      name: env.TOKEN?.NAME,
      version: env.TOKEN?.VERSION,
    },
  }
}

/**
 * Decode payment from X-PAYMENT header
 *
 * @param {string} payment - Base64-encoded payment string
 * @param {number} [x402Version=1] - X402 version. Default is `1`
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
 * @param {(
 *   payload: PaymentPayload,
 *   paymentRequirements: PaymentRequirements,
 * ) => Promise<VerifyResponse>} verify
 *   - Function to verify payment
 *
 * @returns {Promise<VerifyResponse>}
 */
export async function verifyPayment(paymentPayload, requirements, verify) {
  try {
    return await verify(paymentPayload, requirements)
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
 * @param {PaymentPayload} paymentPayload - Decoded payment from X-PAYMENT
 *   header
 * @param {PaymentRequirements} requirements - Payment requirements
 * @param {(
 *   payload: PaymentPayload,
 *   paymentRequirements: PaymentRequirements,
 * ) => Promise<SettleResponse>} settle
 *   - Function to settle payment
 *
 * @returns {Promise<SettleResponse>}
 */
export async function settlePayment(paymentPayload, requirements, settle) {
  try {
    return await settle(paymentPayload, requirements)
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
      Math.pow(10, env.TOKEN?.DECIMALS)

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
