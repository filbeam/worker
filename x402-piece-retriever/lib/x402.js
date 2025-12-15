import { toJsonSafe } from 'x402/shared'
import { getPaywallHtml } from 'x402/paywall'
import { useFacilitator } from 'x402/verify'

/**
 * @typedef {Object} X402Metadata
 * @property {string} price - Price in smallest token units
 * @property {string} block - Block number when price was set
 */

/**
 * @typedef {Object} PaymentRequirement
 * @property {string} scheme
 * @property {string} network
 * @property {string} maxAmountRequired
 * @property {string} resource
 * @property {Object} asset
 * @property {string} asset.address
 * @property {number} asset.decimals
 * @property {string} asset.eip712
 * @property {string} payTo
 * @property {number} maxTimeoutSeconds
 * @property {Object} extra
 */

/**
 * @typedef {Object} VerifyResponse
 * @property {boolean} isValid
 * @property {string} [invalidReason]
 * @property {string} [payer]
 */

/**
 * @typedef {Object} SettleResponse
 * @property {boolean} success
 * @property {string} [transaction]
 * @property {string} [network]
 * @property {string} [errorReason]
 */

// USDC on base-sepolia
const USDC_ADDRESS = '0x036CbD53842c5426634e7929541eC2318f3dCF7e'
const USDC_DECIMALS = 6

// EIP-712 domain for USDC permit
const USDC_EIP712 = JSON.stringify({
  name: 'USD Coin',
  version: '2',
})

/**
 * Build payment requirements from x402 metadata
 *
 * @param {string} payeeAddress - Ethereum address to receive payment
 * @param {X402Metadata} metadata - Price and block info from KV
 * @param {Request} request - Original request for resource URL
 * @param {Env} env - Worker environment
 * @returns {PaymentRequirement}
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
    asset: {
      address: env.USDC_ADDRESS || USDC_ADDRESS,
      decimals: USDC_DECIMALS,
      eip712: USDC_EIP712,
    },
    payTo: payeeAddress,
    maxTimeoutSeconds: 60, // default timeout
    extra: {},
  }
}

/**
 * Verify a payment with the facilitator
 *
 * @param {object} paymentPayload - Decoded payment from X-PAYMENT header
 * @param {PaymentRequirement} requirements - Payment requirements
 * @param {Env} env - Worker environment
 * @returns {Promise<VerifyResponse>}
 */
export async function verifyPayment(paymentPayload, requirements, env) {
  const facilitator = useFacilitator({ url: env.FACILITATOR_URL })

  try {
    const response = await facilitator.verify(paymentPayload, [requirements])
    return response
  } catch (error) {
    console.error('Payment verification error:', error)
    return {
      isValid: false,
      invalidReason:
        error instanceof Error ? error.message : 'Payment verification failed',
    }
  }
}

/**
 * Settle a verified payment with the facilitator
 *
 * @param {object} paymentPayload - Decoded payment from X-PAYMENT header
 * @param {PaymentRequirement} requirements - Payment requirements
 * @param {Env} env - Worker environment
 * @returns {Promise<SettleResponse>}
 */
export async function settlePayment(paymentPayload, requirements, env) {
  const facilitator = useFacilitator({ url: env.FACILITATOR_URL })

  try {
    const response = await facilitator.settle(paymentPayload, [requirements])
    return response
  } catch (error) {
    console.error('Payment settlement error:', error)
    return {
      success: false,
      errorReason:
        error instanceof Error ? error.message : 'Payment settlement failed',
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
 * @param {boolean} isWebBrowser
 * @param {PaymentRequirement} requirements
 * @param {string} [errorMessage]
 * @returns {Response}
 */
export function buildPaymentRequiredResponse(
  isWebBrowser,
  requirements,
  errorMessage,
) {
  const responseBody = {
    x402Version: 1,
    error: errorMessage || 'Payment Required',
    accepts: [requirements],
  }

  // For browser requests, return HTML paywall
  if (isWebBrowser) {
    // Calculate display amount in USD (convert from smallest units)
    const decimals = requirements.asset?.decimals || 6
    const displayAmount =
      parseInt(requirements.maxAmountRequired) / Math.pow(10, decimals)

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
