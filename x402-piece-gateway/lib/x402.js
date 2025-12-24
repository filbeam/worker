import { toJsonSafe } from 'x402/shared'
import { getPaywallHtml } from 'x402/paywall'
import { getAddress } from 'viem'

/**
 * Build payment requirements from x402 metadata
 *
 * @param {string} payeeAddress - Ethereum address to receive payment
 * @param {string} price - Price and sanctioned status from database
 * @param {Request} request - Original request for resource URL
 * @param {Env} env - Worker environment
 * @returns {import('x402/types').PaymentRequirements}
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
 * Build HTTP 402 Payment Required response
 *
 * @param {Env} env
 * @param {boolean} isWebBrowser
 * @param {import('x402/types').PaymentRequirements} requirements
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
