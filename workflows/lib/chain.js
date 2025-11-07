import { createPublicClient, createWalletClient, http } from 'viem'
import { privateKeyToAccount } from 'viem/accounts'
import { filecoinCalibration, filecoin } from 'viem/chains'

/**
 * @param {{
 *   ENVIRONMENT: 'mainnet' | 'calibration' | 'dev'
 *   RPC_URL: string
 *   FILBEAM_CONTROLLER_PRIVATE_KEY?: string
 *   FILBEAM_OPERATOR_PAYMENT_SETTLER_PRIVATE_KEY?: string
 * }} env
 */
export function getChainClient(env) {
  const chain = env.ENVIRONMENT === 'mainnet' ? filecoin : filecoinCalibration
  const transport = http(env.RPC_URL)

  const publicClient = createPublicClient({
    chain,
    transport,
  })

  // Support both payment-settler, terminator and usage-reporter private keys
  const privateKey = /** @type {`0x${string}`} */ (
    env.FILBEAM_CONTROLLER_PRIVATE_KEY ||
      env.FILBEAM_OPERATOR_PAYMENT_SETTLER_PRIVATE_KEY
  )

  const account = privateKeyToAccount(privateKey)

  const walletClient = createWalletClient({
    chain,
    transport,
    account,
  })

  return { publicClient, walletClient, account }
}
