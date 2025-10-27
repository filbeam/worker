import { createPublicClient, http } from 'viem'
import { filecoinCalibration, filecoin } from 'viem/chains'

/**
 * Get chain client for interacting with the Filecoin network
 *
 * @param {{
 *   ENVIRONMENT: 'dev' | 'calibration' | 'mainnet'
 *   RPC_URL: string
 * }} env
 */
export function getChainClient(env) {
  const chain = env.ENVIRONMENT === 'mainnet' ? filecoin : filecoinCalibration
  const transport = http(env.RPC_URL)
  const publicClient = createPublicClient({
    chain,
    transport,
  })
  return publicClient
}
