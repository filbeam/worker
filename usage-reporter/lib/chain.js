import { createPublicClient, createWalletClient, http } from 'viem'
import { privateKeyToAccount } from 'viem/accounts'
import { filecoinCalibration, filecoin } from 'viem/chains'

/** @param {Env} env */
export function getChainClient(env) {
  const chain = env.ENVIRONMENT === 'mainnet' ? filecoin : filecoinCalibration
  const transport = http(env.RPC_URL)

  const publicClient = createPublicClient({
    chain,
    transport,
  })

  const account = privateKeyToAccount(env.FILBEAM_CONTROLLER_PRIVATE_KEY)

  const walletClient = createWalletClient({
    chain,
    transport,
    account,
  })

  return { publicClient, walletClient, account }
}
