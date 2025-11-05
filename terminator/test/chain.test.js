import { describe, it, expect } from 'vitest'
import { getChainClient } from '../lib/chain.js'

describe('getChainClient', () => {
  it('creates clients for mainnet environment', async () => {
    const env = {
      ENVIRONMENT: 'mainnet',
      RPC_URL: 'https://api.node.glif.io/',
      FILBEAM_CONTRACT_ADDRESS: '0x0000000000000000000000000000000000000000',
      FILBEAM_CONTROLLER_PRIVATE_KEY:
        '0xdead000000000000000000000000000000000000000000000000000000000000',
    }

    const { walletClient, publicClient, account } = getChainClient(env)
    expect(walletClient).toBeDefined()
    expect(publicClient).toBeDefined()
    expect(account).toBeDefined()
    expect(account.address).toBe('0xe1AB69E519d887765cF0bb51D0cFFF2264B38080')
    const chainId = await publicClient.getChainId()
    expect(chainId).toBe(314)
  })

  it('creates clients for calibration environment', async () => {
    const env = {
      ENVIRONMENT: 'calibration',
      RPC_URL: 'https://api.calibration.node.glif.io/',
      FILBEAM_CONTRACT_ADDRESS: '0x0000000000000000000000000000000000000000',
      FILBEAM_CONTROLLER_PRIVATE_KEY:
        '0xdead000000000000000000000000000000000000000000000000000000000000',
    }

    const { walletClient, publicClient, account } = getChainClient(env)
    expect(walletClient).toBeDefined()
    expect(publicClient).toBeDefined()
    expect(account).toBeDefined()
    expect(account.address).toBe('0xe1AB69E519d887765cF0bb51D0cFFF2264B38080')
    const chainId = await publicClient.getChainId()
    expect(chainId).toBe(314159)
  })

  it('creates clients for dev environment (uses calibration chain)', async () => {
    const env = {
      ENVIRONMENT: 'dev',
      RPC_URL: 'https://api.calibration.node.glif.io/',
      FILBEAM_CONTRACT_ADDRESS: '0x0000000000000000000000000000000000000000',
      FILBEAM_CONTROLLER_PRIVATE_KEY:
        '0xdead000000000000000000000000000000000000000000000000000000000000',
    }

    const { walletClient, publicClient, account } = getChainClient(env)
    expect(walletClient).toBeDefined()
    expect(publicClient).toBeDefined()
    expect(account).toBeDefined()
    expect(account.address).toBe('0xe1AB69E519d887765cF0bb51D0cFFF2264B38080')
    const chainId = await publicClient.getChainId()
    expect(chainId).toBe(314159)
  })
})
