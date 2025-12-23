/** @import {WorkflowStep} from 'cloudflare:workers' */
import { env } from 'cloudflare:test'
import { describe, it, expect, vi, afterEach } from 'vitest'
import { TransactionMonitorWorkflow } from '../lib/transaction-monitor-workflow.js'

vi.mock('../lib/chain.js', () => ({
  getChainClient: vi.fn(),
}))

describe('TransactionMonitorWorkflow', () => {
  /** @type {WorkflowStep} */
  const mockStep = { do: vi.fn() }
  /** @type {{ send: (message: unknown) => Promise<void> }} */
  const mockQueue = { send: vi.fn().mockResolvedValue() }
  /**
   * @type {{
   *   TRANSACTION_QUEUE: { send: (message: any) => Promise<void> }
   *   ENVIRONMENT: string
   *   RPC_URL: string
   *   FILCDN_CONTROLLER_ADDRESS_PRIVATE_KEY: string
   * }}
   */
  const mockEnv = {
    ...env,
    TRANSACTION_QUEUE: mockQueue,
    FILCDN_CONTROLLER_ADDRESS_PRIVATE_KEY: '0x1234',
  }

  afterEach(() => {
    vi.clearAllMocks()
  })

  describe('run', () => {
    it('should wait for transaction receipt', async () => {
      const workflow = Object.create(TransactionMonitorWorkflow.prototype)
      workflow.env = mockEnv

      const transactionHash = '0xabc123'
      const mockReceipt = { status: 'success', blockNumber: 12345n }

      mockStep.do.mockImplementation(async (name, options, callback) => {
        if (callback) return await callback()
        return options
      })

      const { getChainClient } = await import('../lib/chain.js')
      getChainClient.mockReturnValue({
        publicClient: {
          waitForTransactionReceipt: vi.fn().mockResolvedValue(mockReceipt),
        },
      })

      await workflow.run(
        { payload: { transactionHash, metadata: {} } },
        mockStep,
      )

      expect(mockStep.do).toHaveBeenCalledWith(
        `wait for transaction receipt ${transactionHash}`,
        {
          timeout: '10 minutes',
          retries: { delay: '10 seconds', limit: 5, backoff: 'exponential' },
        },
        expect.any(Function),
      )

      expect(getChainClient).toHaveBeenCalledWith(mockEnv)
    })

    it('should send success message when onSuccess is provided', async () => {
      const workflow = Object.create(TransactionMonitorWorkflow.prototype)
      workflow.env = mockEnv

      const transactionHash = '0xdef456'
      const mockReceipt = { status: 'success', blockNumber: 12345n }
      const successData = { upToTimestamp: Date.now() }

      mockStep.do.mockImplementation(async (name, options, callback) => {
        if (callback) return await callback()
        return options
      })

      const { getChainClient } = await import('../lib/chain.js')
      getChainClient.mockReturnValue({
        publicClient: {
          waitForTransactionReceipt: vi.fn().mockResolvedValue(mockReceipt),
        },
      })

      await workflow.run(
        {
          payload: {
            transactionHash,
            metadata: {
              onSuccess: 'transaction-confirmed',
              successData,
            },
          },
        },
        mockStep,
      )

      expect(mockStep.do).toHaveBeenCalledTimes(2)

      expect(mockStep.do).toHaveBeenCalledWith(
        'send confirmation to queue',
        { timeout: '30 seconds' },
        expect.any(Function),
      )

      expect(mockQueue.send).toHaveBeenCalledWith({
        type: 'transaction-confirmed',
        transactionHash,
        ...successData,
      })
    })

    it('should send retry message on transaction failure', async () => {
      const workflow = Object.create(TransactionMonitorWorkflow.prototype)
      workflow.env = mockEnv

      const transactionHash = '0xghi789'
      const retryData = { upToTimestamp: Date.now() }
      const error = new Error('Transaction failed')

      mockStep.do.mockImplementation(async (name, options, callback) => {
        if (callback) return await callback()
        return options
      })

      const { getChainClient } = await import('../lib/chain.js')
      getChainClient.mockReturnValue({
        publicClient: {
          waitForTransactionReceipt: vi.fn().mockRejectedValue(error),
        },
      })

      await workflow.run(
        {
          payload: {
            transactionHash,
            metadata: {
              retryData,
            },
          },
        },
        mockStep,
      )

      expect(mockStep.do).toHaveBeenCalledWith(
        'send to retry queue',
        { timeout: '30 seconds' },
        expect.any(Function),
      )

      expect(mockQueue.send).toHaveBeenCalledWith({
        type: 'transaction-retry',
        transactionHash,
        ...retryData,
      })
    })

    it('should handle timeout and send retry', async () => {
      const workflow = Object.create(TransactionMonitorWorkflow.prototype)
      workflow.env = mockEnv

      const transactionHash = '0xjkl012'
      const timeoutError = new Error('Timeout waiting for transaction')

      mockStep.do.mockImplementation(async (name, options, callback) => {
        if (callback) return await callback()
        return options
      })

      const { getChainClient } = await import('../lib/chain.js')
      getChainClient.mockReturnValue({
        publicClient: {
          waitForTransactionReceipt: vi.fn().mockRejectedValue(timeoutError),
        },
      })

      await workflow.run(
        {
          payload: { transactionHash, metadata: {} },
        },
        mockStep,
      )

      expect(mockStep.do).toHaveBeenCalledWith(
        'send to retry queue',
        { timeout: '30 seconds' },
        expect.any(Function),
      )

      expect(mockQueue.send).toHaveBeenCalledWith({
        type: 'transaction-retry',
        transactionHash,
      })
    })

    it('should handle minimal payload without metadata', async () => {
      const workflow = Object.create(TransactionMonitorWorkflow.prototype)
      workflow.env = mockEnv

      const transactionHash = '0xmno345'
      const mockReceipt = { status: 'success', blockNumber: 12345n }

      mockStep.do.mockImplementation(async (name, options, callback) => {
        if (callback) return await callback()
        return options
      })

      const { getChainClient } = await import('../lib/chain.js')
      getChainClient.mockReturnValue({
        publicClient: {
          waitForTransactionReceipt: vi.fn().mockResolvedValue(mockReceipt),
        },
      })

      await workflow.run({ payload: { transactionHash } }, mockStep)

      expect(mockStep.do).toHaveBeenCalledTimes(1)
      expect(mockStep.do).toHaveBeenCalledWith(
        `wait for transaction receipt ${transactionHash}`,
        expect.any(Object),
        expect.any(Function),
      )

      expect(mockQueue.send).not.toHaveBeenCalled()
    })

    it('should respect retry configuration', async () => {
      const workflow = Object.create(TransactionMonitorWorkflow.prototype)
      workflow.env = mockEnv

      const transactionHash = '0xpqr678'

      mockStep.do.mockImplementation(async (name, options, callback) => {
        if (callback) return await callback()
        return options
      })

      const { getChainClient } = await import('../lib/chain.js')
      getChainClient.mockReturnValue({
        publicClient: {
          waitForTransactionReceipt: vi.fn().mockResolvedValue({}),
        },
      })

      await workflow.run(
        { payload: { transactionHash, metadata: {} } },
        mockStep,
      )

      const firstCall = mockStep.do.mock.calls[0]
      expect(firstCall[1]).toEqual({
        timeout: '10 minutes',
        retries: { delay: '10 seconds', limit: 5, backoff: 'exponential' },
      })
    })

    it('should handle both success and retry data', async () => {
      const workflow = Object.create(TransactionMonitorWorkflow.prototype)
      workflow.env = mockEnv

      const transactionHash = '0xstu901'
      const successData = { confirmationData: 'success' }
      const retryData = { retryInfo: 'retry' }
      const mockReceipt = { status: 'success', blockNumber: 12345n }

      mockStep.do.mockImplementation(async (name, options, callback) => {
        if (callback) return await callback()
        return options
      })

      const { getChainClient } = await import('../lib/chain.js')
      getChainClient.mockReturnValue({
        publicClient: {
          waitForTransactionReceipt: vi.fn().mockResolvedValue(mockReceipt),
        },
      })

      await workflow.run(
        {
          payload: {
            transactionHash,
            metadata: {
              onSuccess: 'custom-success',
              successData,
              retryData,
            },
          },
        },
        mockStep,
      )

      expect(mockQueue.send).toHaveBeenCalledWith({
        type: 'custom-success',
        transactionHash,
        ...successData,
      })

      expect(mockQueue.send).not.toHaveBeenCalledWith(
        expect.objectContaining(retryData),
      )
    })

    it('should include retry data in failure scenario', async () => {
      const workflow = Object.create(TransactionMonitorWorkflow.prototype)
      workflow.env = mockEnv

      const transactionHash = '0xvwx234'
      const retryData = { attemptCount: 1, originalTimestamp: Date.now() }
      const error = new Error('Network error')

      mockStep.do.mockImplementation(async (name, options, callback) => {
        if (callback) return await callback()
        return options
      })

      const { getChainClient } = await import('../lib/chain.js')
      getChainClient.mockReturnValue({
        publicClient: {
          waitForTransactionReceipt: vi.fn().mockRejectedValue(error),
        },
      })

      await workflow.run(
        {
          payload: {
            transactionHash,
            metadata: {
              retryData,
            },
          },
        },
        mockStep,
      )

      expect(mockQueue.send).toHaveBeenCalledWith({
        type: 'transaction-retry',
        transactionHash,
        ...retryData,
      })
    })
  })

  describe('WorkflowEntrypoint inheritance', () => {
    it('should extend WorkflowEntrypoint', () => {
      const workflow = Object.create(TransactionMonitorWorkflow.prototype)
      workflow.env = mockEnv

      expect(workflow.run).toBeDefined()
      expect(workflow.env).toBe(mockEnv)
      expect(Object.getPrototypeOf(workflow)).toBe(
        TransactionMonitorWorkflow.prototype,
      )
    })
  })
})
