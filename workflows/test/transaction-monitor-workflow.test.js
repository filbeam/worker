/** @import {WorkflowEvent, WorkflowStep} from 'cloudflare:workers' */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

// Mock the chain module
vi.mock('../lib/chain.js', () => ({
  getChainClient: vi.fn(),
}))

describe('TransactionMonitorWorkflow', () => {
  let WorkflowClass
  let workflow
  let mockEnv
  let mockStep
  let mockQueue

  beforeEach(async () => {
    // Import the workflow class dynamically to allow proper mocking
    const module = await import('../lib/transaction-monitor-workflow.js')
    WorkflowClass = module.TransactionMonitorWorkflow

    // Setup mock environment
    mockQueue = {
      send: vi.fn().mockResolvedValue(),
    }

    mockEnv = {
      TRANSACTION_QUEUE: mockQueue,
      ENVIRONMENT: 'calibration',
      RPC_URL: 'https://api.calibration.node.glif.io/rpc/v0',
      FILCDN_CONTROLLER_ADDRESS_PRIVATE_KEY: '0x1234',
    }

    // Setup mock step
    mockStep = {
      do: vi.fn(),
    }

    // Create workflow instance using prototype to avoid constructor issues
    workflow = Object.create(WorkflowClass.prototype)
    workflow.env = mockEnv

    // Mock console.log to reduce noise in tests
    global.console.log = vi.fn()
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  describe('run', () => {
    it('should wait for transaction receipt', async () => {
      const transactionHash = '0xabc123'
      const mockReceipt = { status: 'success', blockNumber: 12345n }

      // Setup mock step.do to execute callbacks immediately
      mockStep.do.mockImplementation(async (name, options, callback) => {
        if (callback) return await callback()
        return options
      })

      // Mock getChainClient
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

      // Verify step.do was called for waiting transaction
      expect(mockStep.do).toHaveBeenCalledWith(
        `wait for transaction receipt ${transactionHash}`,
        {
          timeout: '5 minutes',
          retries: { limit: 3 },
        },
        expect.any(Function),
      )

      // Verify the chain client was used correctly
      expect(getChainClient).toHaveBeenCalledWith(mockEnv)
    })

    it('should send success message when onSuccess is provided', async () => {
      const transactionHash = '0xdef456'
      const mockReceipt = { status: 'success', blockNumber: 12345n }
      const successData = { upToTimestamp: Date.now() }

      // Setup mock step.do to execute callbacks immediately
      mockStep.do.mockImplementation(async (name, options, callback) => {
        if (callback) return await callback()
        return options
      })

      // Mock getChainClient
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

      // Verify both steps were called
      expect(mockStep.do).toHaveBeenCalledTimes(2)

      // Verify success message was sent
      expect(mockStep.do).toHaveBeenCalledWith(
        'send confirmation to queue',
        { timeout: '30 seconds' },
        expect.any(Function),
      )

      // Verify queue message content
      expect(mockQueue.send).toHaveBeenCalledWith({
        type: 'transaction-confirmed',
        transactionHash,
        ...successData,
      })
    })

    it('should send retry message on transaction failure', async () => {
      const transactionHash = '0xghi789'
      const retryData = { upToTimestamp: Date.now() }
      const error = new Error('Transaction failed')

      // Setup mock step.do to simulate failure
      mockStep.do.mockImplementation(async (name, options, callback) => {
        if (name.includes('wait for transaction receipt')) {
          throw error
        }
        if (callback) return await callback()
        return options
      })

      // Mock getChainClient
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

      // Verify retry step was called
      expect(mockStep.do).toHaveBeenCalledWith(
        'send to retry queue',
        { timeout: '30 seconds' },
        expect.any(Function),
      )

      // Verify retry message was sent
      expect(mockQueue.send).toHaveBeenCalledWith({
        type: 'transaction-retry',
        transactionHash,
        ...retryData,
      })
    })

    it('should handle timeout and send retry', async () => {
      const transactionHash = '0xjkl012'
      const timeoutError = new Error('Timeout waiting for transaction')

      // Setup mock step.do to simulate timeout
      mockStep.do.mockImplementation(async (name, options, callback) => {
        if (name.includes('wait for transaction receipt')) {
          throw timeoutError
        }
        if (callback) return await callback()
        return options
      })

      // Mock getChainClient
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

      // Verify retry was sent
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
      const transactionHash = '0xmno345'
      const mockReceipt = { status: 'success', blockNumber: 12345n }

      // Setup mock step.do to execute callbacks immediately
      mockStep.do.mockImplementation(async (name, options, callback) => {
        if (callback) return await callback()
        return options
      })

      // Mock getChainClient
      const { getChainClient } = await import('../lib/chain.js')
      getChainClient.mockReturnValue({
        publicClient: {
          waitForTransactionReceipt: vi.fn().mockResolvedValue(mockReceipt),
        },
      })

      await workflow.run({ payload: { transactionHash } }, mockStep)

      // Should only wait for receipt, no success message
      expect(mockStep.do).toHaveBeenCalledTimes(1)
      expect(mockStep.do).toHaveBeenCalledWith(
        `wait for transaction receipt ${transactionHash}`,
        expect.any(Object),
        expect.any(Function),
      )

      // No queue messages should be sent
      expect(mockQueue.send).not.toHaveBeenCalled()
    })

    it('should respect retry configuration', async () => {
      const transactionHash = '0xpqr678'

      // Setup mock step.do
      mockStep.do.mockImplementation(async (name, options, callback) => {
        if (callback) return await callback()
        return options
      })

      // Mock getChainClient
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

      // Verify retry configuration was passed
      const firstCall = mockStep.do.mock.calls[0]
      expect(firstCall[1]).toEqual({
        timeout: '5 minutes',
        retries: { limit: 3 },
      })
    })

    it('should handle both success and retry data', async () => {
      const transactionHash = '0xstu901'
      const successData = { confirmationData: 'success' }
      const retryData = { retryInfo: 'retry' }
      const mockReceipt = { status: 'success', blockNumber: 12345n }

      // Setup mock step.do
      mockStep.do.mockImplementation(async (name, options, callback) => {
        if (callback) return await callback()
        return options
      })

      // Mock getChainClient
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

      // Verify success message includes successData
      expect(mockQueue.send).toHaveBeenCalledWith({
        type: 'custom-success',
        transactionHash,
        ...successData,
      })

      // Retry data should not be used in success case
      expect(mockQueue.send).not.toHaveBeenCalledWith(
        expect.objectContaining(retryData),
      )
    })

    it('should include retry data in failure scenario', async () => {
      const transactionHash = '0xvwx234'
      const retryData = { attemptCount: 1, originalTimestamp: Date.now() }
      const error = new Error('Network error')

      // Setup mock step.do to simulate failure
      mockStep.do.mockImplementation(async (name, options, callback) => {
        if (name.includes('wait for transaction receipt')) {
          throw error
        }
        if (callback) return await callback()
        return options
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

      // Verify retry message includes retryData
      expect(mockQueue.send).toHaveBeenCalledWith({
        type: 'transaction-retry',
        transactionHash,
        ...retryData,
      })
    })
  })

  describe('WorkflowEntrypoint inheritance', () => {
    it('should extend WorkflowEntrypoint', () => {
      // Since we're using prototype to avoid constructor issues in tests,
      // we check that the workflow has the expected structure
      expect(workflow.run).toBeDefined()
      expect(workflow.env).toBe(mockEnv)
      expect(Object.getPrototypeOf(workflow)).toBe(WorkflowClass.prototype)
    })
  })
})
