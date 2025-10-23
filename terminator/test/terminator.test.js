import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  env,
  createExecutionContext,
  waitOnExecutionContext,
} from 'cloudflare:test'
import terminator from '../bin/terminator.js'

describe('Terminator - scheduled entrypoint', () => {
  it('calls terminateCDNServiceForSanctionedWallets with correct env', async () => {
    const mockController = {}
    const ctx = createExecutionContext()

    const mockTerminateCDNServiceForSanctionedWallets = vi
      .fn()
      .mockResolvedValue(undefined)
    await terminator.scheduled(mockController, env, ctx, {
      terminateCDNServiceForSanctionedWallets:
        mockTerminateCDNServiceForSanctionedWallets,
    })
    await waitOnExecutionContext(ctx)

    expect(
      vi.mocked(mockTerminateCDNServiceForSanctionedWallets),
    ).toHaveBeenCalledWith(env)
    expect(
      vi.mocked(mockTerminateCDNServiceForSanctionedWallets),
    ).toHaveBeenCalledTimes(1)
  })
})

describe('Terminator - queue entrypoint', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('processes terminate-cdn-service messages correctly', async () => {
    const ctx = createExecutionContext()
    const mockMessage = {
      body: { type: 'terminate-cdn-service', dataSetId: '123' },
      ack: vi.fn(),
      retry: vi.fn(),
    }

    const mockHandleTerminateCdnServiceQueueMessage = vi
      .fn()
      .mockResolvedValue(undefined)
    const batch = { messages: [mockMessage] }

    // Mock the queue handler
    await terminator.queue(batch, env, ctx, {
      handleTerminateCdnServiceQueueMessage:
        mockHandleTerminateCdnServiceQueueMessage,
    })
    await waitOnExecutionContext(ctx)

    expect(mockHandleTerminateCdnServiceQueueMessage).toHaveBeenCalledWith(
      mockMessage.body,
      env,
    )
    expect(mockMessage.ack).toHaveBeenCalled()
    expect(mockMessage.retry).not.toHaveBeenCalled()
  })

  it('processes transaction-retry messages correctly', async () => {
    const ctx = createExecutionContext()
    const mockMessage = {
      body: { type: 'transaction-retry', transactionHash: '0xabc123' },
      ack: vi.fn(),
      retry: vi.fn(),
    }

    const mockHandleTransactionRetryQueueMessage = vi
      .fn()
      .mockResolvedValue(undefined)
    const batch = { messages: [mockMessage] }

    // Mock the queue handler
    await terminator.queue(batch, env, ctx, {
      handleTransactionRetryQueueMessage:
        mockHandleTransactionRetryQueueMessage,
    })

    await waitOnExecutionContext(ctx)

    expect(mockHandleTransactionRetryQueueMessage).toHaveBeenCalledWith(
      mockMessage.body,
      env,
    )
    expect(mockMessage.ack).toHaveBeenCalled()
    expect(mockMessage.retry).not.toHaveBeenCalled()
  })

  it('handles unknown message types -- single unknown message', async () => {
    const ctx = createExecutionContext()
    const mockMessage = {
      body: { data: 'test' },
      ack: vi.fn(),
      retry: vi.fn(),
    }
    const batch = { messages: [mockMessage] }

    await expect(terminator.queue(batch, env, ctx)).rejects.toThrowError(
      'Unknown message type: {"data":"test"}',
    )
    await waitOnExecutionContext(ctx)
    expect(mockMessage.ack).toHaveBeenCalled()
    expect(mockMessage.retry).not.toHaveBeenCalled()
  })

  it('handles unknown message types -- multiple unknown messages', async () => {
    const ctx = createExecutionContext()
    const mockMessage = {
      body: { type: 'unknown-type', data: 'test' },
      ack: vi.fn(),
      retry: vi.fn(),
    }
    const batch = { messages: [mockMessage, mockMessage] }

    await expect(terminator.queue(batch, env, ctx)).rejects.toThrowError(
      'Unknown message types',
    )
    await waitOnExecutionContext(ctx)
    expect(mockMessage.ack).toHaveBeenCalledTimes(2)
    expect(mockMessage.retry).not.toHaveBeenCalled()
  })

  it('retries messages on handler failure', async () => {
    const ctx = createExecutionContext()
    const error = new Error('Handler failed')
    const mockMessage = {
      body: { type: 'terminate-cdn-service', dataSetId: 456 },
      ack: vi.fn(),
      retry: vi.fn(),
    }
    const batch = { messages: [mockMessage] }
    // Mock the queue handler to throw an error
    const mockHandleTerminateCdnServiceQueueMessage = vi
      .fn()
      .mockRejectedValue(error)

    await terminator.queue(batch, env, ctx, {
      handleTerminateCdnServiceQueueMessage:
        mockHandleTerminateCdnServiceQueueMessage,
    })
    await waitOnExecutionContext(ctx)

    expect(mockHandleTerminateCdnServiceQueueMessage).toHaveBeenCalledWith(
      mockMessage.body,
      env,
    )
    expect(mockMessage.ack).not.toHaveBeenCalled()
    expect(mockMessage.retry).toHaveBeenCalled()
  })

  it('processes multiple messages in batch', async () => {
    const ctx = createExecutionContext()
    const messages = [
      {
        body: { type: 'terminate-cdn-service', dataSetId: 123 },
        ack: vi.fn(),
        retry: vi.fn(),
      },
      {
        body: { type: 'transaction-retry', transactionHash: '0xdef456' },
        ack: vi.fn(),
        retry: vi.fn(),
      },
    ]
    const batch = { messages }

    // Mock the queue handlers
    const mockHandleTerminateCdnServiceQueueMessage = vi
      .fn()
      .mockResolvedValue(undefined)
    const mockHandleTransactionRetryQueueMessage = vi
      .fn()
      .mockResolvedValue(undefined)

    await terminator.queue(batch, env, ctx, {
      handleTerminateCdnServiceQueueMessage:
        mockHandleTerminateCdnServiceQueueMessage,
      handleTransactionRetryQueueMessage:
        mockHandleTransactionRetryQueueMessage,
    })

    await waitOnExecutionContext(ctx)

    expect(mockHandleTerminateCdnServiceQueueMessage).toHaveBeenCalledWith(
      messages[0].body,
      env,
    )
    expect(mockHandleTransactionRetryQueueMessage).toHaveBeenCalledWith(
      messages[1].body,
      env,
    )
    expect(messages[0].ack).toHaveBeenCalled()
    expect(messages[1].ack).toHaveBeenCalled()
  })

  it('continues processing other messages when one fails', async () => {
    const ctx = createExecutionContext()
    const error = new Error('First handler failed')
    const messages = [
      {
        body: { type: 'terminate-cdn-service', dataSetId: 123 },
        ack: vi.fn(),
        retry: vi.fn(),
      },
      {
        body: { type: 'transaction-retry', transactionHash: '0xdef456' },
        ack: vi.fn(),
        retry: vi.fn(),
      },
    ]
    const batch = { messages }

    // Mock the queue handlers - first fails, second succeeds
    const mockHandleTerminateCdnServiceQueueMessage = vi
      .fn()
      .mockRejectedValue(error)
    const mockHandleTransactionRetryQueueMessage = vi
      .fn()
      .mockResolvedValue(undefined)

    await terminator.queue(batch, env, ctx, {
      handleTerminateCdnServiceQueueMessage:
        mockHandleTerminateCdnServiceQueueMessage,
      handleTransactionRetryQueueMessage:
        mockHandleTransactionRetryQueueMessage,
    })

    await waitOnExecutionContext(ctx)

    expect(mockHandleTerminateCdnServiceQueueMessage).toHaveBeenCalledWith(
      messages[0].body,
      env,
    )
    expect(mockHandleTransactionRetryQueueMessage).toHaveBeenCalledWith(
      messages[1].body,
      env,
    )
    expect(messages[0].retry).toHaveBeenCalled()
    expect(messages[1].ack).toHaveBeenCalled()
  })
})
