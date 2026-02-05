import { defineWorkspace } from 'vitest/config'

export default defineWorkspace([
  'indexer',
  'piece-retriever',
  'tail-handler',
  'bad-bits',
  'terminator',
  'workflows',
  'usage-reporter',
  'payment-settler',
  'retrieval',
  'stats-api',
  'x402-piece-gateway',
])
