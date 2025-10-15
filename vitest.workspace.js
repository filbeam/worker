import { defineWorkspace } from 'vitest/config'

export default defineWorkspace([
  'indexer',
  'piece-retriever',
  'bad-bits',
  'terminator',
  'payment-settler',
  'usage-reporter',
  'workflows',
])
