import { defineWorkspace } from 'vitest/config'

export default defineWorkspace([
  'analytics-writer',
  'indexer',
  'piece-retriever',
  'bad-bits',
  'terminator',
  'workflows',
])
