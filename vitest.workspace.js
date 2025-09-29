import { defineWorkspace } from 'vitest/config'

export default defineWorkspace([
  'indexer',
  'ipfs-retriever',
  'piece-retriever',
  'bad-bits',
  'terminator',
])
