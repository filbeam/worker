import neostandard from 'neostandard'

export default neostandard({
  noStyle: true, // Disable style-related rules, we use Prettier
  ts: true,
  ignores: ['**/dist', '**/worker-configuration.d.ts', 'subgraph/generated'],
  globals: ['caches'],
})
