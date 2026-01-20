import { defineWorkersProject } from '@cloudflare/vitest-pool-workers/config'

export default defineWorkersProject({
  test: {
    restoreMocks: true,
    poolOptions: {
      workers: {
        singleWorker: true,
        wrangler: {
          configPath: './wrangler.toml',
          environment: 'dev',
        },
      },
    },
  },
})
