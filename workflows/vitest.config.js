import { defineWorkersProject } from '@cloudflare/vitest-pool-workers/config'

export default defineWorkersProject(async () => {
  return {
    test: {
      poolOptions: {
        workers: {
          wrangler: {
            configPath: './wrangler.toml',
            environment: 'dev',
          },
        },
      },
    },
  }
})
