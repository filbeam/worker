import { defineWorkersProject } from '@cloudflare/vitest-pool-workers/config'

export default defineWorkersProject(async () => {
  // Read all migrations in the `migrations` directory
  return {
    test: {
      setupFiles: [],
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
  }
})
