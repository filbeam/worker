import path from 'node:path'
import {
  defineWorkersProject,
  readD1Migrations,
} from '@cloudflare/vitest-pool-workers/config'

export default defineWorkersProject(async () => {
  const migrationsPath = path.join(__dirname, '../db/migrations')
  const migrations = await readD1Migrations(migrationsPath)
  return {
    test: {
      setupFiles: ['./test/apply-migrations.ts'],
      restoreMocks: true,
      poolOptions: {
        workers: {
          singleWorker: true,
          wrangler: {
            configPath: './wrangler.toml',
            environment: 'dev',
          },
          miniflare: {
            bindings: { TEST_MIGRATIONS: migrations },
          },
        },
      },
    },
  }
})
