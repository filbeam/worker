import { defineWorkersProject } from '@cloudflare/vitest-pool-workers/config'

export default defineWorkersProject({
  test: {
    poolOptions: {
      workers: {
        singleWorker: true,
        wrangler: {
          configPath: './wrangler.toml',
          environment: 'dev',
        },
        miniflare: {
          // Mock the PIECE_RETRIEVER service binding for tests
          serviceBindings: {
            PIECE_RETRIEVER: () => {
              return new Response('mock piece content', {
                status: 200,
                headers: { 'Content-Type': 'application/octet-stream' },
              })
            },
          },
        },
      },
    },
  },
})
