# CLAUDE.md

Filecoin Beam is the incentivized data delivery layer for Filecoin. FilBeam is implemented as a Cloudflare Workers monorepo that retrieves and caches content from Filecoin PDP (Proof of Data Possession) Service Providers. All traffic coming to and from Filecoin Beam is paid for on-chain.

## Common Commands

```bash
# Install dependencies
npm install

# Run all tests
npm test

# Run tests for a specific worker
npm test -w piece-retriever

# Fix linting and formatting
npm run lint:fix

# Update TypeScript definitions after changing env bindings
npm run build:types

# Start a worker locally
npm start -w piece-retriever
npm start -w indexer

# Reset local database
rm -rf db/.wrangler

# Deploy
npm run deploy:calibration
npm run deploy:mainnet
```

## Architecture

### Monorepo Structure (npm workspaces)

- @./piece-retriever - Main CDN worker handling content retrieval requests
- @./indexer - Processes blockchain events from Goldsky subgraph webhooks and stores in D1
- @./usage-reporter - Scheduled worker reporting egress usage to blockchain
- @./payment-settler - Scheduled worker settling payment rails
- @./terminator - Scheduled worker terminating services for sanctioned clients
- @./bad-bits - Scheduled worker updating content denylist
- @./stats-api - Public statistics API
- @./workflows - Shared Cloudflare Workflows (TransactionMonitorWorkflow)
- @./retrieval - Shared library (address validation, bad-bits, http-assert, stats)
- @./db - Shared D1 database migrations
- @./subgraph - GraphQL subgraph definitions for Goldsky/The Graph

### Data Flow

1. **indexer** receives webhook events from Goldsky subgraph about on-chain events (data set creation, pieces added/removed, service termination, payment rail top-ups). It processes and stores this data in D1.
2. **piece-retriever** handles CDN requests: validates payer wallet, looks up service providers from D1, retrieves content from providers with caching, logs usage stats
3. **usage-reporter**, **payment-settler**, and **terminator** run on schedules to report usage to blockchain, settle payments, and terminate services for sanctioned clients respectively
4. **bad-bits** syncs content denylist to KV storage

### Key Technologies

- **Runtime**: Cloudflare Workers with D1 (SQLite), KV, R2, Queues, Workflows
- **Language**: JavaScript ES modules with JSDoc type annotations
- **Blockchain**: Viem for Filecoin interactions
- **Testing**: Vitest with @cloudflare/vitest-pool-workers

### Environment Configuration

Each worker has a `wrangler.toml` with three environments:

- `dev` - Local development
- `calibration` - Filecoin testnet (staging)
- `mainnet` - Production

### Database

Migrations are in `@./db/migrations/`. Applied automatically during deployment and tests via `wrangler d1 migrations apply`. All workers share the same D1 database.

### Testing

Each worker has its own `vitest.config.js` that:
- Uses `@cloudflare/vitest-pool-workers` for Workers runtime simulation
- Loads D1 migrations from `db/migrations/` via setup files
- Runs with `singleWorker: true` to avoid parallel test isolation issues

Tests are colocated in `test/` directories within each worker.

### Code Style

- ESLint via neostandard (style rules disabled, using Prettier)
- TypeScript for type checking only (no transpilation)
- Auto-generated types via `wrangler types` in `worker-configuration.d.ts`

### Workflow

- Make sure to add or change tests when adding new or changing existing code
- Always run `npm run lint:fix` and `npm test` after adding new code
- Use comments sparingly. Only comment complex code.
- Always follow existing code style
- When testing object values always test them as a whole, not just individual properties
- When testing array values always test them against the full array, not just individual items
