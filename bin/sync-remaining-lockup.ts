#!/usr/bin/env node

/*
Sync remaining lockup quotas from on-chain data to D1.

Reads the remaining lockup (lockupFixed) for CDN and cache-miss payment rails
from the FilecoinPay contract, deducts reported-but-unsettled usage from the
FilBeamOperator contract, converts the effective lockup to egress quotas, and
generates SQL that caps D1 quotas to match on-chain values.

The generated SQL uses MIN() to ensure quotas are only decreased, never
increased. This is a one-way sync to prevent over-spending beyond what's
locked up on-chain.

Usage:
  GLIF_TOKEN=<token> node bin/sync-remaining-lockup.ts <calibration|mainnet>

Requires: GLIF_TOKEN environment variable for authenticated Glif RPC access.
*/

import { execSync } from 'node:child_process'
import { readFileSync, writeFileSync } from 'node:fs'
import { createPublicClient, http } from 'viem'
import { filecoin, filecoinCalibration } from 'viem/chains'

const BYTES_PER_TIB = 1024n ** 4n
const ATTO_PER_USDFC = 10n ** 18n
const CDN_RATE_PER_TIB = 7_000_000_000_000_000_000n
const CACHE_MISS_RATE_PER_TIB = 7_000_000_000_000_000_000n

const NETWORK_CONFIG = {
  calibration: {
    chain: filecoinCalibration,
    fwssAddress: '0x02925630df557F957f70E112bA06e50965417CA0' as const,
    operatorAddress: '0x5991E4F9fcEF4AE23959eE03638B4688A7e1EcfF' as const,
    rpcUrl: 'https://api.calibration.node.glif.io/rpc/v1',
    dbName: 'filcdn-calibration-db',
  },
  mainnet: {
    chain: filecoin,
    fwssAddress: '0x8408502033C418E1bbC97cE9ac48E5528F371A9f' as const,
    operatorAddress: '0x9E90749D298C4ca43Bb468CA859Dfe167F9CdCf2' as const,
    rpcUrl: 'https://api.node.glif.io/rpc/v1',
    dbName: 'filcdn-mainnet-db',
  },
}

const fwssAbi = JSON.parse(
  readFileSync(
    new URL(
      '../subgraph/abis/FilecoinWarmStorageService.abi.json',
      import.meta.url,
    ),
    'utf-8',
  ),
)

// StateView and FilecoinPay ABIs are not in the repo, define minimal fragments
const stateViewAbi = [
  {
    type: 'function',
    name: 'getDataSet',
    inputs: [{ name: 'dataSetId', type: 'uint256' }],
    outputs: [
      {
        name: '',
        type: 'tuple',
        components: [
          { name: 'pdpRailId', type: 'uint256' },
          { name: 'cacheMissRailId', type: 'uint256' },
          { name: 'cdnRailId', type: 'uint256' },
          { name: 'payer', type: 'address' },
          { name: 'payee', type: 'address' },
          { name: 'serviceProvider', type: 'address' },
          { name: 'commissionBps', type: 'uint256' },
          { name: 'clientDataSetId', type: 'uint256' },
          { name: 'pdpEndEpoch', type: 'uint256' },
          { name: 'providerId', type: 'uint256' },
          { name: 'dataSetId', type: 'uint256' },
        ],
      },
    ],
    stateMutability: 'view',
  },
] as const

const filecoinPayAbi = [
  {
    type: 'function',
    name: 'getRail',
    inputs: [{ name: 'railId', type: 'uint256' }],
    outputs: [
      {
        name: '',
        type: 'tuple',
        components: [
          { name: 'token', type: 'address' },
          { name: 'from', type: 'address' },
          { name: 'to', type: 'address' },
          { name: 'operator', type: 'address' },
          { name: 'validator', type: 'address' },
          { name: 'paymentRate', type: 'uint256' },
          { name: 'lockupPeriod', type: 'uint256' },
          { name: 'lockupFixed', type: 'uint256' },
          { name: 'settledUpTo', type: 'uint256' },
          { name: 'endEpoch', type: 'uint256' },
          { name: 'commissionRateBps', type: 'uint256' },
          { name: 'serviceFeeRecipient', type: 'address' },
        ],
      },
    ],
    stateMutability: 'view',
  },
] as const

const filBeamOperatorAbi = JSON.parse(
  readFileSync(
    new URL('../subgraph/abis/FilBeamOperator.abi.json', import.meta.url),
    'utf-8',
  ),
)

// Space Meridian / FilBeam Cloudflare account
const CLOUDFLARE_ACCOUNT_ID = '37573110e38849a343d93b727953188f'

// Parse and validate args
const network = process.argv[2]
if (network !== 'calibration' && network !== 'mainnet') {
  console.error(
    'Usage: node bin/sync-remaining-lockup.ts <calibration|mainnet>',
  )
  process.exit(1)
}

const glifToken = process.env.GLIF_TOKEN
if (!glifToken) {
  console.error('Error: GLIF_TOKEN environment variable is required')
  process.exit(1)
}

const config = NETWORK_CONFIG[network]

// Query D1 for datasets with CDN enabled
console.log(`Querying D1 for datasets with CDN enabled on ${network}...`)
const d1Output = execSync(
  `CLOUDFLARE_ACCOUNT_ID=${CLOUDFLARE_ACCOUNT_ID} npx wrangler d1 execute ${config.dbName} --remote --command "SELECT ds.id, q.cdn_egress_quota, q.cache_miss_egress_quota FROM data_sets ds JOIN data_set_egress_quotas q ON ds.id = q.data_set_id WHERE ds.with_cdn = 1" --json`,
  { encoding: 'utf-8' },
)
let d1Result: Array<{
  results: Array<{
    id: string
    cdn_egress_quota: number
    cache_miss_egress_quota: number
  }>
}>
try {
  d1Result = JSON.parse(d1Output)
} catch {
  console.error('Failed to parse wrangler output as JSON:')
  console.error(d1Output)
  process.exit(1)
}
const datasets = d1Result[0].results
console.log(`Found ${datasets.length} dataset(s) with CDN enabled`)

if (datasets.length === 0) {
  console.log('No datasets with CDN enabled. Nothing to do.')
  process.exit(0)
}

// Set up viem client
const transport = http(config.rpcUrl, {
  fetchOptions: {
    headers: { Authorization: `Bearer ${glifToken}` },
  },
})
const publicClient = createPublicClient({ chain: config.chain, transport })

// Discover contract addresses from FWSS
console.log('Discovering contract addresses...')
const [viewContractAddress, paymentsContractAddress] = (await Promise.all([
  publicClient.readContract({
    address: config.fwssAddress,
    abi: fwssAbi,
    functionName: 'viewContractAddress',
  }),
  publicClient.readContract({
    address: config.fwssAddress,
    abi: fwssAbi,
    functionName: 'paymentsContractAddress',
  }),
])) as [`0x${string}`, `0x${string}`]
console.log(`  StateView: ${viewContractAddress}`)
console.log(`  FilecoinPay: ${paymentsContractAddress}`)

// Process each dataset
const results = []
for (const dataset of datasets) {
  const dataSetId = dataset.id
  const d1CdnQuota = BigInt(dataset.cdn_egress_quota)
  const d1CacheMissQuota = BigInt(dataset.cache_miss_egress_quota)
  console.log(`\nProcessing dataset ${dataSetId}...`)

  // Get rail IDs from StateView
  const dataSetInfo = await publicClient.readContract({
    address: viewContractAddress,
    abi: stateViewAbi,
    functionName: 'getDataSet',
    args: [BigInt(dataSetId)],
  })

  const { cdnRailId, cacheMissRailId } = dataSetInfo
  console.log(`  CDN rail: ${cdnRailId}, Cache-miss rail: ${cacheMissRailId}`)

  // Get lockupFixed for both rails and unsettled usage in parallel
  const [cdnRail, cacheMissRail, usage] = await Promise.all([
    publicClient.readContract({
      address: paymentsContractAddress,
      abi: filecoinPayAbi,
      functionName: 'getRail',
      args: [cdnRailId],
    }),
    publicClient.readContract({
      address: paymentsContractAddress,
      abi: filecoinPayAbi,
      functionName: 'getRail',
      args: [cacheMissRailId],
    }),
    publicClient.readContract({
      address: config.operatorAddress,
      abi: filBeamOperatorAbi,
      functionName: 'dataSetUsage',
      args: [BigInt(dataSetId)],
    }) as Promise<readonly [bigint, bigint, bigint]>,
  ])

  const cdnLockupFixed = cdnRail.lockupFixed
  const cacheMissLockupFixed = cacheMissRail.lockupFixed
  const [cdnUnsettled, cacheMissUnsettled] = usage

  // Deduct unsettled usage from lockup before converting to quota
  const effectiveCdnLockup =
    cdnLockupFixed > cdnUnsettled ? cdnLockupFixed - cdnUnsettled : 0n
  const effectiveCacheMissLockup =
    cacheMissLockupFixed > cacheMissUnsettled
      ? cacheMissLockupFixed - cacheMissUnsettled
      : 0n

  const cdnQuota = (effectiveCdnLockup * BYTES_PER_TIB) / CDN_RATE_PER_TIB
  const cacheMissQuota =
    (effectiveCacheMissLockup * BYTES_PER_TIB) / CACHE_MISS_RATE_PER_TIB

  const fmtChange = (onChain: bigint, d1: bigint) => {
    const delta = onChain - d1
    const prefix = delta > 0n ? '+' : ''
    const sign = onChain < d1 ? '↓' : '✓'
    return `${sign} delta: ${prefix}${fmtTiB(delta)}`
  }

  const fmtLine = (
    label: string,
    onChain: bigint,
    d1: bigint,
    lockup: bigint,
    unsettled: bigint,
    effective: bigint,
  ) =>
    `  ${label.padEnd(10)} ${fmtChange(onChain, d1)}, lockup: ${fmtUsdfc(lockup)}, unsettled: ${fmtUsdfc(unsettled)}, effective: ${fmtUsdfc(effective)}, on-chain quota: ${fmtTiB(onChain)}, D1 quota: ${fmtTiB(d1)}`

  console.log(
    fmtLine(
      'CDN',
      cdnQuota,
      d1CdnQuota,
      cdnLockupFixed,
      cdnUnsettled,
      effectiveCdnLockup,
    ),
  )
  console.log(
    fmtLine(
      'Cache-miss',
      cacheMissQuota,
      d1CacheMissQuota,
      cacheMissLockupFixed,
      cacheMissUnsettled,
      effectiveCacheMissLockup,
    ),
  )

  results.push({ dataSetId, cdnQuota, cacheMissQuota })
}

// Generate SQL file
const sqlStatements = results.map(
  ({ dataSetId, cdnQuota, cacheMissQuota }) =>
    `UPDATE data_set_egress_quotas
SET cdn_egress_quota = MIN(cdn_egress_quota, ${cdnQuota}),
    cache_miss_egress_quota = MIN(cache_miss_egress_quota, ${cacheMissQuota})
WHERE data_set_id = '${dataSetId}';`,
)

const sqlFile = `sync-remaining-lockup-${network}.sql`
writeFileSync(sqlFile, sqlStatements.join('\n\n') + '\n')

// Print summary
console.log('\n--- Summary ---')
console.log(
  'Dataset ID'.padEnd(20) +
    'CDN Quota'.padEnd(30) +
    'Cache-miss Quota'.padEnd(30),
)
for (const { dataSetId, cdnQuota, cacheMissQuota } of results) {
  console.log(
    String(dataSetId).padEnd(20) +
      fmtTiB(cdnQuota).padEnd(30) +
      fmtTiB(cacheMissQuota).padEnd(30),
  )
}

console.log(`\nSQL written to: ${sqlFile}`)
console.log('\nTo apply, run:')
console.log(
  `  CLOUDFLARE_ACCOUNT_ID=${CLOUDFLARE_ACCOUNT_ID} npx wrangler d1 execute ${config.dbName} --remote --file ${sqlFile}`,
)

// --- Formatting helpers ---

function fmtUsdfc(attoUsdfc: bigint) {
  const whole = attoUsdfc / ATTO_PER_USDFC
  const frac = ((attoUsdfc % ATTO_PER_USDFC) * 1000n) / ATTO_PER_USDFC
  const sign = attoUsdfc < 0n ? '-' : ''
  const absWhole = whole < 0n ? -whole : whole
  const absFrac = frac < 0n ? -frac : frac
  return `${sign}$${absWhole}.${String(absFrac).padStart(3, '0')}`
}

function fmtTiB(bytes: bigint) {
  const whole = bytes / BYTES_PER_TIB
  const frac = ((bytes % BYTES_PER_TIB) * 1000n) / BYTES_PER_TIB
  const sign = bytes < 0n ? '-' : ''
  const absWhole = whole < 0n ? -whole : whole
  const absFrac = frac < 0n ? -frac : frac
  return `${sign}${absWhole}.${String(absFrac).padStart(3, '0')} TiB`
}
