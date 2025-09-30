// Helper to seed a wallet
export async function withWallet(env, address, isSanctioned = false) {
  await env.DB.prepare(
    `INSERT INTO wallet_details (address, is_sanctioned, last_screened_at) VALUES (?, ?, datetime('now'))`,
  )
    .bind(address, isSanctioned)
    .run()
}

// Helper to seed a data set
export async function withDataSet(
  env,
  {
    id = '1',
    serviceProviderId = '1',
    payerAddress = '0xPayer',
    withCDN = true,
    terminateServiceTxHash = null,
    lastReportedEpoch = null,
  },
) {
  // Ensure service provider exists
  await env.DB.prepare(
    `INSERT OR IGNORE INTO service_providers (id, service_url) VALUES (?, ?)`,
  )
    .bind(String(serviceProviderId), 'https://example.com')
    .run()

  await env.DB.prepare(
    `INSERT INTO data_sets (id, service_provider_id, payer_address, with_cdn, terminate_service_tx_hash, last_reported_epoch) VALUES (?, ?, ?, ?, ?, ?)`,
  )
    .bind(
      String(id),
      String(serviceProviderId),
      payerAddress,
      withCDN,
      terminateServiceTxHash,
      lastReportedEpoch,
    )
    .run()
}

export const randomId = () => String(Math.ceil(Math.random() * 1e10))
