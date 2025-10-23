/** @param {Env} env */
export async function terminateCDNServiceForSanctionedWallets(env) {
  /** @type {D1Result<{ id: string }>} */
  const { results: dataSets } = await env.DB.prepare(
    `
      SELECT DISTINCT data_sets.id
      FROM data_sets
        LEFT JOIN wallet_details ON data_sets.payer_address = wallet_details.address
      WHERE data_sets.with_cdn = 1 AND wallet_details.is_sanctioned = 1 AND data_sets.terminate_service_tx_hash IS NULL;
  `,
  ).run()

  console.log(`Found ${dataSets.length} sanctioned data sets to terminate`)

  // Send messages to queue for processing
  const messages = dataSets.map(({ id: dataSetId }) => ({
    body: {
      dataSetId,
      type: 'terminate-cdn-service',
    },
  }))

  if (messages.length > 0) {
    // Queue is utilised here so we can avoid tracking nonces for transactions.
    // By setting the queue concurrency to 1 we're able to send transactions
    // one at a time and hence pick up new nonce every time we send a new transaction.
    await env.TRANSACTION_QUEUE.sendBatch(messages)
    console.log(`Sent ${messages.length} messages to transaction queue`)
  }
}
