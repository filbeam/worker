/**
 * Handle FilBeam UsageReported event
 *
 * @param {{ DB: D1Database }} env
 * @param {any} payload
 * @returns {Promise<Response>}
 */
export async function handleFilBeamUsageReported(env, payload) {
  const dataSetId = String(payload.data_set_id)
  const newEpoch = Number(payload.new_epoch)

  // Check if dataset exists and get current last_rollup_reported_at_epoch
  const result =
    /** @type {{ last_rollup_reported_at_epoch: number | null } | null} */ (
      await env.DB.prepare(
        'SELECT last_rollup_reported_at_epoch FROM data_sets WHERE id = ?',
      )
        .bind(dataSetId)
        .first()
    )

  if (!result) {
    console.warn(
      `FilBeam.UsageReported: Dataset ${dataSetId} not found, skipping update`,
    )
    // Return 200 OK - the event is valid, just the dataset doesn't exist in our DB yet
    return new Response('OK', { status: 200 })
  }

  const lastRollupReportedAtEpoch = result.last_rollup_reported_at_epoch

  // Validate that new epoch is greater than last reported epoch
  if (
    lastRollupReportedAtEpoch !== null &&
    newEpoch <= lastRollupReportedAtEpoch
  ) {
    console.error(
      `FilBeam.UsageReported: Invalid epoch progression for dataset ${dataSetId}. ` +
        `Current epoch: ${lastRollupReportedAtEpoch}, new epoch: ${newEpoch}`,
    )
    return new Response(
      `Bad Request: new_epoch (${newEpoch}) must be greater than last_rollup_reported_at_epoch (${lastRollupReportedAtEpoch})`,
      { status: 400 },
    )
  }

  // Update the last_rollup_reported_at_epoch
  await env.DB.prepare(
    `UPDATE data_sets
     SET last_rollup_reported_at_epoch = ?
     WHERE id = ?
     AND (last_rollup_reported_at_epoch IS NULL OR last_rollup_reported_at_epoch < ?)`,
  )
    .bind(newEpoch, dataSetId, newEpoch)
    .run()

  console.log(
    `FilBeam.UsageReported: Updated dataset ${dataSetId} last_rollup_reported_at_epoch from ${lastRollupReportedAtEpoch} to ${newEpoch}`,
  )

  return new Response('OK', { status: 200 })
}
