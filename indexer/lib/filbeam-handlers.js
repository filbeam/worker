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

  // Check if dataset exists and get current last_reported_epoch
  const result = /** @type {{ last_reported_epoch: number | null } | null} */ (
    await env.DB.prepare(
      'SELECT last_reported_epoch FROM data_sets WHERE id = ?',
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

  const lastReportedEpoch = result.last_reported_epoch

  // Validate that new epoch is greater than last reported epoch
  if (lastReportedEpoch !== null && newEpoch <= lastReportedEpoch) {
    console.error(
      `FilBeam.UsageReported: Invalid epoch progression for dataset ${dataSetId}. ` +
        `Current epoch: ${lastReportedEpoch}, new epoch: ${newEpoch}`,
    )
    return new Response(
      `Bad Request: new_epoch (${newEpoch}) must be greater than last_reported_epoch (${lastReportedEpoch})`,
      { status: 400 },
    )
  }

  // Update the last_reported_epoch
  await env.DB.prepare(
    `UPDATE data_sets
     SET last_reported_epoch = ?
     WHERE id = ?
     AND (last_reported_epoch IS NULL OR last_reported_epoch < ?)`,
  )
    .bind(newEpoch, dataSetId, newEpoch)
    .run()

  console.log(
    `FilBeam.UsageReported: Updated dataset ${dataSetId} last_reported_epoch from ${lastReportedEpoch} to ${newEpoch}`,
  )

  return new Response('OK', { status: 200 })
}
