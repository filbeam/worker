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

  const result = await env.DB.prepare(
    `UPDATE data_sets
     SET last_rollup_reported_at_epoch = ?
     WHERE id = ?
     AND (last_rollup_reported_at_epoch IS NULL OR last_rollup_reported_at_epoch < ?)`,
  )
    .bind(newEpoch, dataSetId, newEpoch)
    .run()

  if (result.meta.changes === 1) {
    console.log(
      `FilBeam.UsageReported: Updated dataset ${dataSetId} last_rollup_reported_at_epoch to ${newEpoch}`,
    )

    return new Response('OK', { status: 200 })
  }

  const dataset = await env.DB.prepare(
    'SELECT last_rollup_reported_at_epoch FROM data_sets WHERE id = ?',
  )
    .bind(dataSetId)
    .first()

  if (!dataset) {
    console.error(`FilBeam.UsageReported: Dataset ${dataSetId} not found`)
    return new Response(`Bad Request: Dataset ${dataSetId} not found`, {
      status: 400,
    })
  }

  console.error(
    `FilBeam.UsageReported: Invalid epoch for dataset ${dataSetId}: ${newEpoch} <= ${dataset.last_rollup_reported_at_epoch}`,
  )
  return new Response(
    `Bad Request: new_epoch must be greater than last_rollup_reported_at_epoch`,
    { status: 400 },
  )
}
