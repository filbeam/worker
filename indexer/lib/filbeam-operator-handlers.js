/**
 * Handle FilBeamOperator UsageReported event
 *
 * @param {{ DB: D1Database }} env
 * @param {any} payload
 * @returns {Promise<Response>}
 */
export async function handleFilBeamOperatorUsageReported(env, payload) {
  const dataSetId = String(payload.data_set_id)
  const epoch = Number(payload.epoch)

  const result = await env.DB.prepare(
    `UPDATE data_sets
     SET last_rollup_reported_at_epoch = ?
     WHERE id = ?
     AND (last_rollup_reported_at_epoch IS NULL OR last_rollup_reported_at_epoch < ?)`,
  )
    .bind(epoch, dataSetId, epoch)
    .run()

  if (result.meta.changes === 1) {
    console.log(
      `FilBeam.UsageReported: Updated data set ${dataSetId} last_rollup_reported_at_epoch to ${epoch}`,
    )

    return new Response('OK', { status: 200 })
  }

  const dataSet = await env.DB.prepare(
    'SELECT last_rollup_reported_at_epoch FROM data_sets WHERE id = ?',
  )
    .bind(dataSetId)
    .first()

  if (!dataSet) {
    console.error(`FilBeam.UsageReported: Data set ${dataSetId} not found`)
    return new Response(`Bad Request: Data set ${dataSetId} not found`, {
      status: 400,
    })
  }

  console.error(
    `FilBeam.UsageReported: Invalid epoch for data set ${dataSetId}: ${epoch} <= ${dataSet.last_rollup_reported_at_epoch}`,
  )
  return new Response(
    `Bad Request: epoch must be greater than last_rollup_reported_at_epoch`,
    { status: 400 },
  )
}
