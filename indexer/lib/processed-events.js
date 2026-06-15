/**
 * Check if an event has already been processed
 *
 * @param {KVNamespace} kv
 * @param {string} eventType - E.g., 'cdn_top_up', 'service_terminated'
 * @param {string} entityId - Unique entity ID from subgraph
 * @returns {Promise<boolean>}
 */
export async function isEventProcessed(kv, eventType, entityId) {
  const key = `${eventType}:${entityId}`
  const value = await kv.get(key)
  return value !== null
}

/**
 * Mark an event as processed
 *
 * @param {KVNamespace} kv
 * @param {string} eventType
 * @param {string} entityId
 */
export async function markEventProcessed(kv, eventType, entityId) {
  const key = `${eventType}:${entityId}`
  // Store timestamp for debugging purposes
  await kv.put(key, Date.now().toString())
}
