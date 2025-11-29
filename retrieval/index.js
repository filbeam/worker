import { logRetrievalResult, updateDataSetStats } from './lib/stats.js'

export * from './lib/address.js'
export * from './lib/bad-bits-util.js'
export * from './lib/content-security-policy.js'
export * from './lib/http-assert.js'
export * from './lib/stats.js'

export default {
  async fetch() {
    return new Response('Not Implemented', { status: 501 })
  },

  /**
   * @param {TailEvent[]} events
   * @param {Env} env
   */
  async tail(events, env) {
    for (const event of events) {
      const meta = new Map()
      for (const messageData of event.diagnosticsChannelEvents) {
        meta.set(messageData.channel, messageData.message)
      }
      await logRetrievalResult(env, {
        cacheMiss: meta.get('cacheMiss') ?? null,
        responseStatus: event.event.response.status,
        egressBytes: meta.get('egressBytes') ?? null,
        requestCountryCode: meta.get('requestCountryCode'),
        timestamp: event.eventTimestamp,
        dataSetId: meta.get('dataSetId'),
        botName: meta.get('botName'),
        performanceStats: meta.get('performanceStats'),
      })
      if (
        event.eventPhase.response.status < 300 &&
        meta.get('egressBytes') > 0
      ) {
        await updateDataSetStats(env, {
          dataSetId: meta.get('dataSetId'),
          egressBytes: meta.get('egressBytes'),
          cacheMiss: meta.get('cacheMiss'),
          enforceEgressQuota: env.ENFORCE_EGRESS_QUOTA,
        })
      }
    }
  },
}
