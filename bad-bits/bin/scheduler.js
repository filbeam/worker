import { fetchAndStoreBadBits } from '../lib/bad-bits.js'

/**
 * @type {ExportedHandler<{
 *   BAD_BITS_KV: KVNamespace
 *   BAD_BITS_R2: R2Bucket
 * }>}
 */
export default {
  /**
   * @param {ScheduledController} _controller
   * @param {{ BAD_BITS_KV: KVNamespace; BAD_BITS_R2: R2Bucket }} env
   * @param {ExecutionContext} _ctx
   */
  async scheduled(_controller, env, _ctx) {
    console.log('Running scheduled bad bits update...')
    try {
      await fetchAndStoreBadBits(env)
      console.log('Updated bad bits denylist')
    } catch (error) {
      console.error('Failed to update bad bits denylist:', error)
      throw error
    }
  },
}
