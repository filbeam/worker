import { fetchAndStoreBadBits } from '../lib/bad-bits.js'

/** @type {ExportedHandler<Env>} */
export default {
  /**
   * @param {ScheduledController} _controller
   * @param {Env} env
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
