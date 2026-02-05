import {
  handleProductAdded,
  handleProductUpdated,
  handleProductRemoved,
  handleProviderRemoved,
} from '../lib/service-provider-registry-handlers.js'
import { checkIfAddressIsSanctioned as defaultCheckIfAddressIsSanctioned } from '../lib/chainalysis.js'
import {
  handleFWSSDataSetCreated,
  handleFWSSServiceTerminated,
  handleFWSSCDNPaymentRailsToppedUp,
} from '../lib/fwss-handlers.js'
import {
  removeDataSetPieces,
  insertDataSetPiece,
} from '../lib/pdp-verifier-handlers.js'
import { handleCdnPaymentSettled } from '../lib/filbeam-operator-handlers.js'
import { screenWallets } from '../lib/wallet-screener.js'
import { CID } from 'multiformats/cid'

export default {
  /**
   * @param {Request} request
   * @param {Env} env
   * @param {ExecutionContext} ctx
   * @param {object} options
   * @param {typeof defaultCheckIfAddressIsSanctioned} [options.checkIfAddressIsSanctioned]
   * @returns {Promise<Response>}
   */
  async fetch(
    request,
    env,
    ctx,
    { checkIfAddressIsSanctioned = defaultCheckIfAddressIsSanctioned } = {},
  ) {
    // TypeScript setup is broken in our monorepo
    // There are multiple global Env interfaces defined (one per worker),
    // TypeScript merges them in a way that breaks our code.
    // We should eventually fix that.
    const { SECRET_HEADER_KEY, SECRET_HEADER_VALUE } = env
    if (request.headers.get(SECRET_HEADER_KEY) !== SECRET_HEADER_VALUE) {
      return new Response('Unauthorized', { status: 401 })
    }

    if (request.method !== 'POST') {
      return new Response('Method Not Allowed', { status: 405 })
    }
    const payload = await request.json()

    console.log('Request body', payload)

    const pathname = new URL(request.url).pathname
    if (pathname === '/fwss/data-set-created') {
      if (
        !(typeof payload.data_set_id === 'string') ||
        !payload.payer ||
        !(typeof payload.provider_id === 'string') ||
        !Array.isArray(payload.metadata_keys) ||
        !Array.isArray(payload.metadata_values)
      ) {
        console.error('FWSS.DataSetCreated: Invalid payload', payload)
        return new Response('Bad Request', { status: 400 })
      }

      console.log(
        `New FWSS data set (data_set_id=${payload.data_set_id}, provider_id=${payload.provider_id}, payer=${payload.payer}, metadata_keys=[${payload.metadata_keys.join(', ')}], metadata_values=[${payload.metadata_values.join(
          ', ',
        )}])`,
      )

      try {
        await handleFWSSDataSetCreated(env, payload, {
          checkIfAddressIsSanctioned,
        })
      } catch (err) {
        console.log(
          `Error handling FWSS data set creation: ${err}. Retrying...`,
        )
        // @ts-ignore
        env.RETRY_QUEUE.send({
          type: 'fwss-data-set-created',
          payload,
        })
      }

      return new Response('OK', { status: 200 })
    } else if (pathname === '/fwss/piece-added') {
      if (
        !(typeof payload.data_set_id === 'string') ||
        !payload.piece_id ||
        !(typeof payload.piece_id === 'string') ||
        !payload.piece_cid ||
        !(typeof payload.piece_cid === 'string') ||
        !Array.isArray(payload.metadata_keys) ||
        !Array.isArray(payload.metadata_values)
      ) {
        console.error('FWSS.PieceAdded: Invalid payload', payload)
        return new Response('Bad Request', { status: 400 })
      }

      /** @type {string} */
      const pieceId = payload.piece_id

      const cidBytes = Buffer.from(payload.piece_cid.slice(2), 'hex')
      const rootCidObj = CID.decode(cidBytes)
      const pieceCid = rootCidObj.toString()

      const ipfsRootCidIndex = payload.metadata_keys.indexOf('ipfsRootCID')
      const ipfsRootCid =
        ipfsRootCidIndex === -1
          ? null
          : payload.metadata_values[ipfsRootCidIndex]

      const x402PriceIndex = payload.metadata_keys.indexOf('x402Price')
      const x402PriceRaw =
        x402PriceIndex === -1 ? null : payload.metadata_values[x402PriceIndex]
      const x402Price =
        x402PriceRaw && /^\d+$/.test(x402PriceRaw) ? x402PriceRaw : null

      console.log(
        `New piece (piece_id=${pieceId}, piece_cid=${pieceCid}, data_set_id=${payload.data_set_id} metadata_keys=[${payload.metadata_keys.join(', ')}], metadata_values=[${payload.metadata_values.join(
          ', ',
        )}])`,
      )

      await insertDataSetPiece(
        env,
        payload.data_set_id,
        pieceId,
        pieceCid,
        ipfsRootCid,
        x402Price,
      )

      return new Response('OK', { status: 200 })
    } else if (pathname === '/pdp-verifier/pieces-removed') {
      if (
        !(typeof payload.data_set_id === 'string') ||
        !payload.piece_ids ||
        !Array.isArray(payload.piece_ids)
      ) {
        console.error('PDPVerifier.PiecesRemoved: Invalid payload', payload)
        return new Response('Bad Request', { status: 400 })
      }

      /** @type {string[]} */
      const pieceIds = payload.piece_ids

      console.log(
        `Removing pieces (piece_ids=[${pieceIds.join(', ')}], data_set_id=${payload.data_set_id})`,
      )

      await removeDataSetPieces(env, payload.data_set_id, pieceIds)
      return new Response('OK', { status: 200 })
    } else if (
      pathname === '/fwss/service-terminated' ||
      pathname === '/fwss/cdn-service-terminated'
    ) {
      if (
        !payload.data_set_id ||
        !(
          typeof payload.data_set_id === 'number' ||
          typeof payload.data_set_id === 'string'
        ) ||
        typeof payload.block_number !== 'number'
      ) {
        console.error(
          'FilecoinWarmStorageService.(ServiceTerminated | CDNServiceTerminated): Invalid payload',
          payload,
        )
        return new Response('Bad Request', { status: 400 })
      }

      console.log(
        `Terminating service for data set (data_set_id=${payload.data_set_id})`,
      )

      await handleFWSSServiceTerminated(env, payload)
      return new Response('OK', { status: 200 })
    } else if (pathname === '/service-provider-registry/product-added') {
      const {
        provider_id: providerId,
        product_type: productType,
        capability_keys: capabilityKeys,
        capability_values: capabilityValues,
        block_number: blockNumber,
      } = payload
      return await handleProductAdded(
        env,
        providerId,
        productType,
        capabilityKeys,
        capabilityValues,
        blockNumber,
      )
    } else if (pathname === '/service-provider-registry/product-updated') {
      const {
        provider_id: providerId,
        product_type: productType,
        capability_keys: capabilityKeys,
        capability_values: capabilityValues,
        block_number: blockNumber,
      } = payload
      return await handleProductUpdated(
        env,
        providerId,
        productType,
        capabilityKeys,
        capabilityValues,
        blockNumber,
      )
    } else if (pathname === '/service-provider-registry/product-removed') {
      const { provider_id: providerId, product_type: productType } = payload
      return await handleProductRemoved(env, providerId, productType)
    } else if (pathname === '/service-provider-registry/provider-removed') {
      const { provider_id: providerId } = payload
      return await handleProviderRemoved(env, providerId)
    } else if (pathname === '/fwss/cdn-payment-rails-topped-up') {
      if (
        typeof payload.id !== 'string' ||
        typeof payload.data_set_id !== 'string' ||
        typeof payload.cdn_amount_added !== 'string' ||
        typeof payload.cache_miss_amount_added !== 'string'
      ) {
        console.error('FWSS.CDNPaymentRailsToppedUp: Invalid payload', payload)
        return new Response('Bad Request', { status: 400 })
      }

      console.log(
        `CDN Payment Rails topped up (data_set_id=${payload.data_set_id}, ` +
          `cdn_amount_added=${payload.cdn_amount_added}, cache_miss_amount_added=${payload.cache_miss_amount_added})`,
      )

      try {
        await handleFWSSCDNPaymentRailsToppedUp(env, payload)
      } catch (err) {
        console.error('Error handling CDN Payment Rails top-up:', err)
        return new Response('Internal Server Error', { status: 500 })
      }

      return new Response('OK', { status: 200 })
    } else if (pathname === '/filbeam-operator/cdn-payment-settled') {
      if (
        typeof payload.data_set_id !== 'string' ||
        typeof payload.block_number !== 'number'
      ) {
        console.error(
          'FilBeamOperator.CdnPaymentSettled: Invalid payload',
          payload,
        )
        return new Response('Bad Request', { status: 400 })
      }

      console.log(
        `CDN payment settled (data_set_id=${payload.data_set_id}, block_number=${payload.block_number})`,
      )

      await handleCdnPaymentSettled(env, payload)
      return new Response('OK', { status: 200 })
    } else {
      return new Response('Not Found', { status: 404 })
    }
  },
  /**
   * Handles incoming messages from the retry queue.
   *
   * @param {MessageBatch<{ type: string; payload: any }>} batch
   * @param {Env} env
   * @param {object} options
   * @param {typeof defaultCheckIfAddressIsSanctioned} [options.checkIfAddressIsSanctioned]
   */
  async queue(
    batch,
    env,
    { checkIfAddressIsSanctioned = defaultCheckIfAddressIsSanctioned } = {},
  ) {
    for (const message of batch.messages) {
      if (message.body.type === 'fwss-data-set-created') {
        try {
          await handleFWSSDataSetCreated(env, message.body.payload, {
            checkIfAddressIsSanctioned,
          })

          message.ack()
        } catch (err) {
          console.log(
            `Error handling FWSS data set creation: ${err}. Retrying...`,
          )
          message.retry({ delaySeconds: 10 })
        }
      } else {
        console.error(`Unknown message type: ${message.body.type}.`)
        message.ack() // Acknowledge unknown messages to avoid reprocessing
      }
    }
  },

  /**
   * @param {any} _controller
   * @param {Env} env
   * @param {ExecutionContext} _ctx
   * @param {object} [options]
   * @param {typeof globalThis.fetch} [options.fetch]
   * @param {typeof defaultCheckIfAddressIsSanctioned} [options.checkIfAddressIsSanctioned]
   */
  async scheduled(
    _controller,
    env,
    _ctx,
    {
      fetch = globalThis.fetch,
      checkIfAddressIsSanctioned = defaultCheckIfAddressIsSanctioned,
    } = {},
  ) {
    const results = await Promise.allSettled([
      this.checkGoldskyStatus(env, { fetch }),
      screenWallets(env, {
        batchSize: Number(env.WALLET_SCREENING_BATCH_SIZE),
        staleThresholdMs: Number(env.WALLET_SCREENING_STALE_THRESHOLD_MS),
        checkIfAddressIsSanctioned,
      }),
      this.reportSettlementStats(env),
    ])
    const errors = results
      .filter((r) => r.status === 'rejected')
      .map((r) => r.reason)
    if (errors.length === 1) {
      throw errors[0]
    } else if (errors.length) {
      throw new AggregateError(errors, 'One or more scheduled tasks failed')
    }
  },

  /**
   * @param {Env} env
   * @param {object} options
   * @param {typeof globalThis.fetch} options.fetch
   */
  async checkGoldskyStatus(env, { fetch }) {
    const query = `
      query {
        _meta {
          hasIndexingErrors
          block {
            number
          }
        }
      }
    `

    /** @type {Response} */
    let res
    try {
      res = await fetch(env.GOLDSKY_SUBGRAPH_URL, {
        method: 'POST',
        body: JSON.stringify({ query }),
      })
    } catch (err) {
      console.warn(
        `Goldsky fetch failed: ${err instanceof Error ? err.message : String(err)}`,
      )
      return
    }

    if (!res.ok) {
      let errorText
      try {
        errorText = await res.text()
      } catch (err) {
        const details = err instanceof Error ? err.stack : String(err)
        errorText = 'Error reading response body: ' + details
      }
      console.warn(`Goldsky returned ${res.status}: ${errorText}`)
      return
    }

    const { data } = await res.json()

    if (typeof data?._meta !== 'object' || data?._meta === null) {
      console.warn(`Unexpected Goldsky response: ${JSON.stringify(data)}`)
      return
    }

    const lastIndexedBlock = data._meta.block?.number
    const hasIndexingErrors = data._meta.hasIndexingErrors

    console.log('Goldsky status', { lastIndexedBlock, hasIndexingErrors })

    env.GOLDSKY_STATS.writeDataPoint({
      doubles: [lastIndexedBlock, hasIndexingErrors ? 1 : 0],
    })
  },

  /** @param {Env} env */
  async reportSettlementStats(env) {
    /**
     * @type {{
     *   id: string
     *   cdn_payments_settled_until: string
     * } | null}
     */
    const row = await env.DB.prepare(
      `
      SELECT id, cdn_payments_settled_until
      FROM data_sets
      WHERE usage_reported_until > cdn_payments_settled_until
      ORDER BY cdn_payments_settled_until ASC
      LIMIT 1
    `,
    ).first()

    if (!row) {
      console.log('No data sets with unsettled CDN usage')
      env.SETTLEMENT_STATS.writeDataPoint({
        doubles: [Date.now(), 0],
        blobs: [''],
      })
      return
    }

    const timestampMs = new Date(row.cdn_payments_settled_until).getTime()
    const lagMs = Date.now() - timestampMs

    console.log(
      `Oldest unsettled CDN usage: data_set=${row.id}, cdn_payments_settled_until=${row.cdn_payments_settled_until}, lag_ms=${lagMs}`,
    )

    env.SETTLEMENT_STATS.writeDataPoint({
      doubles: [timestampMs, lagMs],
      blobs: [row.id],
    })
  },
}
