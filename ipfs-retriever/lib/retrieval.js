import { CarBlockIterator } from '@ipld/car'
// @ts-ignore - Types exist but package.json exports configuration prevents resolution
import * as carBlockValidator from '@web3-storage/car-block-validator'
import { recursive as exporter } from 'ipfs-unixfs-exporter'
import { httpAssert, originCacheOptions } from '@filbeam/retrieval'

/** @import {UnixFSBasicEntry} from 'ipfs-unixfs-exporter' */
/** @typedef {{ cid: import('multiformats').CID; bytes: Uint8Array }} Block */

/** @type {(block: Block) => Promise<void> | undefined} */
const validateBlock = carBlockValidator.validateBlock

/**
 * Retrieves the IPFS content from the SP serving requests at the provided base
 * URL.
 *
 * @param {string} baseUrl - The base URL of service provider.
 * @param {string} ipfsRootCid - The IPFS Root CID to retrieve from.
 * @param {string} ipfsSubpath - The subpath inside the UnixFS archive to
 *   retrieve, e.g. `/favicon.ico`.
 * @param {number} [cacheTtl=86400] - Cache TTL in seconds (default: 86400).
 *   Default is `86400`
 * @param {object} [options] - Optional parameters.
 * @param {AbortSignal} [options.signal] - An optional AbortSignal to cancel the
 *   fetch request.
 * @returns {Promise<{
 *   response: Response
 *   cacheMiss: boolean
 * }>}
 *
 *   - The response from the fetch request, the cache miss and the content length.
 */
export async function retrieveIpfsContent(
  baseUrl,
  ipfsRootCid,
  ipfsSubpath,
  cacheTtl = 86400,
  { signal } = {},
) {
  // TODO: allow the caller to tweak Trustless GW parameters like `dag-scope` when requesting `format=car`.
  // See https://specs.ipfs.tech/http-gateways/trustless-gateway/
  // TODO: support `raw` format too, see https://github.com/filbeam/worker/issues/295
  const url = getRetrievalUrl(baseUrl, ipfsRootCid, ipfsSubpath) + '?format=car'
  console.log(`Fetching IPFS content from: ${url}`)
  const response = await fetch(url, {
    cf: originCacheOptions(cacheTtl),
    signal,
  })
  const cacheStatus = response.headers.get('CF-Cache-Status')
  if (!cacheStatus) {
    console.log(`CF-Cache-Status was not provided for ${url}`)
  }

  const cacheMiss = cacheStatus !== 'HIT'

  return { response, cacheMiss }
}

/**
 * @param {string} serviceUrl
 * @param {string} rootCid
 * @param {string} subpath
 * @returns {string}
 */
export function getRetrievalUrl(serviceUrl, rootCid, subpath) {
  if (!serviceUrl.endsWith('/')) {
    serviceUrl += '/'
  }
  let url = `${serviceUrl}ipfs/${rootCid}`
  // Curio 404s with trailing slash
  if (subpath !== '/') {
    url += subpath
  }
  return url
}

/**
 * @param {Response} response
 * @param {object} options
 * @param {string} options.ipfsRootCid
 * @param {string} options.ipfsSubpath
 * @param {string | null} options.ipfsFormat
 * @param {AbortSignal} [options.signal]
 * @returns {Promise<{
 *   body: ReadableStream<Uint8Array> | null
 *   getOriginEgressBytes: () => number | null
 *   headers: Headers
 * }>}
 *   - `body` is the stream to serve to the client: raw bytes when converting from
 *       CAR, the original body when serving CAR or passing through.
 *   - `getOriginEgressBytes` returns the number of CAR bytes read from the service
 *       provider, or `null` when the body is passed through unchanged (in that
 *       case the bytes served equal the bytes fetched). Because the CAR is
 *       streamed lazily, the count is only final once `body` has been fully
 *       consumed, so call this after streaming the response.
 *   - `headers` are the response headers to serve, with the CAR-to-raw adjustments
 *       applied when converting.
 */
export async function processIpfsResponse(
  response,
  { ipfsRootCid, ipfsSubpath, ipfsFormat, signal },
) {
  const body = response.body
  if (!response.ok || !body || ipfsFormat === 'car') {
    return {
      body,
      getOriginEgressBytes: () => null,
      headers: response.headers,
    }
  }

  httpAssert(
    ipfsFormat === null,
    400,
    `Unsupported ?format value: "${ipfsFormat}"`,
  )

  // When converting from CAR to raw, set content-disposition to inline so
  // browsers display the content instead of downloading it, and drop the
  // upstream content type so the browser sniffs the raw bytes.
  const headers = new Headers(response.headers)
  headers.set('content-disposition', 'inline')
  headers.delete('content-type')
  headers.delete('x-content-type-options')

  // Count the CAR bytes fetched from the service provider as they stream
  // through. `CarBlockIterator` decodes only the CAR header up front and yields
  // blocks lazily, so the whole archive is never held in memory. The byte count
  // is therefore only final once the caller has fully consumed the returned
  // body, so it is exposed via `getOriginEgressBytes` rather than as a value.
  let originEgressBytes = 0
  const countingBody = (async function* () {
    for await (const chunk of body) {
      originEgressBytes += chunk.length
      yield chunk
    }
  })()

  const blocks = await CarBlockIterator.fromIterable(countingBody)
  const blocksReader = blocks[Symbol.asyncIterator]()

  const entries = exporter(
    `${ipfsRootCid}${ipfsSubpath}`,
    {
      async get(blockCid) {
        const res = await blocksReader.next()
        if (res.done || !res.value) {
          throw new Error(`Block ${blockCid} not found in CAR ${ipfsRootCid}`)
        }
        const block = res.value

        // Compare only the multihashes, so a block stored under an equivalent
        // CID with a different codec or CID version still matches. validateBlock
        // below verifies the block bytes hash to this multihash.
        const actualMultihash = block.cid.multihash.bytes
        const expectedMultihash = blockCid.multihash.bytes
        if (
          actualMultihash.length !== expectedMultihash.length ||
          !actualMultihash.every((byte, i) => byte === expectedMultihash[i])
        ) {
          throw new Error(
            `Unexpected block CID ${block.cid}, expected ${blockCid}`,
          )
        }

        try {
          await validateBlock(block)
        } catch (err) {
          throw new Error(`Invalid block ${blockCid} of root ${ipfsRootCid}`, {
            cause: err,
          })
        }

        return block.bytes
      },
    },
    { signal, blockReadConcurrency: 1 },
  )

  // eslint-disable-next-line no-unreachable-loop
  for await (const entry of entries) {
    signal?.throwIfAborted()
    console.log(`Entry: ${entry.path} (${entry.type})`)

    const expectedPath =
      ipfsSubpath === '/' ? ipfsRootCid : `${ipfsRootCid}${ipfsSubpath}`
    if (entry.path !== expectedPath) {
      throw new Error(
        `Unexpected entry - wrong path: ${describeEntry(entry)} (expected: ${expectedPath})`,
      )
    }

    if (entry.type !== 'file' && entry.type !== 'raw') {
      console.log(`Unexpected entry - wrong type: ${describeEntry(entry)}`)
      httpAssert(false, 404, 'Not Found')
    }

    const entryContent = entry.content()

    // Convert AsyncGenerator to ReadableStream for Response body
    const rawDataStream = new ReadableStream({
      async start(controller) {
        try {
          for await (const chunk of entryContent) {
            signal?.throwIfAborted()
            controller.enqueue(chunk)
          }
          controller.close()
        } catch (error) {
          controller.error(error)
        }
      },
    })

    return {
      body: rawDataStream,
      getOriginEgressBytes: () => originEgressBytes,
      headers,
    }
  }

  httpAssert(false, 404, 'Not Found')
}

/** @param {UnixFSBasicEntry} entry */
export function describeEntry(entry) {
  return JSON.stringify(
    entry,
    (_, v) => (typeof v === 'bigint' ? v.toString() : v),
    2,
  )
}
