import { CarReader } from '@ipld/car'
// @ts-ignore - Types exist but package.json exports configuration prevents resolution
import * as carBlockValidator from '@web3-storage/car-block-validator'
import { recursive as exporter } from 'ipfs-unixfs-exporter'
import { httpAssert } from './http-assert'

/** @import {UnixFSBasicEntry} from 'ipfs-unixfs-exporter' */
/** @typedef {CarReader['_blocks'][0]} Block */

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
  const response = await fetch(url, {
    cf: {
      cacheTtlByStatus: {
        '200-299': cacheTtl,
        404: 0,
        '500-599': 0,
      },
      cacheEverything: true,
    },
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
 * Measures the egress of a request by reading from a readable stream and return
 * the total number of bytes transferred.
 *
 * @param {ReadableStreamDefaultReader<Uint8Array>} reader - The reader for the
 *   readable stream.
 * @returns {Promise<number>} - A promise that resolves to the total number of
 *   bytes transferred.
 */
export async function measureStreamedEgress(reader) {
  let total = 0
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    total += value.length
  }
  return total
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
 * @param {ReadableStream<Uint8Array>} body
 * @param {object} options
 * @param {string} options.ipfsRootCid
 * @param {string} options.ipfsSubpath
 * @param {string | null} options.ipfsFormat
 * @param {AbortSignal} [options.signal]
 * @returns {Promise<ReadableStream<Uint8Array>>}
 */
export async function processIpfsResponse(
  body,
  { ipfsRootCid, ipfsSubpath, ipfsFormat, signal },
) {
  if (ipfsFormat === 'car') return body
  httpAssert(
    ipfsFormat === null,
    400,
    `Unsupported ?format value: "${ipfsFormat}"`,
  )

  const reader = await CarReader.fromIterable(body)
  const blocksReader = reader.blocks()

  const entries = exporter(
    `${ipfsRootCid}${ipfsSubpath}`,
    {
      async get(blockCid) {
        const res = await blocksReader.next()
        if (res.done || !res.value) {
          throw new Error(`Block ${blockCid} not found in CAR ${ipfsRootCid}`)
        }
        const block = res.value

        // TODO: compare multihashes only
        if (block.cid.toString() !== blockCid.toString()) {
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

    if (entry.type !== 'file') {
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

    return rawDataStream
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
