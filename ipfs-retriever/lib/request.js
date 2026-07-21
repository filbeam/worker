import { httpAssert } from '@filbeam/retrieval'
import { base32ToBigInt } from './bigint-util.js'

/**
 * Parse params found in path of the request URL
 *
 * @param {Request} request
 * @param {object} options
 * @param {string} options.DNS_ROOT
 * @returns {{
 *   dataSetId: string
 *   pieceId: string
 *   ipfsSubpath: string
 *   ipfsFormat: string | null
 * }}
 */
export function parseRequest(request, { DNS_ROOT }) {
  const url = new URL(request.url)
  console.log('retrieval request', { DNS_ROOT, url })

  httpAssert(
    url.hostname.endsWith(DNS_ROOT),
    400,
    `Invalid hostname: ${url.hostname}. It must end with ${DNS_ROOT}.`,
  )

  const slug = url.hostname.slice(0, -DNS_ROOT.length)
  const parts = slug.split('-')

  httpAssert(
    parts.length === 3,
    400,
    `The hostname must be in the format: 1-{dataSetId}-{pieceId}${DNS_ROOT}`,
  )

  const [version, encodedDataSetId, encodedPieceId] = parts

  httpAssert(
    version === '1',
    400,
    `Unsupported slug version: ${version}. Expected version 1.`,
  )

  httpAssert(
    encodedDataSetId && encodedPieceId,
    400,
    `The hostname must be in the format: 1-{dataSetId}-{pieceId}${DNS_ROOT}`,
  )

  let dataSetId
  let pieceId

  try {
    dataSetId = base32ToBigInt(encodedDataSetId).toString()
  } catch (error) {
    httpAssert(
      false,
      400,
      `Invalid dataSetId encoding in slug: ${encodedDataSetId}. ${error instanceof Error ? error.message : String(error)}`,
    )
  }

  try {
    pieceId = base32ToBigInt(encodedPieceId).toString()
  } catch (error) {
    httpAssert(
      false,
      400,
      `Invalid pieceId encoding in slug: ${encodedPieceId}. ${error instanceof Error ? error.message : String(error)}`,
    )
  }

  const ipfsSubpath = url.pathname || '/'
  const ipfsFormat = url.searchParams.get('format')

  return { dataSetId, pieceId, ipfsSubpath, ipfsFormat }
}
