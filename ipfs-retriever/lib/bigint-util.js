import { base32 } from 'multiformats/bases/base32'

/**
 * @param {BigInt} value
 * @returns {Uint8Array}
 */
export function bigIntToUint8Array(value) {
  if (typeof value !== 'bigint') {
    throw new TypeError('Expected a BigInt value')
  }
  if (value < 0n) {
    throw new Error('Cannot convert negative bigint to Uint8Array')
  }
  let hex = value.toString(16)
  if (hex.length % 2) hex = '0' + hex
  const bytes = hex.match(/.{2}/g).map((byte) => parseInt(byte, 16))
  return new Uint8Array(bytes)
}

/**
 * @param {Uint8Array} value
 * @returns {BigInt}
 */
export function uint8ArrayToBigInt(value) {
  if (!(value instanceof Uint8Array)) {
    throw new TypeError('Expected a Uint8Array value')
  }
  if (value.length === 0) {
    return 0n
  }
  const hex = [...value].map((x) => x.toString(16).padStart(2, '0')).join('')
  return BigInt('0x' + hex)
}

/**
 * Converts a BigInt to a base32-encoded string
 *
 * @param {BigInt} value
 * @returns {string}
 */
export function bigIntToBase32(value) {
  if (typeof value !== 'bigint') {
    throw new TypeError('Expected a BigInt value')
  }
  if (value < 0n) {
    throw new Error('Cannot convert negative bigint to base32')
  }
  // Use "0" for zero value (0 is not a base32 character but is DNS-safe)
  if (value === 0n) {
    return '0'
  }
  const bytes = bigIntToUint8Array(value)
  // Remove the 'b' prefix that multiformats adds
  return base32.encode(bytes).slice(1)
}

/**
 * Converts a base32-encoded string to a BigInt
 *
 * @param {string} value
 * @returns {BigInt}
 */
export function base32ToBigInt(value) {
  if (typeof value !== 'string') {
    throw new TypeError('Expected a string value')
  }
  // Handle special case for zero
  if (value === '0') {
    return 0n
  }
  // Add back the 'b' prefix that multiformats expects
  const bytes = base32.decode('b' + value)
  return uint8ArrayToBigInt(bytes)
}
