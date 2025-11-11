import validator from 'validator'

const PRODUCT_TYPE_PDP = 0

/**
 * @param {{ DB: D1Database }} env
 * @param {string | number} providerId
 * @param {string | number} productType
 * @param {string} capabilityKeys
 * @param {string} capabilityValues
 * @param {number} blockNumber
 * @returns {Promise<Response>}
 */
export async function handleProductAdded(
  env,
  providerId,
  productType,
  capabilityKeys,
  capabilityValues,
  blockNumber,
) {
  if (
    (typeof providerId !== 'string' && typeof providerId !== 'number') ||
    (typeof productType !== 'string' && typeof productType !== 'number') ||
    typeof capabilityKeys !== 'string' ||
    typeof capabilityValues !== 'string' ||
    typeof blockNumber !== 'number'
  ) {
    console.error('ServiceProviderRegistry.ProductAdded: Invalid payload', {
      providerId,
      productType,
    })
    return new Response('Bad Request', { status: 400 })
  }
  if (Number(productType) !== PRODUCT_TYPE_PDP) {
    return new Response('OK', { status: 200 })
  }

  return await handleProviderServiceUrlUpdate(
    env,
    providerId,
    capabilityKeys,
    capabilityValues,
    blockNumber,
  )
}

/**
 * @param {{ DB: D1Database }} env
 * @param {string | number} providerId
 * @param {string | number} productType
 * @param {string} capabilityKeys
 * @param {string} capabilityValues
 * @param {number} blockNumber
 * @returns {Promise<Response>}
 */
export async function handleProductUpdated(
  env,
  providerId,
  productType,
  capabilityKeys,
  capabilityValues,
  blockNumber,
) {
  if (
    (typeof providerId !== 'string' && typeof providerId !== 'number') ||
    (typeof productType !== 'string' && typeof productType !== 'number') ||
    typeof capabilityKeys !== 'string' ||
    typeof capabilityValues !== 'string' ||
    typeof blockNumber !== 'number'
  ) {
    console.error('ServiceProviderRegistry.ProductUpdated: Invalid payload', {
      providerId,
      productType,
    })
    return new Response('Bad Request', { status: 400 })
  }
  if (Number(productType) !== PRODUCT_TYPE_PDP) {
    return new Response('OK', { status: 200 })
  }

  return await handleProviderServiceUrlUpdate(
    env,
    providerId,
    capabilityKeys,
    capabilityValues,
    blockNumber,
  )
}

/**
 * @param {{ DB: D1Database }} env
 * @param {string | number} providerId
 * @param {string | number} productType
 * @returns {Promise<Response>}
 */
export async function handleProductRemoved(env, providerId, productType) {
  if (
    (typeof providerId !== 'string' && typeof providerId !== 'number') ||
    (typeof productType !== 'string' && typeof productType !== 'number')
  ) {
    console.error('ServiceProviderRegistry.ProductRemoved: Invalid payload', {
      providerId,
      productType,
    })
    return new Response('Bad Request', { status: 400 })
  }
  if (Number(productType) !== PRODUCT_TYPE_PDP) {
    return new Response('OK', { status: 200 })
  }

  return await handleProviderRemoval(env, providerId)
}

/**
 * @param {{ DB: D1Database }} env
 * @param {string | number} providerId
 * @returns {Promise<Response>}
 */
export async function handleProviderRemoved(env, providerId) {
  if (typeof providerId !== 'string' && typeof providerId !== 'number') {
    console.error('ServiceProviderRegistry.ProviderRemoved: Invalid payload', {
      providerId,
    })
    return new Response('Bad Request', { status: 400 })
  }

  return await handleProviderRemoval(env, providerId)
}

/**
 * @param {{ DB: D1Database }} env
 * @param {string | number} providerId
 * @param {string} capabilityKeys
 * @param {string} capabilityValues
 * @param {number} blockNumber
 * @returns {Promise<Response>}
 */
async function handleProviderServiceUrlUpdate(
  env,
  providerId,
  capabilityKeys,
  capabilityValues,
  blockNumber,
) {
  const serviceUrlIndex = capabilityKeys.split(',').indexOf('serviceURL')
  if (serviceUrlIndex === -1) {
    console.error('Missing serviceURL in capability keys', {
      capabilityKeys,
    })
    return new Response('OK', { status: 200 })
  }

  const serviceUrlHex = capabilityValues.split(',')[serviceUrlIndex]
  if (!serviceUrlHex) {
    console.error('Missing serviceURL in capability values', {
      capabilityKeys,
      capabilityValues,
    })
    return new Response('OK', { status: 200 })
  }

  if (!serviceUrlHex.startsWith('0x')) {
    console.error('Invalid serviceURL encoding', { serviceUrlHex })
    return new Response('OK', { status: 200 })
  }

  let serviceUrl
  try {
    serviceUrl = Buffer.from(serviceUrlHex.slice(2), 'hex').toString()
  } catch (err) {
    console.error('Invalid serviceURL encoding', {
      serviceUrlHex,
      err,
    })
    return new Response('OK', { status: 200 })
  }

  if (!validator.isURL(serviceUrl)) {
    console.error('Invalid Service URL', {
      serviceUrl,
    })
    return new Response('OK', { status: 200 })
  }

  console.log(
    `Provider service url updated (providerId=${providerId}, serviceUrl=${serviceUrl})`,
  )

  await env.DB.prepare(
    `
        WITH sp AS (SELECT * FROM service_providers WHERE id = ?)
        INSERT OR REPLACE INTO service_providers (
          id,
          service_url,
          block_number,
          is_deleted
        )
        SELECT ?, ?, ?, (SELECT is_deleted FROM sp)
        WHERE NOT EXISTS (
          SELECT * FROM sp WHERE block_number > ?
        )
      `,
  )
    .bind(
      String(providerId),
      String(providerId),
      serviceUrl,
      String(blockNumber),
      String(blockNumber),
    )
    .run()
  return new Response('OK', { status: 200 })
}

/**
 * @param {{ DB: D1Database }} env
 * @param {string | number} providerId
 * @returns {Promise<Response>}
 */
async function handleProviderRemoval(env, providerId) {
  await env.DB.prepare(
    `
        INSERT INTO service_providers (
          id,
          is_deleted
        ) VALUES (?, TRUE)
        ON CONFLICT DO UPDATE SET
          is_deleted = excluded.is_deleted
      `,
  )
    .bind(String(providerId))
    .run()
  return new Response('OK', { status: 200 })
}
