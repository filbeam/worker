import validator from 'validator'

const PRODUCT_TYPE_PDP = 0

/**
 * @param {{ DB: D1Database }} env
 * @param {string | number} providerId
 * @param {string | number} productType
 * @param {string} capabilityKeys
 * @param {string} capabilityValues
 * @returns {Promise<Response>}
 */
export async function handleProductAdded(
  env,
  providerId,
  productType,
  capabilityKeys,
  capabilityValues,
) {
  if (
    (typeof providerId !== 'string' && typeof providerId !== 'number') ||
    (typeof productType !== 'string' && typeof productType !== 'number') ||
    typeof capabilityKeys !== 'string' ||
    typeof capabilityValues !== 'string'
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
  )
}

/**
 * @param {{ DB: D1Database }} env
 * @param {string | number} providerId
 * @param {string | number} productType
 * @param {string} capabilityKeys
 * @param {string} capabilityValues
 * @returns {Promise<Response>}
 */
export async function handleProductUpdated(
  env,
  providerId,
  productType,
  capabilityKeys,
  capabilityValues,
) {
  if (
    (typeof providerId !== 'string' && typeof providerId !== 'number') ||
    (typeof productType !== 'string' && typeof productType !== 'number') ||
    typeof capabilityKeys !== 'string' ||
    typeof capabilityValues !== 'string'
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

  const result = await env.DB.prepare(
    `
        DELETE FROM service_providers WHERE id = ?
      `,
  )
    .bind(String(providerId))
    .run()
  if (result.meta.changes === 0) {
    return new Response('Provider Not Found', { status: 404 })
  }
  return new Response('OK', { status: 200 })
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

  const result = await env.DB.prepare(
    `
        DELETE FROM service_providers WHERE id = ?
      `,
  )
    .bind(String(providerId))
    .run()
  if (result.meta.changes === 0) {
    console.error(
      'ServiceProviderRegistry.ProviderRemoved: Provider not found',
      {
        providerId,
      },
    )
    return new Response('Provider Not Found', { status: 404 })
  }
  return new Response('OK', { status: 200 })
}

/**
 * @param {{ DB: D1Database }} env
 * @param {string | number} providerId
 * @param {string} capabilityKeys
 * @param {string} capabilityValues
 * @returns {Promise<Response>}
 */
async function handleProviderServiceUrlUpdate(
  env,
  providerId,
  capabilityKeys,
  capabilityValues,
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
        INSERT INTO service_providers (
          id,
          service_url
        )
        VALUES (?, ?)
        ON CONFLICT(id) DO UPDATE SET
          service_url=excluded.service_url
      `,
  )
    .bind(String(providerId), serviceUrl)
    .run()
  return new Response('OK', { status: 200 })
}
