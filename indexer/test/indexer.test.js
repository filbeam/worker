import { describe, it, expect, vi, beforeEach } from 'vitest'
import { assertCloseToNow } from './test-helpers.js'
import workerImpl from '../bin/indexer.js'
import {
  env,
  createExecutionContext,
  waitOnExecutionContext,
} from 'cloudflare:test'

const randomId = () => String(Math.ceil(Math.random() * 1e10))

env.SECRET_HEADER_KEY = 'secret-header-key'
env.SECRET_HEADER_VALUE = 'secret-header-value'
env.CHAINALYSIS_API_KEY = 'mock-chainalysis-api-key'

describe('retriever.indexer', () => {
  beforeEach(async () => {
    // Reset the database before each test
    await env.DB.exec('DELETE FROM service_providers')
  })

  it('requires authentication', async () => {
    const req = new Request('https://host/', { method: 'POST' })
    const res = await workerImpl.fetch(req, env)
    expect(res.status).toBe(401)
    expect(await res.text()).toBe('Unauthorized')
  })
  it('returns 405 for non-POST requests', async () => {
    const req = new Request('https://host/', {
      method: 'GET',
      headers: {
        [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
      },
    })
    const res = await workerImpl.fetch(req, env)
    expect(res.status).toBe(405)
    expect(await res.text()).toBe('Method Not Allowed')
  })

  describe('POST /fwss/data-set-created', () => {
    const ctx = {}
    env.RETRY_QUEUE = {
      send: vi.fn(),
      retry: vi.fn(),
    }
    const mockCheckIfAddressIsSanctioned = vi.fn()

    beforeEach(() => {
      // Reset mocks before each test
      vi.resetAllMocks()
    })

    it('returns 400 if property is missing', async () => {
      const req = new Request('https://host/fwss/data-set-created', {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({}),
      })
      const res = await workerImpl.fetch(req, env, ctx, {
        checkIfAddressIsSanctioned: mockCheckIfAddressIsSanctioned,
      })
      expect(res.status).toBe(400)
      expect(await res.text()).toBe('Bad Request')
    })
    it('inserts a data set', async () => {
      const dataSetId = randomId()
      const providerId = randomId()
      const req = new Request('https://host/fwss/data-set-created', {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({
          data_set_id: dataSetId,
          payer: '0xPayerAddress',
          provider_id: providerId,
          metadata_keys: ['withCDN'],
          metadata_values: [''],
        }),
      })

      mockCheckIfAddressIsSanctioned.mockResolvedValueOnce(false)
      const res = await workerImpl.fetch(req, env, ctx, {
        checkIfAddressIsSanctioned: mockCheckIfAddressIsSanctioned,
      })
      expect(res.status).toBe(200)
      expect(await res.text()).toBe('OK')

      const { results: dataSets } = await env.DB.prepare(
        'SELECT * FROM data_sets WHERE id = ?',
      )
        .bind(dataSetId)
        .all()

      const { results: walletDetails } = await env.DB.prepare(
        'SELECT * FROM wallet_details WHERE address = ?',
      )
        .bind('0xPayerAddress'.toLowerCase())
        .all()

      expect(dataSets.length).toBe(1)
      expect(dataSets[0].id).toBe(dataSetId)
      expect(dataSets[0].service_provider_id).toBe(providerId)
      expect(dataSets[0].payer_address).toBe('0xPayerAddress'.toLowerCase())
      expect(dataSets[0].with_cdn).toBe(1)

      expect(walletDetails.length).toBe(1)
      expect(walletDetails[0].is_sanctioned).toBe(0)
      assertCloseToNow(walletDetails[0].last_screened_at)
    })
    it('does not insert duplicate data sets', async () => {
      const dataSetId = randomId()
      const providerId = randomId()
      for (let i = 0; i < 2; i++) {
        const req = new Request('https://host/fwss/data-set-created', {
          method: 'POST',
          headers: {
            [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
          },
          body: JSON.stringify({
            data_set_id: dataSetId,
            payer: '0xPayerAddress',
            provider_id: providerId,
            metadata_keys: ['withCDN'],
            metadata_values: [''],
          }),
        })
        mockCheckIfAddressIsSanctioned.mockResolvedValueOnce(false)
        const res = await workerImpl.fetch(req, env, ctx, {
          checkIfAddressIsSanctioned: mockCheckIfAddressIsSanctioned,
        })
        expect(res.status).toBe(200)
        expect(await res.text()).toBe('OK')
      }

      const { results: dataSets } = await env.DB.prepare(
        'SELECT * FROM data_sets WHERE id = ?',
      )
        .bind(dataSetId)
        .all()
      expect(dataSets.length).toBe(1)
    })

    it('rejects numeric ID values', async () => {
      const dataSetId = Number(randomId())
      const providerId = Number(randomId())
      const req = new Request('https://host/fwss/data-set-created', {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({
          data_set_id: dataSetId,
          payer: '0xPayerAddress',
          provider_id: providerId,
          metadata_keys: ['withCDN'],
          metadata_values: [''],
        }),
      })
      mockCheckIfAddressIsSanctioned.mockResolvedValueOnce(false)
      const res = await workerImpl.fetch(req, env, ctx, {
        checkIfAddressIsSanctioned: mockCheckIfAddressIsSanctioned,
      })
      expect(res.status).toBe(400)
    })

    it('checks if payer address is sanctioned when with_cdn = true', async () => {
      const dataSetId = randomId()
      const providerId = randomId()

      // send first request with with_cdn = true
      let req = new Request('https://host/fwss/data-set-created', {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({
          data_set_id: dataSetId,
          payer: '0xPayerAddress',
          provider_id: providerId,
          metadata_keys: ['withCDN'],
          metadata_values: [''],
        }),
      })

      mockCheckIfAddressIsSanctioned.mockResolvedValue(true)
      let res = await workerImpl.fetch(req, env, ctx, {
        checkIfAddressIsSanctioned: mockCheckIfAddressIsSanctioned,
      })
      expect(res.status).toBe(200)
      expect(await res.text()).toBe('OK')

      // send second request with with_cdn = false
      req = new Request('https://host/fwss/data-set-created', {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({
          data_set_id: randomId(),
          payer: '0xPayerAddress',
          provider_id: providerId,
          metadata_keys: [],
          metadata_values: [],
        }),
      })
      res = await workerImpl.fetch(req, env, ctx, {
        checkIfAddressIsSanctioned: mockCheckIfAddressIsSanctioned,
      })
      expect(res.status).toBe(200)
      expect(await res.text()).toBe('OK')

      // Verify that the sanction check was called only once
      expect(mockCheckIfAddressIsSanctioned).toHaveBeenCalledTimes(1)
      expect(mockCheckIfAddressIsSanctioned).toHaveBeenCalledWith(
        '0xPayerAddress',
        {
          CHAINALYSIS_API_KEY: env.CHAINALYSIS_API_KEY,
        },
      )

      const { results: dataSets } = await env.DB.prepare(
        'SELECT * FROM data_sets WHERE id = ?',
      )
        .bind(dataSetId)
        .all()

      const { results: walletDetails } = await env.DB.prepare(
        'SELECT * FROM wallet_details WHERE address = ?',
      )
        .bind('0xPayerAddress'.toLowerCase())
        .all()

      expect(dataSets.length).toBe(1)
      expect(dataSets[0].payer_address).toBe('0xPayerAddress'.toLowerCase())

      expect(walletDetails.length).toBe(1)
      expect(walletDetails[0].address).toBe('0xPayerAddress'.toLowerCase())
      expect(walletDetails[0].is_sanctioned).toBe(1)
    })

    it('updates is_sanctioned status of existing wallet', async () => {
      // When the wallet creates the first ProofSet, it's not sanctioned yet
      let req = new Request('https://host/fwss/data-set-created', {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({
          data_set_id: randomId(),
          payer: '0xPayerAddress',
          provider_id: randomId(),
          metadata_keys: ['withCDN'],
          metadata_values: [''],
        }),
      })

      mockCheckIfAddressIsSanctioned.mockResolvedValue(false)
      let res = await workerImpl.fetch(req, env, ctx, {
        checkIfAddressIsSanctioned: mockCheckIfAddressIsSanctioned,
      })
      expect(res.status).toBe(200)
      expect(await res.text()).toBe('OK')

      const { results: initialWalletDetails } = await env.DB.prepare(
        'SELECT * FROM wallet_details WHERE address = ?',
      )
        .bind('0xPayerAddress'.toLowerCase())
        .all()
      expect(initialWalletDetails.length).toBe(1)
      expect(initialWalletDetails[0].address).toBe(
        '0xPayerAddress'.toLowerCase(),
      )
      expect(initialWalletDetails[0].is_sanctioned).toBe(0)
      assertCloseToNow(initialWalletDetails[0].last_screened_at)

      // When the wallet creates the second ProofSet some time later,
      // it's flagged as sanctioned

      await env.DB.exec(
        'UPDATE wallet_details SET last_screened_at = datetime("now", "-1 day")',
      )
      mockCheckIfAddressIsSanctioned.mockResolvedValue(true)
      req = new Request('https://host/fwss/data-set-created', {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({
          data_set_id: randomId(),
          payer: '0xPayerAddress',
          provider_id: randomId(),
          metadata_keys: ['withCDN'],
          metadata_values: [''],
        }),
      })
      res = await workerImpl.fetch(req, env, ctx, {
        checkIfAddressIsSanctioned: mockCheckIfAddressIsSanctioned,
      })
      expect(res.status).toBe(200)
      expect(await res.text()).toBe('OK')

      const { results: walletDetails } = await env.DB.prepare(
        'SELECT * FROM wallet_details WHERE address = ?',
      )
        .bind('0xPayerAddress'.toLowerCase())
        .all()

      expect(walletDetails.length).toBe(1)
      expect(walletDetails[0].address).toBe('0xPayerAddress'.toLowerCase())
      expect(walletDetails[0].is_sanctioned).toBe(1)
      assertCloseToNow(walletDetails[0].last_screened_at)
    })

    it('sends message to queue if sanction check fails', async () => {
      const dataSetId = randomId()
      const providerId = randomId()
      const payload = {
        data_set_id: dataSetId,
        payer: '0xPayerAddress',
        provider_id: providerId,
        metadata_keys: ['withCDN'],
        metadata_values: [''],
      }
      const req = new Request('https://host/fwss/data-set-created', {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify(payload),
      })
      const res = await workerImpl.fetch(req, env, ctx, {
        checkIfAddressIsSanctioneded: async (apiKey, address) => {
          throw Error('fail')
        },
      })
      expect(res.status).toBe(200)
      expect(await res.text()).toBe('OK')

      expect(env.RETRY_QUEUE.send).toHaveBeenCalledTimes(1)
      expect(env.RETRY_QUEUE.send).toHaveBeenCalledWith({
        type: 'fwss-data-set-created',
        payload,
      })

      const { results: dataSets } = await env.DB.prepare(
        'SELECT * FROM data_sets WHERE id = ?',
      )
        .bind(dataSetId)
        .all()
      expect(dataSets.length).toBe(0)
    })

    it('inserts a data set with both withCDN and withIPFSIndexing enabled', async () => {
      const dataSetId = randomId()
      const providerId = randomId()
      const req = new Request('https://host/fwss/data-set-created', {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({
          data_set_id: dataSetId,
          payer: '0xPayerAddress',
          provider_id: providerId,
          metadata_keys: ['withCDN', 'withIPFSIndexing'],
          metadata_values: ['', ''],
        }),
      })

      mockCheckIfAddressIsSanctioned.mockResolvedValueOnce(false)
      const res = await workerImpl.fetch(req, env, ctx, {
        checkIfAddressIsSanctioned: mockCheckIfAddressIsSanctioned,
      })
      expect(res.status).toBe(200)
      expect(await res.text()).toBe('OK')

      const { results: dataSets } = await env.DB.prepare(
        'SELECT * FROM data_sets WHERE id = ?',
      )
        .bind(dataSetId)
        .all()

      expect(dataSets.length).toBe(1)
      expect(dataSets[0].id).toBe(dataSetId)
      expect(dataSets[0].service_provider_id).toBe(providerId)
      expect(dataSets[0].payer_address).toBe('0xPayerAddress'.toLowerCase())
      expect(dataSets[0].with_cdn).toBe(1)
      expect(dataSets[0].with_ipfs_indexing).toBe(1)
    })

    it('inserts a data set with neither withCDN nor withIPFSIndexing', async () => {
      const dataSetId = randomId()
      const providerId = randomId()
      const req = new Request('https://host/fwss/data-set-created', {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({
          data_set_id: dataSetId,
          payer: '0xPayerAddress',
          provider_id: providerId,
          metadata_keys: [],
          metadata_values: [],
        }),
      })

      const res = await workerImpl.fetch(req, env, ctx, {
        checkIfAddressIsSanctioned: mockCheckIfAddressIsSanctioned,
      })
      expect(res.status).toBe(200)
      expect(await res.text()).toBe('OK')

      const { results: dataSets } = await env.DB.prepare(
        'SELECT * FROM data_sets WHERE id = ?',
      )
        .bind(dataSetId)
        .all()

      expect(dataSets.length).toBe(1)
      expect(dataSets[0].id).toBe(dataSetId)
      expect(dataSets[0].service_provider_id).toBe(providerId)
      expect(dataSets[0].payer_address).toBe('0xPayerAddress'.toLowerCase())
      expect(dataSets[0].with_cdn).toBe(0)
      expect(dataSets[0].with_ipfs_indexing).toBe(0)
    })
  })

  describe('POST /fwss/piece-added', () => {
    const TEST_CID_HEX =
      '0x0155912024c6db010b63fa0aff84de00a4cd98802e03d1df5ea18ea430c3a0cdc84af4fc4024ab2714'

    const CTX = {}

    it('returns 400 if data_set_id or piece_id is missing', async () => {
      const req = new Request('https://host/fwss/piece-added', {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({}),
      })
      const res = await workerImpl.fetch(req, env)
      expect(res.status).toBe(400)
      expect(await res.text()).toBe('Bad Request')
    })

    it('inserts a piece for a data set', async () => {
      const dataSetId = randomId()
      const req = new Request('https://host/fwss/piece-added', {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({
          data_set_id: dataSetId.toString(),
          piece_id: '91',
          piece_cid:
            '0x0155912024c6db010b63fa0aff84de00a4cd98802e03d1df5ea18ea430c3a0cdc84af4fc4024ab2714',
          metadata_keys: [],
          metadata_values: [],
        }),
      })
      const res = await workerImpl.fetch(req, env, CTX)
      expect(res.status).toBe(200)
      expect(await res.text()).toBe('OK')

      const { results: pieces } = await env.DB.prepare(
        'SELECT id, cid FROM pieces WHERE data_set_id = ? ORDER BY id',
      )
        .bind(dataSetId)
        .all()
      expect(pieces).toEqual([
        {
          id: '91',
          cid: 'bafkzcibey3nqcc3d7ifp7bg6acsm3geafyb5dx26ughkimgdudg4qsxu7racjkzhcq',
        },
      ])
    })

    it('does not insert duplicate pieces for the same data set', async () => {
      const dataSetId = randomId()
      const pieceId = randomId().toString()
      for (let i = 0; i < 2; i++) {
        const req = new Request('https://host/fwss/piece-added', {
          method: 'POST',
          headers: {
            [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
          },
          body: JSON.stringify({
            data_set_id: dataSetId,
            piece_id: pieceId,
            piece_cid: TEST_CID_HEX,
            metadata_keys: [],
            metadata_values: [],
          }),
        })
        const res = await workerImpl.fetch(req, env, CTX)
        expect(res.status).toBe(200)
        expect(await res.text()).toBe('OK')
      }

      const { results: pieces } = await env.DB.prepare(
        'SELECT * FROM pieces WHERE data_set_id = ?',
      )
        .bind(dataSetId)
        .all()
      expect(pieces.length).toBe(1)
    })

    it('allows multiple data sets to have the same piece id', async () => {
      const dataSetIds = [randomId(), randomId()]
      dataSetIds.sort()

      for (const dataSetId of dataSetIds) {
        const req = new Request('https://host/fwss/piece-added', {
          method: 'POST',
          headers: {
            [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
          },
          body: JSON.stringify({
            data_set_id: dataSetId,
            piece_id: '0',
            piece_cid: TEST_CID_HEX,
            metadata_keys: [],
            metadata_values: [],
          }),
        })
        const res = await workerImpl.fetch(req, env, CTX)
        const body = await res.text()
        expect(`${res.status} ${body}`).toBe('200 OK')
      }

      const { results: pieces } = await env.DB.prepare(
        'SELECT data_set_id, id FROM pieces WHERE data_set_id = ? OR data_set_id = ? ORDER BY data_set_id',
      )
        .bind(dataSetIds[0], dataSetIds[1])
        .all()

      expect(pieces).toEqual([
        {
          data_set_id: dataSetIds[0],
          id: '0',
        },
        {
          data_set_id: dataSetIds[1],
          id: '0',
        },
      ])
    })

    it('inserts a piece with ipfsRootCid metadata', async () => {
      const dataSetId = randomId()
      const ipfsRootCid =
        'bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi'
      const req = new Request('https://host/fwss/piece-added', {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({
          data_set_id: dataSetId.toString(),
          piece_id: '42',
          piece_cid: TEST_CID_HEX,
          metadata_keys: ['ipfsRootCID'],
          metadata_values: [ipfsRootCid],
        }),
      })
      const res = await workerImpl.fetch(req, env, CTX)
      expect(res.status).toBe(200)
      expect(await res.text()).toBe('OK')

      const { results: pieces } = await env.DB.prepare(
        'SELECT id, cid, ipfs_root_cid FROM pieces WHERE data_set_id = ? ORDER BY id',
      )
        .bind(dataSetId)
        .all()
      expect(pieces).toEqual([
        {
          id: '42',
          cid: 'bafkzcibey3nqcc3d7ifp7bg6acsm3geafyb5dx26ughkimgdudg4qsxu7racjkzhcq',
          ipfs_root_cid: ipfsRootCid,
        },
      ])
    })
  })

  describe('POST /pdp-verifier/pieces-removed', () => {
    const CTX = {}
    it('returns 400 if data_set_id or piece_ids is missing', async () => {
      const req = new Request('https://host/pdp-verifier/pieces-removed', {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({}),
      })
      const res = await workerImpl.fetch(req, env, {})
      expect(res.status).toBe(400)
      expect(await res.text()).toBe('Bad Request')
    })

    it('deletes pieces for a data set', async () => {
      const dataSetId = randomId()
      const pieceIds = [randomId().toString(), randomId().toString()]
      const pieceCids = [randomId(), randomId()]
      const req = new Request('https://host/pdp-verifier/pieces-removed', {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({
          data_set_id: dataSetId,
          piece_ids: pieceIds,
        }),
      })

      await withPieces(env, dataSetId, pieceIds, pieceCids)
      const res = await workerImpl.fetch(req, env, CTX, {})
      expect(res.status).toBe(200)
      expect(await res.text()).toBe('OK')

      const { results: pieces } = await env.DB.prepare(
        'SELECT * FROM pieces WHERE data_set_id = ?',
      )
        .bind(dataSetId)
        .all()
      expect(pieces.length).toBe(0)
    })

    it('clears index cache entries', async () => {
      const payerAddress = '0xPayerAddress'
      const dataSetId = await withDataSet(env, {
        withCDN: true,
        serviceProviderId: '1',
        payerAddress,
      })
      const pieceIds = [randomId(), randomId()]
      const pieceCids = [randomId(), randomId()]
      await withPieces(env, dataSetId, pieceIds, pieceCids)
      for (const pieceCid of pieceCids) {
        await env.KV.put(
          `${payerAddress}/${pieceCid}`,
          JSON.stringify([dataSetId, 'https://service.url/'])
        )
      }
      const req = new Request('https://host/pdp-verifier/pieces-removed', {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({
          data_set_id: dataSetId,
          piece_ids: pieceIds,
        }),
      })
      const res = await workerImpl.fetch(req, env)
      expect(res.status).toBe(200)

      for (const pieceCid of pieceCids) {
        expect(await env.KV.get(`${payerAddress}/${pieceCid}`)).toBe(null)
      }
    })
  })

  describe('POST /service-provider-registry/product-added', () => {
    it('returns 400 if provider_id and product_type are missing', async () => {
      const req = new Request(
        'https://host/service-provider-registry/product-added',
        {
          method: 'POST',
          headers: {
            [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
          },
          body: JSON.stringify({}),
        },
      )
      const res = await workerImpl.fetch(req, env)
      expect(res.status).toBe(400)
      expect(await res.text()).toBe('Bad Request')
    })
    it('inserts a provider service URL', async () => {
      const serviceUrl = 'https://provider.example.com'
      const providerId = 123
      const req = new Request(
        'https://host/service-provider-registry/product-added',
        {
          method: 'POST',
          headers: {
            [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
          },
          body: JSON.stringify({
            provider_id: providerId,
            product_type: 0,
            service_url: serviceUrl,
          }),
        },
      )
      const ctx = createExecutionContext()
      const res = await workerImpl.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)
      expect(res.status).toBe(200)
      expect(await res.text()).toBe('OK')

      const { results: providers } = await env.DB.prepare(
        'SELECT * FROM service_providers',
      ).all()
      expect(providers).toEqual([
        {
          id: providerId.toString(),
          service_url: serviceUrl,
        },
      ])
    })
  })
  describe('POST /service-provider-registry/product-updated', () => {
    it('updates service URLs for an existing provider', async () => {
      const serviceUrl = 'https://provider.example.com'
      const providerId = 0
      const newServiceUrl = 'https://new-provider.example.com'

      // First insert the initial provider URL
      let req = new Request(
        'https://host/service-provider-registry/product-added',
        {
          method: 'POST',
          headers: {
            [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
          },
          body: JSON.stringify({
            provider_id: providerId,
            product_type: 0,
            service_url: serviceUrl,
          }),
        },
      )
      let ctx = createExecutionContext()
      let res = await workerImpl.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)
      expect(res.status).toBe(200)
      expect(await res.text()).toBe('OK')

      // Now update the provider URL
      req = new Request(
        'https://host/service-provider-registry/product-added',
        {
          method: 'POST',
          headers: {
            [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
          },
          body: JSON.stringify({
            provider_id: providerId,
            product_type: 0,
            service_url: newServiceUrl,
          }),
        },
      )
      ctx = createExecutionContext()
      res = await workerImpl.fetch(req, env, ctx)
      await waitOnExecutionContext(ctx)
      expect(res.status).toBe(200)
      expect(await res.text()).toBe('OK')

      const { results: providers } = await env.DB.prepare(
        'SELECT * FROM service_providers WHERE id = ?',
      )
        .bind(String(providerId))
        .all()
      expect(providers.length).toBe(1)
      expect(providers[0].service_url).toBe(newServiceUrl)
    })
  })
  describe('POST /service-provider-registry/product-removed', () => {
    it('returns 400 if provider id and product type are missing', async () => {
      const req = new Request(
        'https://host/service-provider-registry/product-removed',
        {
          method: 'POST',
          headers: {
            [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
          },
          body: JSON.stringify({}),
        },
      )
      const res = await workerImpl.fetch(req, env)
      expect(res.status).toBe(400)
      expect(await res.text()).toBe('Bad Request')
    })

    it('removes a provider from the providers table', async () => {
      const providerId = 0
      const productType = 0
      const serviceUrl = 'https://provider.example.com'

      // First, insert a provider
      const insertReq = new Request(
        'https://host/service-provider-registry/product-added',
        {
          method: 'POST',
          headers: {
            [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
          },
          body: JSON.stringify({
            provider_id: providerId,
            product_type: productType,
            service_url: serviceUrl,
          }),
        },
      )
      const ctx = createExecutionContext()
      const insertRes = await workerImpl.fetch(insertReq, env, ctx)
      await waitOnExecutionContext(ctx)
      expect(insertRes.status).toBe(200)
      expect(await insertRes.text()).toBe('OK')

      // Now, remove the provider
      const removeReq = new Request(
        'https://host/service-provider-registry/product-removed',
        {
          method: 'POST',
          headers: {
            [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
          },
          body: JSON.stringify({
            provider_id: providerId,
            product_type: productType,
          }),
        },
      )
      const removeRes = await workerImpl.fetch(removeReq, env)
      expect(removeRes.status).toBe(200)
      expect(await removeRes.text()).toBe('OK')

      // Verify that the provider is removed from the database
      const { results: providers } = await env.DB.prepare(
        'SELECT * FROM service_providers WHERE id = ?',
      )
        .bind(String(providerId))
        .all()
      expect(providers.length).toBe(0) // The provider should be removed
    })

    it('returns 404 if the provider does not exist', async () => {
      const req = new Request(
        'https://host/service-provider-registry/product-removed',
        {
          method: 'POST',
          headers: {
            [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
          },
          body: JSON.stringify({
            provider_id: 13,
            product_type: 0,
          }),
        },
      )
      const res = await workerImpl.fetch(req, env)
      expect(res.status).toBe(404)
      expect(await res.text()).toBe('Provider Not Found')
    })

    it('clears index cache entries', async () => {
      const payerAddress = '0xPayerAddress'
      const serviceProviderId = '1'
      const productType = 0
      const serviceUrl = 'https://service.url/'
      const dataSetId = await withDataSet(env, {
        withCDN: true,
        serviceProviderId,
        payerAddress,
      })
      const pieceIds = [randomId(), randomId()]
      const pieceCids = [randomId(), randomId()]
      await withPieces(env, dataSetId, pieceIds, pieceCids)
      for (const pieceCid of pieceCids) {
        await env.KV.put(
          `${payerAddress}/${pieceCid}`,
          JSON.stringify([dataSetId, serviceUrl])
        )
      }

      const insertReq = new Request(
        'https://host/service-provider-registry/product-added',
        {
          method: 'POST',
          headers: {
            [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
          },
          body: JSON.stringify({
            provider_id: serviceProviderId,
            product_type: productType,
            service_url: serviceUrl,
          }),
        },
      )
      const ctx = createExecutionContext()
      const insertRes = await workerImpl.fetch(insertReq, env, ctx)
      await waitOnExecutionContext(ctx)
      expect(insertRes.status).toBe(200)

      const req = new Request('https://host/service-provider-registry/product-removed', {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({
          provider_id: serviceProviderId,
          product_type: productType,
        }),
      })
      const res = await workerImpl.fetch(req, env)
      expect(res.status).toBe(200)

      for (const pieceCid of pieceCids) {
        expect(await env.KV.get(`${payerAddress}/${pieceCid}`)).toBe(null)
      }
    })
  })
})
describe('POST /service-provider-registry/provider-removed', () => {
  it('returns 400 if the provider id is missing', async () => {
    const req = new Request(
      'https://host/service-provider-registry/provider-removed',
      {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({}),
      },
    )
    const res = await workerImpl.fetch(req, env)
    expect(res.status).toBe(400)
    expect(await res.text()).toBe('Bad Request')
  })

  it('removes a provider from the providers table', async () => {
    const providerId = 0
    const blockNumber = 10
    const serviceUrl = 'https://provider.example.com'

    // First, insert a provider
    const insertReq = new Request(
      'https://host/service-provider-registry/product-added',
      {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({
          provider_id: providerId,
          product_type: 0,
          block_number: blockNumber,
          service_url: serviceUrl,
        }),
      },
    )
    const ctx = createExecutionContext()
    const insertRes = await workerImpl.fetch(insertReq, env, ctx)
    await waitOnExecutionContext(ctx)
    expect(insertRes.status).toBe(200)
    expect(await insertRes.text()).toBe('OK')

    // Now, remove the provider
    const removeReq = new Request(
      'https://host/service-provider-registry/provider-removed',
      {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({
          provider_id: providerId,
        }),
      },
    )
    const removeRes = await workerImpl.fetch(removeReq, env)
    expect(removeRes.status).toBe(200)
    expect(await removeRes.text()).toBe('OK')

    // Verify that the provider is removed from the database
    const { results: providers } = await env.DB.prepare(
      'SELECT * FROM service_providers WHERE id = ?',
    )
      .bind(providerId)
      .all()
    expect(providers.length).toBe(0) // The provider should be removed
  })

  it('returns 404 if the provider does not exist', async () => {
    const req = new Request(
      'https://host/service-provider-registry/provider-removed',
      {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({
          provider_id: 13,
        }),
      },
    )
    const res = await workerImpl.fetch(req, env)
    expect(res.status).toBe(404)
    expect(await res.text()).toBe('Provider Not Found')
  })

  it('clears index cache entries', async () => {
      const payerAddress = '0xPayerAddress'
      const serviceProviderId = '1'
      const productType = 0
      const serviceUrl = 'https://service.url/'
      const dataSetId = await withDataSet(env, {
        withCDN: true,
        serviceProviderId,
        payerAddress,
      })
      const pieceIds = [randomId(), randomId()]
      const pieceCids = [randomId(), randomId()]
      await withPieces(env, dataSetId, pieceIds, pieceCids)
      for (const pieceCid of pieceCids) {
        await env.KV.put(
          `${payerAddress}/${pieceCid}`,
          JSON.stringify([dataSetId, serviceUrl])
        )
      }

      const insertReq = new Request(
        'https://host/service-provider-registry/product-added',
        {
          method: 'POST',
          headers: {
            [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
          },
          body: JSON.stringify({
            provider_id: serviceProviderId,
            product_type: productType,
            service_url: serviceUrl,
          }),
        },
      )
      const ctx = createExecutionContext()
      const insertRes = await workerImpl.fetch(insertReq, env, ctx)
      await waitOnExecutionContext(ctx)
      expect(insertRes.status).toBe(200)

      const req = new Request('https://host/service-provider-registry/provider-removed', {
        method: 'POST',
        headers: {
          [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
        },
        body: JSON.stringify({
          provider_id: serviceProviderId,
        }),
      })
      const res = await workerImpl.fetch(req, env)
      expect(res.status).toBe(200)

      for (const pieceCid of pieceCids) {
        expect(await env.KV.get(`${payerAddress}/${pieceCid}`)).toBe(null)
      }
    })
})

async function withPieces(
  env,
  dataSetId,
  pieceIds,
  pieceCids,
  ipfsRootCids = [],
) {
  await env.DB.prepare(
    `
    INSERT INTO pieces (
      id,
      data_set_id,
      cid,
      ipfs_root_cid
    )
    VALUES ${new Array(pieceIds.length)
      .fill(null)
      .map(() => '(?, ?, ?, ?)')
      .join(', ')}
    ON CONFLICT DO NOTHING
  `,
  )
    .bind(
      ...pieceIds.flatMap((pieceId, i) => [
        String(pieceId),
        String(dataSetId),
        pieceCids[i],
        ipfsRootCids[i] || null,
      ]),
    )
    .run()
}

describe('POST /fwss/cdn-service-terminated', () => {
  beforeEach(async () => {
    await env.DB.exec('DELETE FROM data_sets')
  })

  it('returns 400 if data_set_id is missing', async () => {
    const req = new Request('https://host/fwss/cdn-service-terminated', {
      method: 'POST',
      headers: {
        [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
      },
      body: JSON.stringify({}),
    })
    const res = await workerImpl.fetch(req, env)
    expect(res.status).toBe(400)
    expect(await res.text()).toBe('Bad Request')
  })

  it('sets `withCDN` flag to `false`', async () => {
    const dataSetId = await withDataSet(env, {
      withCDN: true,
      serviceProviderId: '1',
      payerAddress: '0xPayerAddress',
    })
    const req = new Request('https://host/fwss/cdn-service-terminated', {
      method: 'POST',
      headers: {
        [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
      },
      body: JSON.stringify({
        data_set_id: dataSetId,
      }),
    })
    const res = await workerImpl.fetch(req, env)
    expect(res.status).toBe(200)
    expect(await res.text()).toBe('OK')

    const { results: dataSets } = await env.DB.prepare(
      'SELECT id, with_cdn FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .all()
    expect(dataSets).toStrictEqual([{ id: dataSetId, with_cdn: 0 }])
  })

  it('clears index cache entries', async () => {
    const payerAddress = '0xPayerAddress'
    const dataSetId = await withDataSet(env, {
      withCDN: true,
      serviceProviderId: '1',
      payerAddress,
    })
    const pieceIds = [randomId(), randomId()]
    const pieceCids = [randomId(), randomId()]
    await withPieces(env, dataSetId, pieceIds, pieceCids)
    for (const pieceCid of pieceCids) {
      await env.KV.put(
        `${payerAddress}/${pieceCid}`,
        JSON.stringify([dataSetId, 'https://service.url/'])
      )
    }
    const req = new Request('https://host/fwss/cdn-service-terminated', {
      method: 'POST',
      headers: {
        [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
      },
      body: JSON.stringify({
        data_set_id: dataSetId,
      }),
    })
    const res = await workerImpl.fetch(req, env)
    expect(res.status).toBe(200)
    
    for (const pieceCid of pieceCids) {
      expect(await env.KV.get(`${payerAddress}/${pieceCid}`)).toBe(null)
    }
  })
})

describe('POST /fwss/service-terminated', () => {
  beforeEach(async () => {
    await env.DB.exec('DELETE FROM data_sets')
  })

  it('returns 400 if data_set_id is missing', async () => {
    const req = new Request('https://host/fwss/service-terminated', {
      method: 'POST',
      headers: {
        [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
      },
      body: JSON.stringify({}),
    })
    const res = await workerImpl.fetch(req, env)
    expect(res.status).toBe(400)
    expect(await res.text()).toBe('Bad Request')
  })

  it('sets `withCDN` flag to `false`', async () => {
    const dataSetId = await withDataSet(env, {
      withCDN: true,
      serviceProviderId: '1',
      payerAddress: '0xPayerAddress',
    })
    const req = new Request('https://host/fwss/service-terminated', {
      method: 'POST',
      headers: {
        [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
      },
      body: JSON.stringify({
        data_set_id: dataSetId,
      }),
    })
    const res = await workerImpl.fetch(req, env)
    expect(res.status).toBe(200)
    expect(await res.text()).toBe('OK')

    const { results: dataSets } = await env.DB.prepare(
      'SELECT id, with_cdn FROM data_sets WHERE id = ?',
    )
      .bind(dataSetId)
      .all()
    expect(dataSets).toStrictEqual([{ id: dataSetId, with_cdn: 0 }])
  })

  it('clears index cache entries', async () => {
    const payerAddress = '0xPayerAddress'
    const dataSetId = await withDataSet(env, {
      withCDN: true,
      serviceProviderId: '1',
      payerAddress,
    })
    const pieceIds = [randomId(), randomId()]
    const pieceCids = [randomId(), randomId()]
    await withPieces(env, dataSetId, pieceIds, pieceCids)
    for (const pieceCid of pieceCids) {
      await env.KV.put(
        `${payerAddress}/${pieceCid}`,
        JSON.stringify([dataSetId, 'https://service.url/'])
      )
    }
    const req = new Request('https://host/fwss/service-terminated', {
      method: 'POST',
      headers: {
        [env.SECRET_HEADER_KEY]: env.SECRET_HEADER_VALUE,
      },
      body: JSON.stringify({
        data_set_id: dataSetId,
      }),
    })
    const res = await workerImpl.fetch(req, env)
    expect(res.status).toBe(200)
    
    for (const pieceCid of pieceCids) {
      expect(await env.KV.get(`${payerAddress}/${pieceCid}`)).toBe(null)
    }
  })
})

async function withDataSet(
  env,
  {
    dataSetId = randomId(),
    withCDN = true,
    withIPFSIndexing = false,
    serviceProviderId,
    payerAddress,
  },
) {
  await env.DB.prepare(
    `
    INSERT INTO data_sets (
      id,
      with_cdn,
      with_ipfs_indexing,
      service_provider_id,
      payer_address
    )
    VALUES (?, ?, ?, ?, ?)`,
  )
    .bind(
      String(dataSetId),
      withCDN,
      withIPFSIndexing,
      serviceProviderId,
      payerAddress,
    )
    .run()

  return dataSetId
}
