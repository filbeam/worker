/**
 * @type {{
 *   serviceProviderId: string
 *   serviceUrl: string
 *   ipfsRootCid: string
 *   dataSetId: number
 * }[]}
 */
export const CONTENT_STORED_ON_CALIBRATION = [
  {
    // This Piece must have IPFS RootCID set and IPFS Indexing enabled at the dataset level
    serviceProviderId: '2',
    serviceUrl: 'https://calibnet.pspsps.io/',
    pieceCid:
      'bafkzcibdqqwat4m7ymdhkvsbbo5m7jsejchayo75udw6v3qlfgofpz2lbppe7ea7',
    ipfsRootCid: 'bafk4todo',
    dataSetId: 9,
  },
  {
    serviceProviderId: '3',
    serviceUrl: 'https://calib.ezpdpz.net/',
    pieceCid:
      'bafkzcibdtrjavqxb56hzzq2tyayggqtujzamyf227cg4evbillgsfcdurht3cwyb',
    ipfsRootCid: null,
    dataSetId: 12,
  },
]
