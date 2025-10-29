/**
 * @type {{
 *   serviceProviderId: string
 *   serviceUrl: string
 *   ipfsRootCid: string
 *   dataSetId: number
 *   pieceId: string
 * }[]}
 */
export const CONTENT_STORED_ON_CALIBRATION = [
  {
    // This Piece must have IPFS RootCID set and IPFS Indexing enabled at the dataset level
    serviceProviderId: '23',
    serviceUrl: 'https://pdp.oplian.com/',
    pieceCid:
      'bafkzcibe2g5acdgp624n6qglofslq4dl2aixoeecjsqiqzcbk4ji6vpmyvcr2pytaq',
    ipfsRootCid: 'bafybeidt6ugk5xeoeeumev3eexamnjxvexbfpfajx4kgzgsa5hkrwlhavu',
    dataSetId: 845,
    pieceId: '0',
  },
  {
    serviceProviderId: '3',
    serviceUrl: 'https://calib.ezpdpz.net/',
    pieceCid:
      'bafkzcibdtrjavqxb56hzzq2tyayggqtujzamyf227cg4evbillgsfcdurht3cwyb',
    ipfsRootCid: null,
    dataSetId: 12,
    pieceId: '2',
  },
]
