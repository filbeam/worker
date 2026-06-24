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
    serviceProviderId: '2',
    serviceUrl: 'https://calib2.ezpdpz.net/',
    pieceCid:
      'bafkzcibdzabqtx4ovk72zspicej5vmbjse2237cfzduljnevpmd4kfvccb5h44y4',
    ipfsRootCid: 'bafkreiheygfzn22dfeos3xoay5cxnfb464znd2rszieyzcinlsgu2z7kau',
    dataSetId: 14578,
    pieceId: '0',
  },
  {
    serviceProviderId: '4',
    serviceUrl: 'https://caliberation-pdp.infrafolio.com/',
    pieceCid:
      'bafkzcibd7r7avok5z3tdn4uq6shuqghxm75e5jgirvdzsdmrkly2u5dldodic4jb',
    ipfsRootCid: 'bafkreigo55ody3xm4g6mbkitgdytcshanhluzywpi25f3qalfypc7bpna4',
    dataSetId: 14577,
    pieceId: '1',
  },
]
