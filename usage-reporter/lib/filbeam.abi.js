export default [
  {
    inputs: [
      {
        internalType: 'uint256',
        name: 'toEpoch',
        type: 'uint256',
      },
      {
        internalType: 'uint256[]',
        name: 'dataSetIds',
        type: 'uint256[]',
      },
      {
        internalType: 'uint256[]',
        name: 'cdnBytesUsed',
        type: 'uint256[]',
      },
      {
        internalType: 'uint256[]',
        name: 'cacheMissBytesUsed',
        type: 'uint256[]',
      },
    ],
    stateMutability: 'nonpayable',
    type: 'function',
    name: 'recordUsageRollups',
  },
]
