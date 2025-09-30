## Off-Chain Worker Implementation

### Rollup Worker

#### Responsibilities

- **Data Aggregation**: Sum usage data by dataset and type (CDN, cache miss)
- **Periodic Reporting**: Periodically submit usage reports to FilBeam (Operator) contract
- **State Tracking**: Maintain record of max reported epoch per dataset

#### Implementation Details

- **Scheduling**: Use Cloudflare Workers' scheduled events to trigger rollup process
- **Finding Current Epoch**: Query chain for current block number (epoch) using viem
- **Query Logic**: Select retrieval logs grouped by data set ID from last reported epoch to previous epoch
- **Filtering**: Skip datasets with zero usage to lower gas fees for contract calls
- **On-Chain Interaction**: Submit aggregated usage data to FilBeam contract using viem

#### Timestamp to Epoch conversion

To find epoch for a given timestamp, the following formula is used:

```
Filecoin_Epoch = floor((Unix_Timestamp - 1598306400) / 30)
```

Where:

- `1598306400` is the Unix timestamp for the Filecoin genesis block
