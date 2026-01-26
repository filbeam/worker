# Worker Analytics

FilBeam uses Cloudflare Analytics Engine to collect telemetry from all workers via the `tail-handler` worker.

## Goldsky Subgraph Stats

The `indexer` worker writes Goldsky subgraph status to a separate Analytics Engine dataset. This allows monitoring of the subgraph's health and indexing progress.

### Dataset Configuration

| Environment   | Dataset Name                        |
| ------------- | ----------------------------------- |
| `dev`         | `filbeam_goldsky_stats_dev`         |
| `calibration` | `filbeam_goldsky_stats_calibration` |
| `mainnet`     | `filbeam_goldsky_stats_mainnet`     |

### Schema

| Field        | Type   | Description                       |
| ------------ | ------ | --------------------------------- |
| `doubles[0]` | number | Latest indexed block number       |
| `doubles[1]` | number | Has indexing errors (1=yes, 0=no) |

### Example Queries

#### Latest indexed block over time

```sql
SELECT
  toStartOfMinute(timestamp) AS minute,
  MAX(double1) AS latest_block
FROM filbeam_goldsky_stats_mainnet
WHERE timestamp > NOW() - INTERVAL '1' HOUR
GROUP BY minute
ORDER BY minute DESC
```

#### Indexing error rate

```sql
SELECT
  toStartOfHour(timestamp) AS hour,
  COUNT(*) AS checks,
  SUM(double2) AS errors,
  SUM(double2) / COUNT(*) AS error_rate
FROM filbeam_goldsky_stats_mainnet
WHERE timestamp > NOW() - INTERVAL '24' HOUR
GROUP BY hour
ORDER BY hour DESC
```
