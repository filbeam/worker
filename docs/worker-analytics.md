# Worker Analytics

FilBeam uses Cloudflare Analytics Engine to collect telemetry from workers. Query the data using the [Cloudflare GraphQL Analytics API](https://developers.cloudflare.com/analytics/graphql-api/) or the dashboard.

## Adding a New Dataset

When adding a new dataset, follow this structure:

- `## dataset_name` - second-level heading (without `filbeam_` prefix)
  - `### Configuration` - binding and environment-specific dataset names
  - `### Schema` - fields table
  - `### Example Queries` - with individual queries as `####` headings

## retrieval_stats

Basic performance metrics for all workers, collected via the `tail-handler` [Tail Worker](https://developers.cloudflare.com/workers/observability/logs/tail-workers/).

### Configuration

Analytics are written to the `RETRIEVAL_STATS` binding, which maps to environment-specific datasets:

| Environment   | Dataset Name                          |
| ------------- | ------------------------------------- |
| `dev`         | `filbeam_retrieval_stats_dev`         |
| `calibration` | `filbeam_retrieval_stats_calibration` |
| `mainnet`     | `filbeam_retrieval_stats_mainnet`     |

### Schema

Each data point contains the following fields:

| Field        | Type   | Description                                                                                         |
| ------------ | ------ | --------------------------------------------------------------------------------------------------- |
| `indexes[0]` | string | Service name. Extracted from `cf:service=<name>` script tag, or falls back to `scriptName`          |
| `doubles[0]` | number | Wall time in milliseconds. Total elapsed time from request start to response completion             |
| `doubles[1]` | number | CPU time in milliseconds. Actual CPU execution time consumed by the worker                          |
| `doubles[2]` | number | Response status. HTTP status code from fetch events, or `0` if not available (e.g., scheduled jobs) |
| `blobs[0]`   | string | Outcome. Execution result: `"ok"`, `"exception"`, `"exceededCpu"`, `"exceededMemory"`, etc.         |

### Example Queries

#### Average response time by service

```sql
SELECT
  index1 AS service,
  AVG(double1) AS avg_wall_time_ms,
  AVG(double2) AS avg_cpu_time_ms
FROM filbeam_retrieval_stats_mainnet
WHERE timestamp > NOW() - INTERVAL '1' HOUR
GROUP BY index1
ORDER BY avg_wall_time_ms DESC
```

#### Error rate by service

```sql
SELECT
  index1 AS service,
  COUNT(*) AS total,
  SUM(IF(blob1 != 'ok', 1, 0)) AS errors,
  SUM(IF(blob1 != 'ok', 1, 0)) * 100.0 / COUNT(*) AS error_rate
FROM filbeam_retrieval_stats_mainnet
WHERE timestamp > NOW() - INTERVAL '1' HOUR
GROUP BY index1
ORDER BY error_rate DESC
```

#### Response status distribution

```sql
SELECT
  index1 AS service,
  double3 AS status_code,
  COUNT(*) AS count
FROM filbeam_retrieval_stats_mainnet
WHERE timestamp > NOW() - INTERVAL '1' HOUR
  AND double3 > 0
GROUP BY index1, double3
ORDER BY index1, count DESC
```

#### P95 wall time by service

```sql
SELECT
  index1 AS service,
  QUANTILE(double1, 0.95) AS p95_wall_time_ms
FROM filbeam_retrieval_stats_mainnet
WHERE timestamp > NOW() - INTERVAL '1' HOUR
GROUP BY index1
ORDER BY p95_wall_time_ms DESC
```
