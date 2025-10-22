# Analytics Writer

Cloudflare Worker that receives TTFB (Time To First Byte) metrics from the FilBeam bot and writes them to Analytics Engine.

## API

**POST /** - Send TTFB data

**Authentication Required:** Include the `X-Analytics-Auth` header with your pre-shared key.

**Headers:**
```
X-Analytics-Auth: your-secret-key
Content-Type: application/json
```

**Request Body:**
```json
{
  "blobs": ["url", "location", "client", "cid"],
  "doubles": [ttfb, status, bytes],
  "indexes": ["optional-index-string"]
}
```

**Note:** Use either `index` (string) or `indexes` (array with max 1 item), not both.

**Response:**
```json
{"success": true}
```

**Error Response:**
```json
{
  "success": false,
  "error": "Error message"
}
```

## Data Structure

Each data point consists of:

- **Blobs** (strings) — Dimensions used for grouping and filtering
- **Doubles** (numbers) — Numeric values to record  
- **Indexes** (strings) — Sampling key (single index only)

### Analytics Engine Limits

- **Blobs**: Maximum 20 items, total size must not exceed 16 KB
- **Doubles**: Maximum 20 items
- **Indexes**: Maximum 1 item, each index must not exceed 96 bytes
- **Data Points**: Maximum 25 data points per Worker invocation

### Field Details

- **blobs**: Array of strings containing dimensions for grouping and filtering:
  - `url`: The URL that was requested
  - `location`: Geographic location of the request
  - `client`: Client identifier
  - `cid`: Content identifier

- **doubles**: Array of numbers containing numeric values:
  - `ttfb`: Time to first byte in milliseconds
  - `status`: HTTP status code
  - `bytes`: Number of bytes transferred

- **indexes**: Optional array with at most 1 item for sampling key

## Development

### Set up authentication

For local development, create a `.dev.vars` file in the `analytics-writer` directory:
```bash
echo "ANALYTICS_AUTH_KEY=your-local-dev-key" > .dev.vars
```

### Run locally
```bash
npm start
```

### Run tests
```bash
npm test
```

### Deploy

Set the authentication key as a secret (one time for each environment):
```bash
wrangler secret put ANALYTICS_AUTH_KEY --env calibration
wrangler secret put ANALYTICS_AUTH_KEY --env mainnet
```

Deploy to calibration:
```bash
npm run deploy:calibration
```

Deploy to mainnet:
```bash
npm run deploy:mainnet
```

## Configuration

The worker uses Cloudflare Analytics Engine with the dataset `ttfb_metrics`. The binding `analytics_engine` is configured in `wrangler.toml`.

## Error Handling

The worker includes comprehensive error handling:
- Validates authentication header (401 if missing or incorrect)
- Validates request method (POST only, returns 405)
- Validates JSON payload structure
- Validates array lengths for blobs and doubles
- Returns appropriate HTTP status codes
- Logs server errors (5xx) to console

## Security

The worker uses header-based authentication with a pre-shared key stored as a Cloudflare secret. All requests must include the `X-Analytics-Auth` header with the correct key value to access the API.
