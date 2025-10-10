# Analytics Worker

Cloudflare Worker that receives TTFB (Time To First Byte) metrics from the FilBeam bot and stores them in Analytics Engine.

## API

**POST /** - Send TTFB data

```json
{
  "blobs": ["url", "location", "client", "cid"],
  "doubles": [ttfb, status, bytes]
}
```

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

- **blobs**: Array of 4 strings containing:
  - `url`: The URL that was requested
  - `location`: Geographic location of the request
  - `client`: Client identifier
  - `cid`: Content identifier

- **doubles**: Array of 3 numbers containing:
  - `ttfb`: Time to first byte in milliseconds
  - `status`: HTTP status code
  - `bytes`: Number of bytes transferred

## Development

### Run locally
```bash
npm start
```

### Run tests
```bash
npm test
```

### Deploy

#### Deploy to Dev Environment
```bash
npm run deploy:dev
```

#### Deploy to Calibration
```bash
npm run deploy:calibration
```

#### Deploy to Mainnet
```bash
npm run deploy:mainnet
```

## Configuration

The worker uses Cloudflare Analytics Engine with the dataset `ttfb_metrics`. The binding `analytics-engine` is configured in `wrangler.toml`.

## Error Handling

The worker includes comprehensive error handling:
- Validates request method (POST only)
- Validates JSON payload structure
- Validates array lengths for blobs and doubles
- Returns appropriate HTTP status codes
- Logs server errors (5xx) to console
