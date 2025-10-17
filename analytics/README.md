# Analytics Worker

Cloudflare Worker that receives TTFB (Time To First Byte) metrics from the FilBeam bot and stores them in Analytics Engine.

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

### Set up authentication

For local development, create a `.dev.vars` file in the `analytics` directory:
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

The worker uses Cloudflare Analytics Engine with the dataset `ttfb_metrics`. The binding `analytics-engine` is configured in `wrangler.toml`.

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
