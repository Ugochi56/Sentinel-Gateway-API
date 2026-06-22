# Sentinel Gateway

Unbreakable webhook middleware. Catches incoming webhook payloads, stores them in Supabase, and retries delivery with exponential backoff until the client server recovers.

## Architecture

```
Provider (Stripe/GitHub/etc.)
        │
        │  POST /hooks/{endpoint_id}
        ▼
  Sentinel Gateway (Render)
        │
        ├── Persists raw body + headers to Supabase immediately
        └── Returns 200 OK in <100ms
        
  Background Worker (same process, asyncio task)
        │
        └── Polls Supabase every 10s
            ├── Delivers to client destination_url
            ├── On success: status = delivered
            └── On failure: exponential backoff → max 5 retries → failed_permanently
```

## Backoff Schedule

| Retry | Wait    | Cumulative |
|-------|---------|------------|
| 1     | 1 min   | 1 min      |
| 2     | 2 min   | 3 min      |
| 3     | 4 min   | 7 min      |
| 4     | 8 min   | 15 min     |
| 5     | 16 min  | 31 min     |
| 6     | 32 min  | 63 min → **failed_permanently** |

## Setup

### 1. Supabase

1. Create a new Supabase project at https://supabase.com
2. Open the SQL Editor and run `sql/schema.sql`
3. Copy your project URL and `anon` key (or `service_role` key for production)

### 2. Local development

```bash
git clone <repo>
cd sentinel-gateway
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your Supabase credentials
uvicorn app.main:app --reload
```

API docs available at http://localhost:8000/docs

### 3. Deploy to Render

1. Push to GitHub
2. In Render dashboard: New → Web Service → connect repo
3. Render auto-detects `render.yaml`
4. Set the four secret env vars in the Render dashboard:
   - `SUPABASE_URL`
   - `SUPABASE_KEY`
   - `RAPIDAPI_PROXY_SECRET`
   - `BASE_URL` (your Render URL, e.g. `https://sentinel-gateway.onrender.com`)

### 4. UptimeRobot

Configure a monitor to ping `https://sentinel-gateway.onrender.com/health` every 5 minutes.
This prevents Render's free tier from sleeping and also keeps the asyncio worker alive.

### 5. RapidAPI

1. Create an API listing on RapidAPI
2. Set your Render URL as the base URL
3. Copy the Proxy Secret from Settings → Security into `RAPIDAPI_PROXY_SECRET`
4. Expose these endpoints:
   - `POST /v1/register`
   - `GET /v1/logs`
   - `GET /v1/logs/{event_id}`
   - (hide `/hooks/*` from the listing — clients get these URLs from `/v1/register`)

## API Reference

### POST /v1/register
Register a destination URL and receive a unique proxy URL to give to webhook providers.

**Headers:**
- `X-RapidAPI-Proxy-Secret`: your proxy secret
- `X-RapidAPI-User`: injected by RapidAPI

**Body:**
```json
{
  "destination_url": "https://your-server.com/webhooks",
  "label": "my-stripe-endpoint"
}
```

**Response:**
```json
{
  "endpoint_id": "uuid",
  "proxy_url": "https://sentinel-gateway.onrender.com/hooks/uuid",
  "destination_url": "https://your-server.com/webhooks",
  "label": "my-stripe-endpoint"
}
```

### POST /hooks/{endpoint_id}
The proxy URL you give to Stripe, GitHub, Shopify, etc. No auth required — secured by UUID entropy.

### GET /v1/logs
List webhook events. Excludes raw payload body to save bandwidth.

**Query params:** `status`, `endpoint_id`, `limit` (max 200), `offset`

### GET /v1/logs/{event_id}
Full event detail including payload and raw body.

### GET /health
UptimeRobot ping target. Returns `{"status": "ok"}`.

## Security Notes

- **Raw body preservation**: stored as `TEXT`, never re-serialised, so HMAC signatures (Stripe, GitHub) remain verifiable by the client.
- **Header forwarding**: all original provider headers are captured and forwarded to the destination.
- **HTTPS enforcement**: outbound delivery rejects non-HTTPS destinations with no retry.
- **Deduplication**: `provider_event_id` unique constraint per endpoint prevents double-delivery when providers retry.
- **Payload size limit**: 1 MB hard cap at ingestion.
- **DB cleanup**: delivered rows older than 7 days are auto-purged every ~1 hour to protect Supabase's 500 MB free cap.

