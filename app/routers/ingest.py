import asyncio
import json
import logging

from fastapi import APIRouter, HTTPException, Path, Request, Response

from app.cache import get_endpoint
from app.config import settings
from app.database import get_db
from app.utils.tiers import get_tier_limits

logger = logging.getLogger(__name__)
router = APIRouter()

# Headers that commonly carry a unique event/delivery ID per provider.
# Checked in priority order — first match wins.
PROVIDER_ID_HEADERS = [
    "x-github-delivery",         # GitHub
    "x-shopify-webhook-id",      # Shopify
    "stripe-webhook-id",         # Stripe
    "x-webhook-id",              # Generic / HubSpot
    "webhook-id",                # Svix-based (Clerk, Resend, etc.)
    "x-gitlab-event-uuid",       # GitLab
    "linear-delivery",           # Linear
    "x-hook-uuid",               # Bitbucket
    "x-twilio-idempotency-token",  # Twilio
    "idempotency-key",           # Generic
]

# Headers we strip before forwarding to avoid confusing the destination.
STRIP_HEADERS = frozenset(
    [
        "host",
        "content-length",
        "transfer-encoding",
        "connection",
        "x-forwarded-for",
        "x-forwarded-proto",
        "x-real-ip",
    ]
)


def _extract_provider_event_id(headers: dict, raw_body: str) -> str | None:
    """
    Returns a stable unique ID for this event if one can be found.
    Checked in order: known headers → common JSON body fields.
    Returns None when no ID is found (dedup constraint is skipped).
    """
    for h in PROVIDER_ID_HEADERS:
        if h in headers:
            return headers[h]

    # Best-effort: look for id / event_id at the root of the JSON body.
    try:
        body = json.loads(raw_body)
        if isinstance(body, dict):
            for field in ("id", "event_id", "webhook_id", "uuid", "messageId"):
                val = body.get(field)
                if val and isinstance(val, str):
                    return val
    except (json.JSONDecodeError, TypeError):
        pass

    return None


@router.post(
    "/hooks/{endpoint_id}",
    status_code=200,
    summary="Webhook ingestion endpoint (give this URL to providers)",
)
async def ingest_webhook(
    endpoint_id: str = Path(..., description="UUID issued at registration"),
    request: Request = None,
):
    # --- 1. Verify endpoint exists (from in-memory cache — no DB round-trip) ---
    ep = await get_endpoint(endpoint_id)

    if not ep:
        # Return 200 silently — don't leak endpoint existence to scanners.
        logger.warning(f"Ingest hit unknown endpoint_id={endpoint_id}")
        return Response(status_code=200)

    user_api_key = ep["user_api_key"]
    plan = ep.get("plan", "Free")
    limits = get_tier_limits(plan)

    # --- 2. Size check (fast path before reading body) ---
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > limits.max_payload_bytes:
        limit_mb = limits.max_payload_bytes / (1024 * 1024)
        raise HTTPException(
            status_code=413,
            detail=f"Payload exceeds {limit_mb:.1f} MB limit for plan '{plan}'."
        )

    # --- 3. Read raw body (preserves bytes for HMAC verification) ---
    raw_body_bytes = await request.body()
    if len(raw_body_bytes) > limits.max_payload_bytes:
        limit_mb = limits.max_payload_bytes / (1024 * 1024)
        raise HTTPException(
            status_code=413,
            detail=f"Payload exceeds {limit_mb:.1f} MB limit for plan '{plan}'."
        )

    raw_body_str = raw_body_bytes.decode("utf-8", errors="replace")

    # --- 4. Capture and sanitise incoming headers ---
    headers_dict: dict[str, str] = {
        k.lower(): v
        for k, v in request.headers.items()
        if k.lower() not in STRIP_HEADERS
    }

    # --- 5. Best-effort JSON parse (for /v1/logs display only) ---
    payload = None
    try:
        payload = json.loads(raw_body_str)
    except (json.JSONDecodeError, ValueError):
        pass

    # --- 6. Extract provider event ID for deduplication ---
    provider_event_id = _extract_provider_event_id(headers_dict, raw_body_str)

    # --- 7. Persist to queue ---
    db = get_db()
    try:
        await asyncio.to_thread(
            lambda: db.table("webhook_events")
            .insert(
                {
                    "endpoint_id": endpoint_id,
                    "user_api_key": user_api_key,
                    "provider_event_id": provider_event_id,
                    "raw_body": raw_body_str,
                    "headers": headers_dict,
                    "payload": payload,
                    "status": "pending",
                }
            )
            .execute()
        )
        logger.info(
            f"Queued event endpoint={endpoint_id} "
            f"provider_event_id={provider_event_id or 'none'} "
            f"size={len(raw_body_bytes)}b"
        )
    except Exception as exc:
        err_str = str(exc).lower()
        # Unique constraint violation → duplicate event, safe to ack.
        if "23505" in err_str or "unique" in err_str:
            logger.info(f"Duplicate event ignored: provider_event_id={provider_event_id}")
            return Response(status_code=200)
        logger.error(f"DB insert error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to queue webhook.")

    # --- 8. Return 200 immediately (provider does not wait for delivery) ---
    return Response(status_code=200)
