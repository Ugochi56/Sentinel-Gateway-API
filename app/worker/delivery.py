"""
Sentinel Delivery Worker
------------------------
Runs as an asyncio background task inside the FastAPI process.
No separate Render worker service required (free-tier compatible).

Lifecycle:
  1. poll_and_deliver() is called every WORKER_POLL_INTERVAL_SECONDS.
  2. It fetches up to WORKER_BATCH_SIZE pending events where next_retry_at <= NOW().
  3. Each event is delivered concurrently (asyncio.gather), limited by a semaphore.
  4. Success  → status = 'delivered'.
  5. Failure  → retry_count++, next_retry_at = backoff(retry_count).
  6. Exceeded → status = 'failed_permanently'.

Every CLEANUP_INTERVAL_TICKS ticks, old delivered rows are purged
to protect the Supabase free-tier 500 MB cap.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import httpx

from app.cache import get_endpoint
from app.config import settings
from app.database import get_db
from app.utils.backoff import calculate_next_retry
from app.utils.tiers import get_tier_limits, TIERS

logger = logging.getLogger(__name__)

CLEANUP_INTERVAL_TICKS = 360  # run cleanup every ~1 hour (360 × 10s)
MAX_CONCURRENT_DELIVERIES = 5  # don't overwhelm a struggling destination

_tick_count = 0
_http_client: httpx.AsyncClient | None = None
_delivery_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DELIVERIES)


# ──────────────────────────────────────────────────────────────────────────────
# HTTP client lifecycle
# ──────────────────────────────────────────────────────────────────────────────


async def _get_http_client() -> httpx.AsyncClient:
    """Return the shared, long-lived httpx client (connection-pooled)."""
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            follow_redirects=False,   # Don't silently follow; HTTP→HTTPS redirects hide misconfig.
            timeout=settings.DELIVERY_TIMEOUT_SECONDS,
            verify=True,              # Enforce valid TLS cert on destination.
        )
    return _http_client


async def _close_http_client() -> None:
    global _http_client
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()
        _http_client = None


# ──────────────────────────────────────────────────────────────────────────────
# Status helpers
# ──────────────────────────────────────────────────────────────────────────────


async def _mark_delivered(event_id: str) -> None:
    db = get_db()
    await asyncio.to_thread(
        lambda: db.table("webhook_events")
        .update(
            {
                "status": "delivered",
                "delivered_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        .eq("id", event_id)
        .execute()
    )


async def _mark_permanently_failed(event_id: str) -> None:
    db = get_db()
    await asyncio.to_thread(
        lambda: db.table("webhook_events")
        .update({"status": "failed_permanently"})
        .eq("id", event_id)
        .execute()
    )


async def _schedule_retry(event_id: str, new_retry_count: int) -> None:
    next_retry = calculate_next_retry(new_retry_count)
    db = get_db()
    await asyncio.to_thread(
        lambda: db.table("webhook_events")
        .update(
            {
                "retry_count": new_retry_count,
                "next_retry_at": next_retry.isoformat(),
            }
        )
        .eq("id", event_id)
        .execute()
    )
    logger.info(
        f"Event {event_id} retry {new_retry_count} scheduled at {next_retry.isoformat()}"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Core delivery
# ──────────────────────────────────────────────────────────────────────────────


async def _deliver_event(event: dict) -> None:
    """
    Attempt HTTP delivery of a single event.
    Updates DB status on both success and failure.
    Wrapped by a semaphore to cap concurrency.
    """
    async with _delivery_semaphore:
        await _deliver_event_inner(event)


async def _deliver_event_inner(event: dict) -> None:
    event_id = event["id"]

    # Fetch destination from cache (no DB round-trip).
    ep = await get_endpoint(event["endpoint_id"])

    if not ep:
        logger.error(f"Event {event_id}: endpoint not found, marking permanently failed.")
        await _mark_permanently_failed(event_id)
        return

    destination_url: str = ep["destination_url"]

    # Reject non-HTTPS destinations immediately (except for localhost/127.0.0.1 in local development).
    is_local = destination_url.startswith("http://localhost") or destination_url.startswith("http://127.0.0.1")
    if not (destination_url.startswith("https://") or is_local):
        logger.error(
            f"Event {event_id}: non-HTTPS destination {destination_url!r}, "
            "marking permanently failed."
        )
        await _mark_permanently_failed(event_id)
        return

    # Build forwarded headers.
    strip = frozenset(["content-length", "host", "transfer-encoding", "connection"])
    forward_headers = {
        k: v
        for k, v in (event.get("headers") or {}).items()
        if k.lower() not in strip
    }
    # Sentinel metadata headers (let the destination know this is a forwarded delivery).
    forward_headers["x-sentinel-event-id"] = event_id
    forward_headers["x-sentinel-retry-count"] = str(event["retry_count"])
    forward_headers["x-sentinel-timestamp"] = datetime.now(timezone.utc).isoformat()

    success = False
    client = await _get_http_client()
    try:
        response = await client.post(
            destination_url,
            content=event["raw_body"].encode("utf-8"),
            headers=forward_headers,
        )

        if 200 <= response.status_code < 300:
            success = True
            logger.info(f"Event {event_id} delivered → {response.status_code}")
        else:
            logger.warning(
                f"Event {event_id} destination returned {response.status_code}"
            )

    except httpx.TimeoutException:
        logger.warning(
            f"Event {event_id} timed out after {settings.DELIVERY_TIMEOUT_SECONDS}s"
        )
    except httpx.ConnectError as exc:
        logger.warning(f"Event {event_id} connection error: {exc}")
    except httpx.SSLError as exc:
        logger.warning(f"Event {event_id} TLS error: {exc}")
    except Exception as exc:
        logger.error(f"Event {event_id} unexpected error: {exc}", exc_info=True)

    if success:
        await _mark_delivered(event_id)
    else:
        plan = ep.get("plan", "Free") if ep else "Free"
        limits = get_tier_limits(plan)
        new_retry_count = event["retry_count"] + 1
        if new_retry_count > limits.max_retries:
            logger.warning(
                f"Event {event_id} exceeded plan '{plan}' max retries ({limits.max_retries}) → failed_permanently"
            )
            await _mark_permanently_failed(event_id)
        else:
            await _schedule_retry(event_id, new_retry_count)


# ──────────────────────────────────────────────────────────────────────────────
# Polling + cleanup
# ──────────────────────────────────────────────────────────────────────────────


async def _poll_and_deliver() -> None:
    """Fetch due events and deliver them concurrently."""
    db = get_db()
    now = datetime.now(timezone.utc).isoformat()

    result = await asyncio.to_thread(
        lambda: db.table("webhook_events")
        .select("id, endpoint_id, raw_body, headers, retry_count")
        .eq("status", "pending")
        .lte("next_retry_at", now)
        .order("next_retry_at")
        .limit(settings.WORKER_BATCH_SIZE)
        .execute()
    )

    events = result.data or []
    if not events:
        return

    logger.info(f"Worker: processing {len(events)} event(s)")
    await asyncio.gather(*[_deliver_event(e) for e in events])


async def _cleanup_old_delivered() -> None:
    """
    Delete delivered rows older than each plan's retention period.
    Keeps the Supabase free-tier storage from bloating.
    """
    db = get_db()
    total_deleted = 0
    
    for plan_name, limits in TIERS.items():
        # 1. Fetch endpoint IDs belonging to this plan
        endpoints_res = await asyncio.to_thread(
            lambda: db.table("endpoints")
            .select("id")
            .eq("plan", plan_name)
            .execute()
        )
        ep_ids = [row["id"] for row in (endpoints_res.data or [])]
        if not ep_ids:
            continue
            
        # 2. Delete delivered events older than retention_days for these endpoints
        cutoff = (
            datetime.now(timezone.utc)
            - timedelta(days=limits.retention_days)
        ).isoformat()
        
        del_res = await asyncio.to_thread(
            lambda: db.table("webhook_events")
            .delete()
            .eq("status", "delivered")
            .lt("delivered_at", cutoff)
            .in_("endpoint_id", ep_ids)
            .execute()
        )
        total_deleted += len(del_res.data or [])
        
    if total_deleted:
        logger.info(f"Cleanup: removed {total_deleted} old delivered event(s)")


# ──────────────────────────────────────────────────────────────────────────────
# Entry point (spawned at FastAPI startup via lifespan)
# ──────────────────────────────────────────────────────────────────────────────


async def worker_loop() -> None:
    global _tick_count
    logger.info(
        f"Sentinel worker started — polling every {settings.WORKER_POLL_INTERVAL_SECONDS}s, "
        f"batch={settings.WORKER_BATCH_SIZE}, max_retries={settings.MAX_RETRIES}, "
        f"max_concurrent={MAX_CONCURRENT_DELIVERIES}"
    )

    try:
        while True:
            try:
                await _poll_and_deliver()
                _tick_count += 1
                if _tick_count % CLEANUP_INTERVAL_TICKS == 0:
                    await _cleanup_old_delivered()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"Worker loop unhandled error: {exc}", exc_info=True)

            await asyncio.sleep(settings.WORKER_POLL_INTERVAL_SECONDS)
    finally:
        logger.info("Worker loop shutting down — closing HTTP client.")
        await _close_http_client()
