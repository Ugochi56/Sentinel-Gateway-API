"""
In-memory endpoint cache with TTL.

Eliminates per-request DB lookups on the ingestion hot path
and in the delivery worker. The endpoints table is small and
rarely changes, so a 60-second TTL is safe.
"""

import asyncio
import logging
import time

from app.database import get_db

logger = logging.getLogger(__name__)

CACHE_TTL_SECONDS = 60

_endpoint_cache: dict[str, dict] = {}
_cache_last_refresh: float = 0
_cache_lock = asyncio.Lock()


async def _refresh_cache() -> None:
    global _endpoint_cache, _cache_last_refresh
    db = get_db()
    result = await asyncio.to_thread(
        lambda: db.table("endpoints")
        .select("id, user_api_key, destination_url")
        .execute()
    )
    _endpoint_cache = {
        row["id"]: {
            "user_api_key": row["user_api_key"],
            "destination_url": row["destination_url"],
        }
        for row in (result.data or [])
    }
    _cache_last_refresh = time.monotonic()
    logger.debug(f"Endpoint cache refreshed: {len(_endpoint_cache)} endpoints")


async def get_endpoint(endpoint_id: str) -> dict | None:
    """
    Returns {"user_api_key": ..., "destination_url": ...} or None.
    Auto-refreshes from Supabase every CACHE_TTL_SECONDS.
    """
    if time.monotonic() - _cache_last_refresh > CACHE_TTL_SECONDS:
        async with _cache_lock:
            # Double-check after acquiring lock to avoid thundering herd.
            if time.monotonic() - _cache_last_refresh > CACHE_TTL_SECONDS:
                await _refresh_cache()
    return _endpoint_cache.get(endpoint_id)


def invalidate_cache() -> None:
    """Force a cache refresh on next access (call after register/update/delete)."""
    global _cache_last_refresh
    _cache_last_refresh = 0
