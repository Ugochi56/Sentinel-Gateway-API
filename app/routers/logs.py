import asyncio
import logging
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, Depends, Header, HTTPException, Query

from app.database import get_db
from app.utils.security import verify_proxy_secret

logger = logging.getLogger(__name__)
router = APIRouter()

ValidStatus = Literal["pending", "delivered", "failed_permanently"]


@router.get(
    "/v1/logs",
    dependencies=[Depends(verify_proxy_secret)],
    summary="List webhook events for the authenticated user",
)
async def get_logs(
    x_rapidapi_user: str = Header(...),
    status: ValidStatus | None = Query(None, description="Filter by delivery status"),
    endpoint_id: str | None = Query(None, description="Filter by registered endpoint UUID"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    db = get_db()

    # Intentionally exclude raw_body and full payload from list response
    # to save bandwidth. Client can fetch the full record separately if needed.
    query = (
        db.table("webhook_events")
        .select(
            "id, endpoint_id, provider_event_id, headers, "
            "status, retry_count, next_retry_at, created_at, delivered_at"
        )
        .eq("user_api_key", x_rapidapi_user)
        .order("created_at", desc=True)
        .limit(limit)
        .offset(offset)
    )

    if status:
        query = query.eq("status", status)

    if endpoint_id:
        query = query.eq("endpoint_id", endpoint_id)

    result = await asyncio.to_thread(lambda: query.execute())

    return {
        "count": len(result.data),
        "offset": offset,
        "limit": limit,
        "events": result.data,
    }


@router.get(
    "/v1/logs/{event_id}",
    dependencies=[Depends(verify_proxy_secret)],
    summary="Fetch full details (including payload) for a single event",
)
async def get_event(
    event_id: str,
    x_rapidapi_user: str = Header(...),
):
    db = get_db()

    result = await asyncio.to_thread(
        lambda: db.table("webhook_events")
        .select("*")
        .eq("id", event_id)
        .eq("user_api_key", x_rapidapi_user)
        .limit(1)
        .execute()
    )

    if not result.data:
        raise HTTPException(status_code=404, detail="Event not found.")

    return result.data[0]


@router.post(
    "/v1/logs/{event_id}/retry",
    dependencies=[Depends(verify_proxy_secret)],
    summary="Retry a failed webhook event",
    description=(
        "Resets a failed_permanently event back to pending so the "
        "delivery worker picks it up again. Also works on pending events "
        "to force an immediate retry."
    ),
)
async def retry_event(
    event_id: str,
    x_rapidapi_user: str = Header(...),
):
    db = get_db()

    result = await asyncio.to_thread(
        lambda: db.table("webhook_events")
        .select("id, status")
        .eq("id", event_id)
        .eq("user_api_key", x_rapidapi_user)
        .limit(1)
        .execute()
    )

    if not result.data:
        raise HTTPException(status_code=404, detail="Event not found.")

    event = result.data[0]
    if event["status"] == "delivered":
        raise HTTPException(
            status_code=400, detail="Event already delivered successfully."
        )

    await asyncio.to_thread(
        lambda: db.table("webhook_events")
        .update(
            {
                "status": "pending",
                "retry_count": 0,
                "next_retry_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        .eq("id", event_id)
        .execute()
    )

    logger.info(f"Event {event_id} reset to pending for retry")
    return {"status": "pending", "event_id": event_id}


@router.post(
    "/v1/test/fast-forward/{event_id}",
    dependencies=[Depends(verify_proxy_secret)],
    summary="Backdoor endpoint to fast-forward an event's retry clock (test only)",
)
async def test_fast_forward_event(
    event_id: str,
    x_rapidapi_user: str = Header(...),
):
    db = get_db()
    await asyncio.to_thread(
        lambda: db.table("webhook_events")
        .update({"next_retry_at": "2020-01-01T00:00:00+00:00"})
        .eq("id", event_id)
        .eq("user_api_key", x_rapidapi_user)
        .execute()
    )
    return {"status": "fast-forwarded", "event_id": event_id}

