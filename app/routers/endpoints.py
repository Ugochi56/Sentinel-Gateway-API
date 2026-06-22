"""
Endpoint management — list, update, delete registered webhook endpoints.
"""

import asyncio
import logging

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, field_validator

from app.cache import invalidate_cache
from app.database import get_db
from app.utils.security import verify_proxy_secret

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get(
    "/v1/endpoints",
    dependencies=[Depends(verify_proxy_secret)],
    summary="List registered webhook endpoints",
)
async def list_endpoints(
    x_rapidapi_user: str = Header(..., description="Injected by RapidAPI gateway"),
):
    db = get_db()
    result = await asyncio.to_thread(
        lambda: db.table("endpoints")
        .select("id, destination_url, label, created_at")
        .eq("user_api_key", x_rapidapi_user)
        .order("created_at", desc=True)
        .execute()
    )
    return {"endpoints": result.data or []}


class UpdateEndpointRequest(BaseModel):
    destination_url: str | None = None
    label: str | None = None

    @field_validator("destination_url")
    @classmethod
    def must_be_https(cls, v: str | None) -> str | None:
        if v is not None and not v.startswith("https://"):
            raise ValueError("destination_url must use HTTPS.")
        return v


@router.patch(
    "/v1/endpoints/{endpoint_id}",
    dependencies=[Depends(verify_proxy_secret)],
    summary="Update a registered webhook endpoint",
)
async def update_endpoint(
    endpoint_id: str,
    body: UpdateEndpointRequest,
    x_rapidapi_user: str = Header(..., description="Injected by RapidAPI gateway"),
):
    db = get_db()

    # Verify ownership.
    existing = await asyncio.to_thread(
        lambda: db.table("endpoints")
        .select("id")
        .eq("id", endpoint_id)
        .eq("user_api_key", x_rapidapi_user)
        .limit(1)
        .execute()
    )
    if not existing.data:
        raise HTTPException(status_code=404, detail="Endpoint not found.")

    update_data = body.model_dump(exclude_none=True)
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update.")

    result = await asyncio.to_thread(
        lambda: db.table("endpoints")
        .update(update_data)
        .eq("id", endpoint_id)
        .execute()
    )

    invalidate_cache()
    logger.info(f"Endpoint {endpoint_id} updated: {list(update_data.keys())}")
    return result.data[0] if result.data else {"status": "updated"}


@router.delete(
    "/v1/endpoints/{endpoint_id}",
    dependencies=[Depends(verify_proxy_secret)],
    summary="Delete a registered webhook endpoint and all its events",
)
async def delete_endpoint(
    endpoint_id: str,
    x_rapidapi_user: str = Header(..., description="Injected by RapidAPI gateway"),
):
    db = get_db()

    # Verify ownership.
    existing = await asyncio.to_thread(
        lambda: db.table("endpoints")
        .select("id")
        .eq("id", endpoint_id)
        .eq("user_api_key", x_rapidapi_user)
        .limit(1)
        .execute()
    )
    if not existing.data:
        raise HTTPException(status_code=404, detail="Endpoint not found.")

    # CASCADE on the FK will also delete all webhook_events for this endpoint.
    await asyncio.to_thread(
        lambda: db.table("endpoints")
        .delete()
        .eq("id", endpoint_id)
        .execute()
    )

    invalidate_cache()
    logger.info(f"Endpoint {endpoint_id} deleted by user {x_rapidapi_user}")
    return {"status": "deleted", "endpoint_id": endpoint_id}
