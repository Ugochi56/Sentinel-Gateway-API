import asyncio
import logging

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, field_validator

from app.cache import invalidate_cache
from app.config import settings
from app.database import get_db
from app.utils.security import verify_proxy_secret
from app.utils.tiers import get_tier_limits

logger = logging.getLogger(__name__)
router = APIRouter()


class RegisterRequest(BaseModel):
    destination_url: str
    label: str | None = None

    @field_validator("destination_url")
    @classmethod
    def must_be_https(cls, v: str) -> str:
        is_local = v.startswith("http://localhost") or v.startswith("http://127.0.0.1")
        if not (v.startswith("https://") or is_local):
            raise ValueError("destination_url must use HTTPS (except localhost for local development).")
        return v


class RegisterResponse(BaseModel):
    endpoint_id: str
    proxy_url: str
    destination_url: str
    label: str | None = None
    plan: str


@router.post(
    "/v1/register",
    response_model=RegisterResponse,
    dependencies=[Depends(verify_proxy_secret)],
    summary="Register a webhook endpoint",
    description=(
        "Creates a unique proxy URL for the client. "
        "Give this URL to Stripe, GitHub, Shopify, etc. as the webhook destination."
    ),
)
async def register_endpoint(
    body: RegisterRequest,
    x_rapidapi_user: str = Header(..., description="Injected by RapidAPI gateway"),
    x_rapidapi_plan: str = Header("Free", alias="x-rapidapi-plan", description="Injected by RapidAPI gateway"),
):
    db = get_db()
    
    # 1. Enforce endpoint count limits per plan
    limits = get_tier_limits(x_rapidapi_plan)
    if limits.max_endpoints is not None:
        count_result = await asyncio.to_thread(
            lambda: db.table("endpoints")
            .select("id", count="exact")
            .eq("user_api_key", x_rapidapi_user)
            .execute()
        )
        current_count = count_result.count or 0
        if current_count >= limits.max_endpoints:
            raise HTTPException(
                status_code=403,
                detail=(
                    f"Plan '{x_rapidapi_plan}' limits you to {limits.max_endpoints} endpoints. "
                    "You have reached this limit. Please upgrade."
                )
            )

    # 2. Insert new endpoint with plan details
    result = await asyncio.to_thread(
        lambda: db.table("endpoints")
        .insert(
            {
                "user_api_key": x_rapidapi_user,
                "destination_url": body.destination_url,
                "label": body.label,
                "plan": x_rapidapi_plan,
            }
        )
        .execute()
    )

    if not result.data:
        logger.error("Supabase insert returned no data during endpoint registration.")
        raise HTTPException(status_code=500, detail="Failed to register endpoint.")

    record = result.data[0]
    proxy_url = f"{settings.BASE_URL}/hooks/{record['id']}"

    invalidate_cache()
    logger.info(
        f"Endpoint registered under plan '{x_rapidapi_plan}': "
        f"{record['id']} → {body.destination_url}"
    )

    return RegisterResponse(
        endpoint_id=record["id"],
        proxy_url=proxy_url,
        destination_url=record["destination_url"],
        label=record.get("label"),
        plan=record.get("plan", "Free"),
    )
