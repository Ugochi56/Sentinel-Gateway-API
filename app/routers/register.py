import asyncio
import logging

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, field_validator

from app.cache import invalidate_cache
from app.config import settings
from app.database import get_db
from app.utils.security import verify_proxy_secret

logger = logging.getLogger(__name__)
router = APIRouter()


class RegisterRequest(BaseModel):
    destination_url: str
    label: str | None = None

    @field_validator("destination_url")
    @classmethod
    def must_be_https(cls, v: str) -> str:
        if not v.startswith("https://"):
            raise ValueError("destination_url must use HTTPS.")
        return v


class RegisterResponse(BaseModel):
    endpoint_id: str
    proxy_url: str
    destination_url: str
    label: str | None = None


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
):
    db = get_db()

    result = await asyncio.to_thread(
        lambda: db.table("endpoints")
        .insert(
            {
                "user_api_key": x_rapidapi_user,
                "destination_url": body.destination_url,
                "label": body.label,
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
    logger.info(f"Endpoint registered: {record['id']} → {body.destination_url}")

    return RegisterResponse(
        endpoint_id=record["id"],
        proxy_url=proxy_url,
        destination_url=record["destination_url"],
        label=record.get("label"),
    )
