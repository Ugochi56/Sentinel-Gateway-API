from fastapi import Header, HTTPException, status
from app.config import settings


async def verify_proxy_secret(x_rapidapi_proxy_secret: str = Header(...)):
    """
    FastAPI dependency. Rejects any request that doesn't carry the correct
    RapidAPI proxy secret. Attach to management routes (register, logs).
    The /hooks/{id} ingestion endpoint is secured by UUID entropy, not this secret,
    because providers like Stripe cannot inject custom headers.
    """
    if x_rapidapi_proxy_secret != settings.RAPIDAPI_PROXY_SECRET:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden: invalid proxy secret.",
        )
