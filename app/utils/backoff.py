from datetime import datetime, timezone, timedelta
from app.config import settings


def calculate_next_retry(retry_count: int) -> datetime:
    """
    Wait = 2^retry_count × 60 seconds.

    retry_count 0 →  60s  (1 min)
    retry_count 1 → 120s  (2 min)
    retry_count 2 → 240s  (4 min)
    retry_count 3 → 480s  (8 min)
    retry_count 4 → 960s  (16 min)
    retry_count 5 → 1920s (32 min)  ← max before permanent failure

    Total coverage before failed_permanently: ~63 minutes across 5 retries.
    """
    wait_seconds = (2 ** retry_count) * 60
    return datetime.now(timezone.utc) + timedelta(seconds=wait_seconds)


def has_exceeded_max_retries(retry_count: int) -> bool:
    return retry_count >= settings.MAX_RETRIES
