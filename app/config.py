from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    SUPABASE_URL: str
    SUPABASE_KEY: str
    RAPIDAPI_PROXY_SECRET: str
    BASE_URL: str = "http://localhost:8000"

    # Ingestion
    MAX_PAYLOAD_SIZE_BYTES: int = 1_000_000  # 1 MB

    # Delivery worker
    MAX_RETRIES: int = 5
    WORKER_POLL_INTERVAL_SECONDS: int = 10
    DELIVERY_TIMEOUT_SECONDS: int = 15
    WORKER_BATCH_SIZE: int = 10

    # Cleanup: delete delivered rows older than N days (protects free-tier 500 MB)
    CLEANUP_DELIVERED_AFTER_DAYS: int = 7

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
