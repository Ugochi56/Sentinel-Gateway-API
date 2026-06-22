import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import endpoints, ingest, logs, register
from app.worker.delivery import worker_loop

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Spawns the delivery worker as a background asyncio task on startup.
    Single Render web service — no extra worker dyno needed (free-tier).
    """
    logger.info("Sentinel Gateway starting up...")
    worker_task = asyncio.create_task(worker_loop(), name="sentinel-worker")
    yield
    logger.info("Sentinel Gateway shutting down...")
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="Sentinel Gateway",
    description=(
        "Unbreakable webhook middleware. "
        "Catches incoming webhook payloads, persists them, "
        "and retries delivery with exponential backoff until the client server recovers."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(register.router, tags=["Registration"])
app.include_router(endpoints.router, tags=["Endpoint Management"])
app.include_router(ingest.router, tags=["Ingestion"])
app.include_router(logs.router, tags=["Logs"])


@app.get("/health", tags=["System"])
async def health():
    """UptimeRobot pings this to keep Render from sleeping."""
    return {"status": "ok", "service": "sentinel-gateway"}
