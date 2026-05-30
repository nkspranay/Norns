import structlog
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app, Counter, Gauge, Histogram
from fastapi import WebSocket, WebSocketDisconnect

from app.config import settings
from app.database import engine, Base
from app.middleware import RateLimitMiddleware
from app.websocket import manager, redis_subscriber



# ── Logging ────────────────────────────────────────────────────────────────────

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)
log = structlog.get_logger()

# ── Prometheus Metrics ─────────────────────────────────────────────────────────

JOBS_SUBMITTED = Counter(
    "scheduler_jobs_submitted_total",
    "Total number of jobs submitted",
    ["job_type", "priority"],
)
JOBS_COMPLETED = Counter(
    "scheduler_jobs_completed_total",
    "Total number of jobs completed successfully",
    ["job_type"],
)
JOBS_FAILED = Counter(
    "scheduler_jobs_failed_total",
    "Total number of jobs that failed",
    ["job_type"],
)
QUEUE_DEPTH = Gauge(
    "scheduler_queue_depth",
    "Current number of jobs in queue",
    ["priority"],
)
JOB_DURATION = Histogram(
    "scheduler_job_duration_seconds",
    "Job execution duration in seconds",
    ["job_type"],
    buckets=[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0],
)

# ── Lifespan ───────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    log.info("scheduler_starting", env=settings.app_env)
    asyncio.create_task(redis_subscriber())
    yield
    # Shutdown
    log.info("scheduler_stopping")
    await engine.dispose()

# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Distributed Job Scheduler",
    description="A production-grade async job scheduling platform",
    version="1.0.0",
    lifespan=lifespan,
)

# ── Middleware ─────────────────────────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    RateLimitMiddleware,
    bucket_capacity=500,
    bucket_refill_rate=100,
    window_limit=100,
    window_seconds=60,
)

# ── Mount Prometheus metrics endpoint ──────────────────────────────────────────

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# ── Routes ─────────────────────────────────────────────────────────────────────

from app.api import jobs as jobs_router  # noqa: E402
app.include_router(jobs_router.router, prefix="/api/v1", tags=["jobs"])

# ── WebSocket endpoint ─────────────────────────────────────────────────────────  

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive — wait for client messages
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# ── Health check ───────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "env":    settings.app_env,
        "version": "1.0.0",
    }

@app.get("/")
async def root():
    return {"message": "Distributed Job Scheduler", "docs": "/docs"}