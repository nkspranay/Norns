import asyncio
import json
import structlog
from fastapi import WebSocket, WebSocketDisconnect
from redis.asyncio import Redis
from app.config import settings

log = structlog.get_logger()


# ── Connection Manager ─────────────────────────────────────────────────────────

class ConnectionManager:
    """
    Manages all active WebSocket connections.
    When an event arrives from Redis pub/sub,
    it fans out to every connected browser.
    """

    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        log.info("websocket_connected",
                 total=len(self.active_connections))

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        log.info("websocket_disconnected",
                 total=len(self.active_connections))

    async def broadcast(self, message: dict):
        """Send a message to every connected browser."""
        if not self.active_connections:
            return
        data = json.dumps(message)
        dead = []
        for connection in self.active_connections:
            try:
                await connection.send_text(data)
            except Exception:
                dead.append(connection)
        # Clean up dead connections
        for connection in dead:
            self.disconnect(connection)


# ── Global manager instance ────────────────────────────────────────────────────

manager = ConnectionManager()

# ── Redis pub/sub channels ─────────────────────────────────────────────────────

CHANNEL_JOBS    = "norns:jobs"
CHANNEL_WORKERS = "norns:workers"
CHANNEL_METRICS = "norns:metrics"


# ── Publisher — workers call these ────────────────────────────────────────────

async def publish_job_event(
    redis: Redis,
    event_type: str,
    job_id: str,
    job_type: str,
    status: str,
    worker_id: str | None = None,
    result: dict | None = None,
    error: str | None = None,
) -> None:
    """Publish a job state change event to Redis pub/sub."""
    message = json.dumps({
        "event":     event_type,
        "job_id":    job_id,
        "job_type":  job_type,
        "status":    status,
        "worker_id": worker_id,
        "result":    result,
        "error":     error,
    })
    await redis.publish(CHANNEL_JOBS, message)


async def publish_worker_event(
    redis: Redis,
    event_type: str,
    worker_id: str,
    worker_name: str,
    status: str,
    current_job_id: str | None = None,
) -> None:
    """Publish a worker state change event to Redis pub/sub."""
    message = json.dumps({
        "event":          event_type,
        "worker_id":      worker_id,
        "worker_name":    worker_name,
        "status":         status,
        "current_job_id": current_job_id,
    })
    await redis.publish(CHANNEL_WORKERS, message)


# ── Subscriber — runs as background task in FastAPI ───────────────────────────

async def redis_subscriber() -> None:
    """
    Runs forever as a background task.
    Subscribes to all Norns channels.
    When an event arrives, broadcasts to all WebSocket clients.
    """
    log.info("redis_subscriber_starting")

    while True:
        try:
            redis = Redis.from_url(
                settings.redis_url,
                encoding="utf-8",
                decode_responses=True,
            )
            pubsub = redis.pubsub()
            await pubsub.subscribe(
                CHANNEL_JOBS,
                CHANNEL_WORKERS,
                CHANNEL_METRICS,
            )
            log.info("redis_subscriber_ready",
                     channels=[CHANNEL_JOBS, CHANNEL_WORKERS])

            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                try:
                    data = json.loads(message["data"])
                    data["channel"] = message["channel"]
                    await manager.broadcast(data)
                except Exception as e:
                    log.error("subscriber_parse_error", error=str(e))

        except Exception as e:
            log.error("redis_subscriber_error", error=str(e))
            await asyncio.sleep(5)  # reconnect after 5 seconds
        finally:
            try:
                await pubsub.unsubscribe()
                await redis.aclose()
            except Exception:
                pass