import json
import asyncio
from datetime import datetime, timezone
from redis.asyncio import Redis
from app.config import settings


# ── Connection ─────────────────────────────────────────────────────────────────

async def get_redis() -> Redis:
    """Create and return an async Redis connection."""
    return Redis.from_url(
        settings.redis_url,
        encoding="utf-8",
        decode_responses=True,
    )


# ── Queue Names ────────────────────────────────────────────────────────────────

QUEUE_HIGH   = "scheduler:queue:high"
QUEUE_MEDIUM = "scheduler:queue:medium"
QUEUE_LOW    = "scheduler:queue:low"
QUEUE_DLQ    = "scheduler:queue:dlq"      # dead-letter queue
WORKERS_KEY  = "scheduler:workers"        # hash of active workers
LOCKS_PREFIX = "scheduler:lock:"          # prefix for job locks


def get_queue_name(priority: str) -> str:
    """Map job priority to the correct Redis queue name."""
    return {
        "high":   QUEUE_HIGH,
        "medium": QUEUE_MEDIUM,
        "low":    QUEUE_LOW,
    }.get(priority, QUEUE_MEDIUM)


# ── Producer — API pushes jobs onto queue ──────────────────────────────────────

async def enqueue_job(redis: Redis, job_id: str, job_type: str,
                      payload: dict, priority: str = "medium") -> None:
    """Push a job onto the appropriate priority queue."""
    queue_name = get_queue_name(priority)
    message = json.dumps({
        "job_id":    job_id,
        "job_type":  job_type,
        "payload":   payload or {},
        "priority":  priority,
        "enqueued_at": datetime.now(timezone.utc).isoformat(),
    })
    await redis.lpush(queue_name, message)


# ── Consumer — workers pop jobs from queue ─────────────────────────────────────

async def dequeue_job(redis: Redis, timeout: int = 5) -> dict | None:
    """
    Block and wait for a job from any queue.
    Checks high priority first, then medium, then low.
    Returns the job dict or None if timeout reached.
    """
    result = await redis.brpop(
        [QUEUE_HIGH, QUEUE_MEDIUM, QUEUE_LOW],
        timeout=timeout,
    )
    if result is None:
        return None
    _, message = result  # brpop returns (queue_name, value)
    return json.loads(message)


# ── Distributed Lock ───────────────────────────────────────────────────────────

async def acquire_lock(redis: Redis, job_id: str,
                       worker_id: str, ttl: int = 30) -> bool:
    """
    Try to acquire a distributed lock for a job.
    Returns True if lock acquired, False if another worker already has it.
    Uses SET NX PX — atomic set-if-not-exists with millisecond expiry.
    """
    lock_key = f"{LOCKS_PREFIX}{job_id}"
    result = await redis.set(
        lock_key,
        worker_id,
        nx=True,   # only set if key does not exist
        px=ttl * 1000,  # expiry in milliseconds
    )
    return result is True


async def refresh_lock(redis: Redis, job_id: str,
                       worker_id: str, ttl: int = 30) -> bool:
    """
    Refresh the lock TTL — called by heartbeat to prevent expiry
    while job is still running.
    Only refreshes if this worker still owns the lock.
    """
    lock_key = f"{LOCKS_PREFIX}{job_id}"
    current_owner = await redis.get(lock_key)
    if current_owner == worker_id:
        await redis.pexpire(lock_key, ttl * 1000)
        return True
    return False  # lock was taken by someone else — we lost it


async def release_lock(redis: Redis, job_id: str, worker_id: str) -> bool:
    """
    Release the lock — called when job completes or fails.
    Only releases if this worker owns the lock — prevents
    accidentally releasing another worker's lock.
    """
    lock_key = f"{LOCKS_PREFIX}{job_id}"
    current_owner = await redis.get(lock_key)
    if current_owner == worker_id:
        await redis.delete(lock_key)
        return True
    return False


# ── Heartbeat ──────────────────────────────────────────────────────────────────

async def send_heartbeat(redis: Redis, worker_id: str,
                         job_id: str | None = None) -> None:
    """
    Worker calls this every HEARTBEAT_INTERVAL seconds.
    Stores worker state in Redis hash with a TTL.
    If heartbeat stops, the key expires and scheduler
    knows the worker is dead.
    """
    key = f"{WORKERS_KEY}:{worker_id}"
    data = json.dumps({
        "worker_id":  worker_id,
        "job_id":     job_id,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "status":     "running" if job_id else "idle",
    })
    await redis.set(key, data, ex=settings.heartbeat_timeout)


async def get_active_workers(redis: Redis) -> list[dict]:
    """Get all workers that have sent a heartbeat recently."""
    pattern = f"{WORKERS_KEY}:*"
    keys = await redis.keys(pattern)
    if not keys:
        return []
    workers = []
    for key in keys:
        data = await redis.get(key)
        if data:
            workers.append(json.loads(data))
    return workers


# ── Dead Letter Queue ──────────────────────────────────────────────────────────

async def send_to_dlq(redis: Redis, job_id: str,
                      job_type: str, payload: dict,
                      error: str, attempts: int) -> None:
    """
    Send a permanently failed job to the dead-letter queue.
    DLQ jobs are stored but not retried automatically.
    """
    message = json.dumps({
        "job_id":    job_id,
        "job_type":  job_type,
        "payload":   payload or {},
        "error":     error,
        "attempts":  attempts,
        "failed_at": datetime.now(timezone.utc).isoformat(),
    })
    await redis.lpush(QUEUE_DLQ, message)


async def get_dlq_jobs(redis: Redis, count: int = 10) -> list[dict]:
    """Inspect jobs in the dead-letter queue without removing them."""
    messages = await redis.lrange(QUEUE_DLQ, 0, count - 1)
    return [json.loads(m) for m in messages]


# ── Queue Stats ────────────────────────────────────────────────────────────────

async def get_queue_stats(redis: Redis) -> dict:
    """Get current depth of all queues — used by monitoring."""
    high   = await redis.llen(QUEUE_HIGH)
    medium = await redis.llen(QUEUE_MEDIUM)
    low    = await redis.llen(QUEUE_LOW)
    dlq    = await redis.llen(QUEUE_DLQ)
    return {
        "high":   high,
        "medium": medium,
        "low":    low,
        "dlq":    dlq,
        "total":  high + medium + low,
    }