import asyncio
import os
import json
import uuid
import time
import structlog
from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.config import settings
from app.database import AsyncSessionLocal
from app.models.job import Job, Execution, Worker, JobStatus, WorkerStatus
from app.queue.redis_queue import (
    get_redis,
    dequeue_job,
    acquire_lock,
    refresh_lock,
    release_lock,
    send_heartbeat,
    send_to_dlq,
)
from app.websocket import publish_job_event, publish_worker_event

from prometheus_client import start_http_server, Counter, Gauge, Histogram

# Worker metrics
WORKER_JOBS_PROCESSED = Counter(
    "worker_jobs_processed_total",
    "Total jobs processed by this worker",
    ["job_type", "status"]
)
WORKER_ACTIVE_JOBS = Gauge(
    "worker_active_jobs",
    "Number of jobs currently being processed"
)
WORKER_JOB_DURATION = Histogram(
    "worker_job_duration_seconds",
    "Job execution duration in seconds",
    ["job_type"],
    buckets=[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0],
)

log = structlog.get_logger()


# ── Job Executor ───────────────────────────────────────────────────────────────

async def execute_job(job_type: str, payload: dict) -> dict:
    """
    The actual job execution logic.
    In Phase 1 this simulates work.
    In Phase 2 this routes to real job handlers.
    """
    log.info("executing_job", job_type=job_type, payload=payload)

    # Simulate different job types
    if job_type == "email":
        await asyncio.sleep(1)  # simulate sending email
        return {"sent_to": payload.get("to"), "status": "delivered"}

    elif job_type == "report":
        await asyncio.sleep(2)  # simulate generating report
        return {"report_id": str(uuid.uuid4()), "status": "generated"}

    elif job_type == "data_pipeline":
        await asyncio.sleep(3)  # simulate data processing
        return {"rows_processed": 1000, "status": "complete"}

    elif job_type == "failing_job":
        # Intentionally fails — used for testing retry logic
        raise ValueError("This job always fails — used for retry testing")

    else:
        await asyncio.sleep(1)
        return {"job_type": job_type, "status": "completed"}
    
# ── Cleanup Dead Workers ────────────────────────────────────────────────────────

async def cleanup_dead_workers() -> None:
    """Mark workers that haven't heartbeated recently as dead."""
    from datetime import timedelta
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Worker))
        workers = result.scalars().all()
        now = datetime.now(timezone.utc)
        for worker in workers:
            if worker.last_heartbeat is None:
                worker.status = WorkerStatus.DEAD
            elif (now - worker.last_heartbeat).seconds > settings.heartbeat_timeout:
                worker.status = WorkerStatus.DEAD
        await db.commit()
        log.info("dead_workers_cleaned_up")

# ── Worker Registration ────────────────────────────────────────────────────────

async def register_worker(worker_id: str, worker_name: str) -> None:
    """Register this worker in PostgreSQL on startup."""
    async with AsyncSessionLocal() as db:
        worker = Worker(
            id=worker_id,
            name=worker_name,
            status=WorkerStatus.IDLE,
        )
        db.add(worker)
        try:
            await db.commit()
            log.info("worker_registered", worker_id=worker_id, name=worker_name)
        except IntegrityError:
            await db.rollback()
            log.info("worker_already_registered", worker_id=worker_id)


# ── Heartbeat Loop ─────────────────────────────────────────────────────────────

async def heartbeat_loop(worker_id: str, current_job: dict) -> None:
    """
    Runs concurrently with job execution.
    Sends heartbeat every HEARTBEAT_INTERVAL seconds.
    Also refreshes the distributed lock so it doesn't expire.
    """
    while True:
        try:
            redis = await get_redis()
            job_id = current_job.get("job_id")

            # Send heartbeat to Redis
            await send_heartbeat(redis, worker_id, job_id)

            # Refresh lock if running a job
            if job_id:
                await refresh_lock(redis, job_id, worker_id)

            await redis.aclose()
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(Worker).where(Worker.id == worker_id)
                )
                worker = result.scalar_one_or_none()
                if worker:
                    worker.last_heartbeat = datetime.now(timezone.utc)
                    worker.current_job_id = job_id
                    worker.status = WorkerStatus.ACTIVE if job_id else WorkerStatus.IDLE
                    await db.commit()
            log.info("heartbeat_sent", worker_id=worker_id, job_id=job_id)

        except Exception as e:
            log.error("heartbeat_failed", worker_id=worker_id, error=str(e))

        await asyncio.sleep(settings.heartbeat_interval)


# ── Process Single Job ─────────────────────────────────────────────────────────

async def process_job(worker_id: str, job_message: dict) -> None:
    """Full lifecycle of processing one job."""
    job_id   = job_message["job_id"]
    job_type = job_message["job_type"]
    payload  = job_message["payload"]

    redis = await get_redis()

    # ── Step 1: Acquire distributed lock ──────────────────────────────────────
    lock_acquired = await acquire_lock(redis, job_id, worker_id)
    if not lock_acquired:
        log.warning("lock_not_acquired", job_id=job_id, worker_id=worker_id)
        await redis.aclose()
        return

    log.info("job_started", job_id=job_id, job_type=job_type, worker_id=worker_id)

    async with AsyncSessionLocal() as db:

        # ── Step 2: Load job from PostgreSQL ──────────────────────────────────
        result = await db.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one_or_none()

        if not job:
            log.error("job_not_found", job_id=job_id)
            await release_lock(redis, job_id, worker_id)
            await redis.aclose()
            return

        # ── Step 3: Check job is still valid ──────────────────────────────────
        if job.status in (JobStatus.CANCELLED, JobStatus.COMPLETED, JobStatus.DEAD):
            log.info("job_skipped", job_id=job_id, status=job.status)
            await release_lock(redis, job_id, worker_id)
            await redis.aclose()
            return

        # ── Step 4: Check idempotency ──────────────────────────────────────────
        idempotency_key = f"{job_id}:attempt:{job.retry_count + 1}"
        existing = await db.execute(
            select(Execution).where(
                Execution.idempotency_key == idempotency_key
            )
        )
        if existing.scalar_one_or_none():
            log.warning("duplicate_execution_prevented",
                       job_id=job_id, key=idempotency_key)
            await release_lock(redis, job_id, worker_id)
            await redis.aclose()
            return

        # ── Step 5: Create execution record ───────────────────────────────────
        execution = Execution(
            job_id=job_id,
            worker_id=worker_id,
            status=JobStatus.RUNNING,
            attempt_number=job.retry_count + 1,
            idempotency_key=idempotency_key,
            started_at=datetime.now(timezone.utc),
        )
        db.add(execution)

        # ── Step 6: Mark job as RUNNING ───────────────────────────────────────
        job.status = JobStatus.RUNNING
        WORKER_ACTIVE_JOBS.inc()
        await db.commit()
        await db.refresh(execution)

        # Publish job started event
        await publish_job_event(
            redis, "job_started", job_id,
            job_type, "running", worker_id
        )
        job_start_time = time.time()
        # ── Step 7: Execute the job ───────────────────────────────────────────
        try:
            result = await execute_job(job_type, payload)
            job_duration = time.time() - job_start_time

            # ── Step 8a: Success ──────────────────────────────────────────────
            execution.status = JobStatus.COMPLETED
            execution.result = result
            execution.completed_at = datetime.now(timezone.utc)
            job.status = JobStatus.COMPLETED

            WORKER_JOB_DURATION.labels(job_type=job_type).observe(job_duration)
            WORKER_JOBS_PROCESSED.labels(job_type=job_type, status="completed").inc()
            #WORKER_ACTIVE_JOBS.dec()

            # Update worker stats in PostgreSQL
            worker_result = await db.execute(
                select(Worker).where(Worker.id == worker_id)
            )
            worker_record = worker_result.scalar_one_or_none()
            if worker_record:
                worker_record.jobs_completed += 1

            await db.commit()
            log.info("job_completed", job_id=job_id, result=result)

            # Publish job completed event
            await publish_job_event(
                redis, "job_completed", job_id,
                job_type, "completed", worker_id, result=result
            )

        except Exception as e:
            error_msg = str(e)
            log.error("job_failed", job_id=job_id, error=error_msg,
                     attempt=job.retry_count + 1)

            # ── Step 8b: Failure — retry or DLQ ──────────────────────────────
            execution.status = JobStatus.FAILED
            execution.error_message = error_msg
            execution.completed_at = datetime.now(timezone.utc)
            job.retry_count += 1

            if job.retry_count >= job.max_retries:
                # Max retries exhausted → dead letter queue
                job.status = JobStatus.DEAD
                await send_to_dlq(
                    redis, job_id, job_type, payload,
                    error_msg, job.retry_count
                )
                WORKER_JOBS_PROCESSED.labels(job_type=job_type, status="failed").inc()
                # WORKER_ACTIVE_JOBS.dec()

                 # Update worker failure stats
                worker_result = await db.execute(
                    select(Worker).where(Worker.id == worker_id)
                )
                worker_record = worker_result.scalar_one_or_none()
                if worker_record:
                    worker_record.jobs_failed += 1
                log.error("job_sent_to_dlq", job_id=job_id,
                         attempts=job.retry_count)
                
                # Publish job dead event
                await publish_job_event(
                    redis, "job_dead", job_id,
                    job_type, "dead", worker_id, error=error_msg
                )
            else:
                # Calculate backoff delay
                backoff = settings.job_retry_backoff ** job.retry_count
                job.status = JobStatus.QUEUED
                log.info("job_retrying", job_id=job_id,
                        attempt=job.retry_count,
                        backoff_seconds=backoff)

                # Wait then requeue
                await asyncio.sleep(backoff)
                from app.queue.redis_queue import enqueue_job
                await enqueue_job(
                    redis, job_id, job_type, payload,
                    priority=job_message.get("priority", "medium")
                )

            await db.commit()

        finally:
            # ── Step 9: Always release the lock ───────────────────────────────
            await release_lock(redis, job_id, worker_id)
            WORKER_ACTIVE_JOBS.dec()
            await redis.aclose()


# ── Main Worker Loop ───────────────────────────────────────────────────────────

async def run_worker() -> None:
    """Main worker loop — runs forever, processing jobs from the queue."""
    worker_id   = str(uuid.uuid4())
    worker_name = f"worker-{worker_id[:8]}"
    current_job = {}  # shared state between heartbeat and processor

    log.info("worker_starting", worker_id=worker_id, name=worker_name)

    # Register in PostgreSQL
    await register_worker(worker_id, worker_name)
    # Start metrics server so Prometheus can scrape this worker
    # start_http_server(9100)
    metrics_port = int(os.environ.get("WORKER_METRICS_PORT", 9100))
    try:
        start_http_server(metrics_port)
        log.info("metrics_server_started", port=metrics_port)
    except OSError:
        # Port already in use — skip metrics server for this worker
        log.warning("metrics_port_in_use", port=metrics_port)
    # log.info("metrics_server_started", port=9100)
    await cleanup_dead_workers()

    # Start heartbeat as a background task
    heartbeat_task = asyncio.create_task(
        heartbeat_loop(worker_id, current_job)
    )

    redis = await get_redis()

    try:
        log.info("worker_ready", worker_id=worker_id)

        while True:
            try:
                # Block and wait for a job — checks high → medium → low
                job_message = await dequeue_job(redis, timeout=5)

                if job_message is None:
                    # No job in queue — loop and wait again
                    continue

                # Update shared state so heartbeat knows current job
                current_job["job_id"] = job_message["job_id"]

                # Process the job
                await process_job(worker_id, job_message)

                # Clear current job from shared state
                current_job.clear()

            except Exception as e:
                log.error("worker_loop_error", error=str(e))
                await asyncio.sleep(1)

    finally:
        heartbeat_task.cancel()
        await redis.aclose()
        log.info("worker_stopped", worker_id=worker_id)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    asyncio.run(run_worker())