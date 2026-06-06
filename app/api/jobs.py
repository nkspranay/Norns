import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from redis.asyncio import Redis

from app.database import get_db
from app.models.job import Job, Execution, Worker, JobStatus
from app.schemas.job import (
    JobCreate, JobResponse, JobListResponse, ExecutionResponse, WorkerResponse
)
from app.queue.redis_queue import (
    get_redis, enqueue_job, get_queue_stats, get_dlq_jobs
)
from app.main import JOBS_SUBMITTED, QUEUE_DEPTH

log = structlog.get_logger()
router = APIRouter()


# ── Dependency — Redis connection ──────────────────────────────────────────────

async def get_redis_conn() -> Redis:
    redis = await get_redis()
    try:
        yield redis
    finally:
        await redis.aclose()


# ── POST /jobs — Submit a new job ──────────────────────────────────────────────

@router.post("/jobs", response_model=JobResponse, status_code=201)
async def create_job(
    job_in: JobCreate,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis_conn),
):
    # 1. Create job record in PostgreSQL
    job = Job(
        name=job_in.name,
        description=job_in.description,
        job_type=job_in.job_type,
        payload=job_in.payload,
        priority=job_in.priority,
        max_retries=job_in.max_retries,
        cron_expression=job_in.cron_expression,
        scheduled_at=job_in.scheduled_at,
        is_recurring=job_in.cron_expression is not None,
        status=JobStatus.QUEUED,
    )
    db.add(job)
    await db.flush()  # get the generated ID without committing yet

    # 2. Push job onto Redis queue
    await enqueue_job(
        redis=redis,
        job_id=job.id,
        job_type=job.job_type,
        payload=job.payload or {},
        priority=job.priority.value,
    )

    # 3. Commit to PostgreSQL
    await db.commit()
    await db.refresh(job)

    # Publish queued event so WebSocket dashboard shows it immediately
    await publish_job_event(
        redis, "job_queued", job.id,
        job.job_type, "queued"
    )

    # 4. Update Prometheus metrics
    JOBS_SUBMITTED.labels(
        job_type=job.job_type,
        priority=job.priority.value,
    ).inc()

    log.info("job_created", job_id=job.id, job_type=job.job_type,
             priority=job.priority.value)

    return job


# ── GET /jobs — List all jobs ──────────────────────────────────────────────────

@router.get("/jobs", response_model=JobListResponse)
async def list_jobs(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    status: JobStatus | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    offset = (page - 1) * page_size

    # Build query
    query = select(Job).order_by(Job.created_at.desc())
    count_query = select(func.count(Job.id))

    # Filter by status if provided
    if status:
        query = query.where(Job.status == status)
        count_query = count_query.where(Job.status == status)

    # Execute
    total_result = await db.execute(count_query)
    total = total_result.scalar()

    result = await db.execute(query.offset(offset).limit(page_size))
    jobs = result.scalars().all()

    return JobListResponse(
        jobs=jobs,
        total=total,
        page=page,
        page_size=page_size,
    )


# ── GET /jobs/{job_id} — Get a single job ─────────────────────────────────────

@router.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job(job_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


# ── DELETE /jobs/{job_id} — Cancel a job ──────────────────────────────────────

@router.delete("/jobs/{job_id}", status_code=204)
async def cancel_job(job_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status == JobStatus.RUNNING:
        raise HTTPException(status_code=400, detail="Cannot cancel a running job")
    job.status = JobStatus.CANCELLED
    await db.commit()
    log.info("job_cancelled", job_id=job_id)


# ── GET /jobs/{job_id}/executions — Get execution history ─────────────────────

@router.get("/jobs/{job_id}/executions", response_model=list[ExecutionResponse])
async def get_job_executions(job_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Execution)
        .where(Execution.job_id == job_id)
        .order_by(Execution.created_at.desc())
    )
    return result.scalars().all()


# ── GET /queue/stats — Queue depth ────────────────────────────────────────────

@router.get("/queue/stats")
async def queue_stats(redis: Redis = Depends(get_redis_conn)):
    stats = await get_queue_stats(redis)
    # Update Prometheus gauges
    for priority, depth in stats.items():
        if priority != "total":
            QUEUE_DEPTH.labels(priority=priority).set(depth)
    return stats


# ── GET /queue/dlq — Inspect dead-letter queue ────────────────────────────────

@router.get("/queue/dlq")
async def dead_letter_queue(
    count: int = Query(default=10, ge=1, le=100),
    redis: Redis = Depends(get_redis_conn),
):
    return await get_dlq_jobs(redis, count)


# ── GET /workers — List active workers ────────────────────────────────────────

@router.get("/workers", response_model=list[WorkerResponse])
async def list_workers(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Worker).order_by(Worker.registered_at.desc())
    )
    return result.scalars().all()