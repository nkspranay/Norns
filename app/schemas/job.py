from pydantic import BaseModel, Field
from datetime import datetime
from typing import Any
from app.models.job import JobStatus, JobPriority


# ── Job Schemas ────────────────────────────────────────────────────────────────

class JobCreate(BaseModel):
    """What the client sends when submitting a new job."""
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = Field(None, max_length=1000)
    job_type: str = Field(..., min_length=1, max_length=100)
    payload: dict[str, Any] | None = None
    priority: JobPriority = JobPriority.MEDIUM
    max_retries: int = Field(default=3, ge=0, le=10)
    cron_expression: str | None = None
    scheduled_at: datetime | None = None


class JobResponse(BaseModel):
    """What the API returns when a job is created or fetched."""
    id: str
    name: str
    description: str | None
    job_type: str
    payload: dict[str, Any] | None
    status: JobStatus
    priority: JobPriority
    max_retries: int
    retry_count: int
    cron_expression: str | None
    scheduled_at: datetime | None
    is_recurring: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class JobListResponse(BaseModel):
    """Paginated list of jobs."""
    jobs: list[JobResponse]
    total: int
    page: int
    page_size: int


# ── Execution Schemas ──────────────────────────────────────────────────────────

class ExecutionResponse(BaseModel):
    """What the API returns for a job execution attempt."""
    id: str
    job_id: str
    worker_id: str | None
    status: JobStatus
    attempt_number: int
    idempotency_key: str
    result: dict[str, Any] | None
    error_message: str | None
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime

    model_config = {"from_attributes": True}


# ── Worker Schemas ─────────────────────────────────────────────────────────────

class WorkerResponse(BaseModel):
    """What the API returns for a worker."""
    id: str
    name: str
    status: str
    last_heartbeat: datetime | None
    current_job_id: str | None
    jobs_completed: int
    jobs_failed: int
    registered_at: datetime

    model_config = {"from_attributes": True}