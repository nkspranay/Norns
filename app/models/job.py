import enum
import uuid
from datetime import datetime
from sqlalchemy import (
    String, Integer, Text, DateTime, Enum, ForeignKey, JSON, Boolean
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func
from app.database import Base


# ── Enums ──────────────────────────────────────────────────────────────────────

class JobStatus(str, enum.Enum):
    PENDING   = "pending"    # submitted, not yet queued
    QUEUED    = "queued"     # sitting in Redis queue
    RUNNING   = "running"    # a worker has picked it up
    COMPLETED = "completed"  # finished successfully
    FAILED    = "failed"     # failed, retries exhausted
    CANCELLED = "cancelled"  # manually cancelled
    DEAD      = "dead"       # in dead-letter queue


class JobPriority(str, enum.Enum):
    LOW    = "low"
    MEDIUM = "medium"
    HIGH   = "high"


class WorkerStatus(str, enum.Enum):
    ACTIVE  = "active"   # heartbeating normally
    IDLE    = "idle"     # connected but no job running
    DEAD    = "dead"     # heartbeat expired


# ── Job ────────────────────────────────────────────────────────────────────────

class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # What to run
    job_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # Scheduling
    cron_expression: Mapped[str | None] = mapped_column(String(100), nullable=True)
    scheduled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_recurring: Mapped[bool] = mapped_column(Boolean, default=False)

    # State
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus), default=JobStatus.PENDING, nullable=False
    )
    priority: Mapped[JobPriority] = mapped_column(
        Enum(JobPriority), default=JobPriority.MEDIUM, nullable=False
    )

    # Retry tracking
    max_retries: Mapped[int] = mapped_column(Integer, default=3)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    executions: Mapped[list["Execution"]] = relationship(
        "Execution", back_populates="job", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Job id={self.id} name={self.name} status={self.status}>"


# ── Execution ──────────────────────────────────────────────────────────────────

class Execution(Base):
    __tablename__ = "executions"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    job_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    worker_id: Mapped[str | None] = mapped_column(String(36), nullable=True)

    # State
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus), default=JobStatus.RUNNING, nullable=False
    )
    attempt_number: Mapped[int] = mapped_column(Integer, default=1)

    # Idempotency — prevents double execution
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)

    # Results
    result: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Timing
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Relationship back to Job
    job: Mapped["Job"] = relationship("Job", back_populates="executions")

    def __repr__(self) -> str:
        return f"<Execution id={self.id} job_id={self.job_id} status={self.status}>"


# ── Worker ─────────────────────────────────────────────────────────────────────

class Worker(Base):
    __tablename__ = "workers"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[WorkerStatus] = mapped_column(
        Enum(WorkerStatus), default=WorkerStatus.IDLE, nullable=False
    )

    # Heartbeat tracking
    last_heartbeat: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    current_job_id: Mapped[str | None] = mapped_column(String(36), nullable=True)

    # Metadata
    jobs_completed: Mapped[int] = mapped_column(Integer, default=0)
    jobs_failed: Mapped[int] = mapped_column(Integer, default=0)

    # Timestamps
    registered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    def __repr__(self) -> str:
        return f"<Worker id={self.id} name={self.name} status={self.status}>"