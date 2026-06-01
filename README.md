<div align="center">

# 🔱 Norns

**A production-grade distributed job scheduling and execution platform**

*Named after the Norse Fates who weave the threads of time — Norns controls when work happens.*

[![Python](https://img.shields.io/badge/Python-3.13-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?style=flat&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat&logo=postgresql&logoColor=white)](https://postgresql.org)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?style=flat&logo=redis&logoColor=white)](https://redis.io)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)](https://docker.com)

</div>

---

## What is Norns?

Norns is a distributed job scheduling platform that reliably executes background work at scale. When a user uploads a video, places an order, or triggers a report — the web server shouldn't wait. Norns takes that work, queues it, distributes it across workers, and guarantees it completes — even when machines crash.

This project implements the same architectural patterns used in large-scale background processing systems — distributed queues, worker pools, failure recovery, and observability — built from scratch.

**Core guarantees:**
- Every submitted job executes **at least once**
- Duplicate execution is **prevented through idempotency safeguards** — each attempt carries a unique key enforced at the database level
- Failed jobs **retry with exponential backoff** up to a configurable limit
- Dead workers are **detected and recovered** automatically via heartbeat expiry
- Every job state transition is **observable** in real time

---

## Architecture

```
┌──────────────────────────────────────────────┐
                         │             Browser / Dashboard              │
                         │                                              │
                         │  • WebSocket connection to FastAPI /ws       │
                         │    (opened on page load, stays persistent)   │
                         │  • Receives pushed events — never polls      │
                         │  • If WebSocket fails → system unaffected    │
                         │  • Browser auto-reconnects every 3 seconds   │
                         └──────┬────────────────────────────────▲──────┘
                                │                                │
                                │ HTTP Traffic                   │ Persistent
                                │ (POST/GET/DELETE)              │ WS Broadcasts
                                ▼                                │
    ┌────────────────────────────────────────────────────────────┴─┐
    │                       FastAPI Process                        │
    │                                                              │
    │  ┌─────────────────┐                  ┌─────────────────┐    │
    │  │    REST API     │                  │  WebSocket Hub  │    │
    │  │                 │                  │                 │    │
    │  │ • rate limiting │                  │ • manages all   │    │
    │  │ • validation    │                  │   browser       │    │
    │  │ • job creation  │                  │   connections   │    │
    │  │ • job queries   │                  │ • broadcasts    │    │
    │  └────────┬────────┘                  └────────▲────────┘    │
    │           │                                    │             │
    └───────────┼────────────────────────────────────┼─────────────┘
                │                                    │
        ┌───────┴──────────────┐                     │ Subscribes to
        │ Reads/Writes         │ Ingestion &         │ norns:jobs &
        │ Metadata/Queries     │ Rate Limiting       │ norns:workers
        ▼                      ▼                     │
    ┌─────────────────┐  ┌───────────────────────────┴──────────┐
    │   PostgreSQL    │  │                Redis                 │
    │                 │  │                                      │
    │ • jobs          │  │ ① job queues (LPUSH / BRPOP)         │
    │ • executions    │  │ ② distributed locks (SET NX PX / DEL)│
    │ • workers       │  │ ③ worker heartbeats (SET with TTL)   │
    │                 │  │ ④ rate limiter (sorted sets)         │
    │                 │  │ ⑤ pub/sub channels                   │
    └────────▲────────┘  └──────────────────▲───────────────────┘
             │                              │
             │                              │ ◄──► Bidirectional
             │ Writes State Changes         │      • BRPOP Job Payload
             │ (status, retry_count,        │      • Acquire/Refresh Locks
             │  result, attempt,            │      • Worker Heartbeats
             │  timing, error)              │      • Requeue on Backoff / DLQ
             │                              │      • Publish State Events
             │                              │
             └───────────────┐      ┌───────▼───────────────────┐
                             │      │        Worker Pool        │
                             │      │                           │
                             │      │   Worker A     Worker B   │
                             │      │   Worker C     Worker N   │
                             └──────┴───────────────────────────┘
```

---

## System design decisions

### Why Redis for the queue instead of Kafka?

Redis lists (`LPUSH`/`BRPOP`) provide sub-millisecond job dispatch with zero operational overhead. Kafka solves problems we don't have yet — multi-consumer log replay, millions of events per second, cross-datacenter replication. At Phase 1 scale, adding Kafka would mean 3 extra days of ops work instead of building reliability features. Redis is also doing four jobs simultaneously: queue, distributed locking, pub/sub, and rate limiting — consolidating infrastructure rather than fragmenting it.

Kafka is the right Phase 2 evolution when queue volume justifies it.

### Why PostgreSQL for job state instead of a dedicated store?

Jobs have relational structure — a job has many executions, executions belong to workers. PostgreSQL gives us ACID transactions, foreign key enforcement, and complex queries at no additional operational cost. The idempotency key unique constraint is enforced at the database level — even if the application layer is bypassed, PostgreSQL prevents duplicate executions.

### Why async throughout?

Every hot path in the scheduler is I/O-bound — database queries, Redis calls, WebSocket pushes. FastAPI with asyncio allows one process to handle hundreds of concurrent requests without threads. Under load testing at 100 concurrent users, median response time was **3ms** with 95th percentile at **8ms**. A synchronous implementation would require a thread pool and degrade under exactly this kind of concurrent load.

### Distributed locking design

Workers claim jobs using Redis `SET key value NX PX ttl` — atomic set-if-not-exists with millisecond expiry. This is the Redlock pattern. The TTL (30 seconds) is the safety net: if a worker crashes mid-execution, the lock expires and the job is reassigned. Workers refresh the lock every 10 seconds via heartbeat — as long as the worker is alive, the lock never expires. If the worker dies, the lock expires in at most 30 seconds and the scheduler requeues the job.

### Rate limiting strategy

Job submission (`POST /jobs`) uses **token bucket** — allows bursts up to 500 tokens because schedulers have inherently bursty workloads (payroll runs, batch processing). Read endpoints use **sliding window** — strict 100 requests/minute per IP with no burst allowance. Both are implemented in Redis with standard `X-RateLimit-*` headers on every response.

---

## Job lifecycle

```
Client submits job
        │
        ▼
API validates request (Pydantic)
        │
        ▼
Job written to PostgreSQL (status: QUEUED)
        │
        ▼
Job pushed to Redis priority queue (high/medium/low)
        │
        ▼
Worker picks up job via BRPOP (blocks until available)
        │
        ▼
Worker acquires distributed lock (SET NX PX)
        │
        ├─── Lock fails? Another worker has it → skip
        │
        ▼
Worker checks idempotency key (prevents duplicate execution)
        │
        ├─── Key exists? Job already ran → skip
        │
        ▼
Execution record created (status: RUNNING)
Job status updated to RUNNING
        │
        ▼
Job executes
        │
        ├─── Success → status: COMPLETED, result stored
        │
        └─── Failure → retry_count++
                │
                ├─── retry_count < max_retries
                │    → exponential backoff (2^n seconds)
                │    → requeued to Redis
                │
                └─── retry_count >= max_retries
                     → status: DEAD
                     → sent to dead-letter queue
```

---

## Worker lifecycle

```
Worker starts
    │
    ▼
Registers in PostgreSQL (unique worker ID)
    │
    ▼
Marks previous workers without recent heartbeat as DEAD
    │
    ├──────────────────────────────────────────────────────┐
    │                                                      │
    ▼                                                   (every 10s)
Main loop: BRPOP from Redis queue              Heartbeat loop:
    │                                            → ping Redis (TTL 30s)
    ▼                                            → refresh distributed lock
Process job                                      → update PostgreSQL
    │                                              last_heartbeat timestamp
    ▼
Release lock + publish event to Redis pub/sub
    │
    ▼
WebSocket hub broadcasts to connected dashboards
```

---

## Failure recovery

**Worker crash mid-execution:**
1. Heartbeat stops refreshing
2. Redis lock TTL expires (within 30 seconds)
3. Scheduler detects missing heartbeat, marks worker DEAD
4. Job status checked — if RUNNING with no active lock, requeued
5. Another worker picks it up, idempotency check prevents duplicate side effects

**Redis connection lost:**
Rate limiter fails open — requests pass through. Queue operations fail with logged errors. Worker retries Redis connection automatically.

**PostgreSQL connection lost:**
API returns 503. Workers pause and retry connection. No data loss — Redis queue preserves job order.

---

## Trade-offs and limitations

**Current design constraints worth knowing:**

- **Redis is a single point of failure** — if Redis goes down, the queue, locking, and pub/sub all fail simultaneously. Phase 2 addresses this with Redis Sentinel or Cluster for high availability.
- **At-least-once delivery** — if a worker executes a job and crashes before acknowledging completion, the job will retry. Idempotency keys prevent duplicate side effects but the execution attempt still happens twice. True exactly-once delivery is not guaranteed by design.
- **Worker discovery is database-driven** — workers register in PostgreSQL and are detected via heartbeat. There is no auto-scaling or container orchestration yet. Phase 2 adds Docker Compose scaling with Prometheus service discovery.
- **Queue persistence depends on Redis durability config** — by default Redis is in-memory. Enabling AOF (append-only file) persistence prevents job loss on Redis restart. Not configured in Phase 1.
- **Single scheduler process** — the API dispatches jobs but there is no dedicated scheduler service yet. High availability for the scheduler (leader election, failover) is a Phase 2 concern.
- **Simulated job execution** — `execute_job()` simulates work with `asyncio.sleep()`. Real job handlers (SMTP, S3, external APIs) are Phase 2. The infrastructure is complete and handler registration requires only adding cases to the executor function.

---

## Observability

Every service exposes a `/metrics` endpoint scraped by Prometheus every 15 seconds.

**Key metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `scheduler_jobs_submitted_total` | Counter | Jobs submitted, labeled by type and priority |
| `scheduler_queue_depth` | Gauge | Current jobs waiting per priority level |
| `scheduler_job_duration_seconds` | Histogram | Execution time distribution per job type |
| `worker_jobs_processed_total` | Counter | Jobs completed/failed per worker |
| `worker_active_jobs` | Gauge | Currently executing jobs per worker |
| `worker_job_duration_seconds` | Histogram | Worker-side execution timing |

Grafana dashboard: **Norns Command Center** — live queue depth, job throughput, worker utilization, and average execution duration.

Structured JSON logging via `structlog` — every log line includes `job_id`, `worker_id`, `attempt`, and timing metadata for full traceability.

---

## API reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/jobs` | Submit a new job |
| `GET` | `/api/v1/jobs` | List jobs (paginated, filterable by status) |
| `GET` | `/api/v1/jobs/{id}` | Get job by ID |
| `DELETE` | `/api/v1/jobs/{id}` | Cancel a job (soft delete) |
| `GET` | `/api/v1/jobs/{id}/executions` | Get execution history for a job |
| `GET` | `/api/v1/queue/stats` | Current queue depth per priority |
| `GET` | `/api/v1/queue/dlq` | Inspect dead-letter queue |
| `GET` | `/api/v1/workers` | List registered workers |
| `GET` | `/health` | Health check |
| `GET` | `/metrics` | Prometheus metrics |
| `WS` | `/ws` | WebSocket — live job and worker events |

**Submit a job:**
```bash
curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Send welcome email",
    "job_type": "email",
    "payload": {"to": "user@example.com", "subject": "Welcome"},
    "priority": "high",
    "max_retries": 3
  }'
```

---

## Getting started

**Prerequisites:** Docker Desktop, Python 3.11+, Git

```bash
# Clone the repository
git clone https://github.com/nkspranay/Norns.git
cd Norns

# Start infrastructure (PostgreSQL, Redis, Prometheus, Grafana)
docker compose up -d

# Install dependencies
pip install -r requirements.txt

# Run database migrations
alembic upgrade head

# Start the API server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Start a worker (in a new terminal)
python3 -m worker.worker

# Start multiple workers (optional)
WORKER_METRICS_PORT=9100 python3 -m worker.worker &
WORKER_METRICS_PORT=9101 python3 -m worker.worker &
WORKER_METRICS_PORT=9102 python3 -m worker.worker &
```

**Access:**
- API docs: `http://localhost:8000/docs`
- Grafana: `http://localhost:3000` (admin / admin123)
- Prometheus: `http://localhost:9090`

---

## Testing

```bash
# Unit tests — isolated logic, no infrastructure required
pytest tests/unit/ -v

# Integration tests — real database, real API
pytest tests/integration/ -v

# Full suite
pytest tests/unit/ tests/integration/ -v

# Load testing — requires API running
locust -f tests/load/locustfile.py
# Open http://localhost:8089, set 100 users at 10/s ramp, host: http://localhost:8000
```

**Test results:**
- Unit tests: 25 passing
- Integration tests: 23 passing
- Load test: **173 RPS** at 100 concurrent users (10/s ramp) — 3ms median, 8ms p95, zero system crashes. Every failure was a `429 Too Many Requests` from rate limiting working correctly, not a system error.

> Note: load test numbers recorded on a local development machine (Snapdragon ARM64, WSL2). Production numbers on cloud infrastructure would be significantly higher.

---

## Tech stack

| Layer | Technology | Reason |
|-------|-----------|--------|
| API framework | FastAPI + uvicorn | Async, OpenAPI auto-docs, dependency injection |
| Database | PostgreSQL 16 + SQLAlchemy 2.0 async | ACID, relational modeling, async ORM |
| Migrations | Alembic | Versioned schema changes |
| Queue + coordination | Redis 7 | Sub-ms latency, atomic operations, pub/sub |
| Real-time | WebSockets (FastAPI native) | Live dashboard without polling |
| Metrics | Prometheus + Grafana | Industry-standard observability stack |
| Logging | structlog | Structured JSON — searchable and parseable |
| Testing | pytest + pytest-asyncio + httpx | Full async test support |
| Load testing | Locust | Python-native, realistic traffic simulation |
| Infrastructure | Docker Compose | One-command local environment |

---

## Phase 2 roadmap

- **Authentication** — JWT + refresh tokens + RBAC
- **Kafka** — replace Redis pub/sub for high-volume event streams
- **Distributed tracing** — OpenTelemetry + Jaeger
- **Horizontal worker scaling** — Docker Compose scale + Prometheus auto-discovery
- **Microservices split** — scheduler, worker, notification as separate services
- **React dashboard** — real-time job monitoring UI

---

<div align="center">
Built with Python · FastAPI · PostgreSQL · Redis · Docker
</div>