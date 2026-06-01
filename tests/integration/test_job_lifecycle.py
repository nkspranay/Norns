import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import select

from app.models.job import Job, JobStatus, JobPriority


# ── Job Creation ───────────────────────────────────────────────────────────────

class TestJobCreation:
    """Tests POST /api/v1/jobs endpoint."""

    @pytest.mark.asyncio
    async def test_create_job_returns_201(self, client: AsyncClient):
        """Job creation returns 201 Created."""
        response = await client.post("/api/v1/jobs", json={
            "name": "Test Job",
            "job_type": "email",
            "payload": {"to": "test@example.com"},
        })
        assert response.status_code == 201

    @pytest.mark.asyncio
    async def test_create_job_returns_correct_fields(self, client: AsyncClient):
        """Response contains all required fields."""
        response = await client.post("/api/v1/jobs", json={
            "name": "Field Test Job",
            "job_type": "email",
            "payload": {"to": "test@example.com"},
        })
        data = response.json()
        assert "id" in data
        assert "name" in data
        assert "status" in data
        assert "created_at" in data
        assert "retry_count" in data
        assert "idempotency_key" not in data  # internal field not exposed

    @pytest.mark.asyncio
    async def test_create_job_status_is_queued(self, client: AsyncClient):
        """Newly created job has QUEUED status."""
        response = await client.post("/api/v1/jobs", json={
            "name": "Status Test",
            "job_type": "email",
            "payload": {},
        })
        assert response.json()["status"] == "queued"

    @pytest.mark.asyncio
    async def test_create_job_default_priority_is_medium(self, client: AsyncClient):
        """Default priority is medium when not specified."""
        response = await client.post("/api/v1/jobs", json={
            "name": "Priority Test",
            "job_type": "email",
            "payload": {},
        })
        assert response.json()["priority"] == "medium"

    @pytest.mark.asyncio
    async def test_create_job_high_priority(self, client: AsyncClient):
        """High priority job is created correctly."""
        response = await client.post("/api/v1/jobs", json={
            "name": "High Priority Job",
            "job_type": "email",
            "payload": {},
            "priority": "high",
        })
        assert response.json()["priority"] == "high"

    @pytest.mark.asyncio
    async def test_create_job_missing_name_returns_422(self, client: AsyncClient):
        """Missing required field returns 422 Unprocessable Entity."""
        response = await client.post("/api/v1/jobs", json={
            "job_type": "email",
            "payload": {},
            # name is missing
        })
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_create_job_missing_job_type_returns_422(self, client: AsyncClient):
        """Missing job_type returns 422."""
        response = await client.post("/api/v1/jobs", json={
            "name": "Test",
            "payload": {},
            # job_type is missing
        })
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_create_job_empty_name_returns_422(self, client: AsyncClient):
        """Empty string name returns 422 — min_length=1 enforced."""
        response = await client.post("/api/v1/jobs", json={
            "name": "",
            "job_type": "email",
            "payload": {},
        })
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_create_job_max_retries_too_high_returns_422(self, client: AsyncClient):
        """max_retries above 10 returns 422 — le=10 enforced."""
        response = await client.post("/api/v1/jobs", json={
            "name": "Test",
            "job_type": "email",
            "payload": {},
            "max_retries": 11,
        })
        assert response.status_code == 422

    #@pytest.mark.asyncio
    #async def test_create_job_persisted_to_database(
    #    self, client: AsyncClient, db_session
    #):
    #    """Job is actually saved to PostgreSQL."""
    #    response = await client.post("/api/v1/jobs", json={
    #        "name": "DB Persistence Test",
    #        "job_type": "email",
    #        "payload": {"to": "db@test.com"},
    #    })
    #    assert response.status_code == 201
    #    job_id = response.json()["id"]

    #    # Verify in database directly
    #   result = await db_session.execute(
    #        select(Job).where(Job.id == job_id)
    #    )
    #    job = result.scalar_one_or_none()
    #    assert job is not None
    #    assert job.name == "DB Persistence Test"
    #    assert job.status == JobStatus.QUEUED

    @pytest.mark.asyncio
    async def test_create_job_persisted_to_database(self, client: AsyncClient):
        """Job is actually saved — verified by fetching it back via API."""
        response = await client.post("/api/v1/jobs", json={
            "name": "DB Persistence Test",
            "job_type": "email",
            "payload": {"to": "db@test.com"},
        })
        assert response.status_code == 201
        job_id = response.json()["id"]

        # Verify by fetching — if it's in the DB, this returns 200
        fetch = await client.get(f"/api/v1/jobs/{job_id}")
        assert fetch.status_code == 200
        assert fetch.json()["name"] == "DB Persistence Test"
        assert fetch.json()["status"] == "queued"


# ── Job Retrieval ──────────────────────────────────────────────────────────────

class TestJobRetrieval:
    """Tests GET /api/v1/jobs and GET /api/v1/jobs/{id}."""

    @pytest.mark.asyncio
    async def test_list_jobs_returns_200(self, client: AsyncClient):
        """Job list endpoint returns 200."""
        response = await client.get("/api/v1/jobs")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_list_jobs_response_shape(self, client: AsyncClient):
        """Response has correct pagination shape."""
        response = await client.get("/api/v1/jobs")
        data = response.json()
        assert "jobs" in data
        assert "total" in data
        assert "page" in data
        assert "page_size" in data

    @pytest.mark.asyncio
    async def test_list_jobs_pagination(self, client: AsyncClient):
        """Pagination parameters work correctly."""
        response = await client.get("/api/v1/jobs?page=1&page_size=5")
        data = response.json()
        assert data["page"] == 1
        assert data["page_size"] == 5
        assert len(data["jobs"]) <= 5

    @pytest.mark.asyncio
    async def test_get_job_by_id(self, client: AsyncClient):
        """Fetch specific job by ID."""
        # Create a job first
        create = await client.post("/api/v1/jobs", json={
            "name": "Fetch By ID Test",
            "job_type": "report",
            "payload": {},
        })
        job_id = create.json()["id"]

        # Fetch it
        response = await client.get(f"/api/v1/jobs/{job_id}")
        assert response.status_code == 200
        assert response.json()["id"] == job_id
        assert response.json()["name"] == "Fetch By ID Test"

    @pytest.mark.asyncio
    async def test_get_nonexistent_job_returns_404(self, client: AsyncClient):
        """Fetching non-existent job returns 404."""
        response = await client.get(
            "/api/v1/jobs/00000000-0000-0000-0000-000000000000"
        )
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_filter_jobs_by_status(self, client: AsyncClient):
        """Filter jobs by status returns only matching jobs."""
        # Create a job
        await client.post("/api/v1/jobs", json={
            "name": "Filter Test",
            "job_type": "email",
            "payload": {},
        })

        # Filter by queued
        response = await client.get("/api/v1/jobs?status=queued")
        data = response.json()
        for job in data["jobs"]:
            assert job["status"] == "queued"


# ── Job Cancellation ───────────────────────────────────────────────────────────

class TestJobCancellation:
    """Tests DELETE /api/v1/jobs/{id}."""

    @pytest.mark.asyncio
    async def test_cancel_queued_job_returns_204(self, client: AsyncClient):
        """Cancelling a queued job returns 204 No Content."""
        create = await client.post("/api/v1/jobs", json={
            "name": "Cancel Test",
            "job_type": "email",
            "payload": {},
        })
        job_id = create.json()["id"]

        response = await client.delete(f"/api/v1/jobs/{job_id}")
        assert response.status_code == 204

    @pytest.mark.asyncio
    async def test_cancelled_job_status_updated(self, client: AsyncClient):
        """Cancelled job shows CANCELLED status."""
        create = await client.post("/api/v1/jobs", json={
            "name": "Status Update Test",
            "job_type": "email",
            "payload": {},
        })
        job_id = create.json()["id"]

        await client.delete(f"/api/v1/jobs/{job_id}")

        # Verify status updated
        response = await client.get(f"/api/v1/jobs/{job_id}")
        assert response.json()["status"] == "cancelled"

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_job_returns_404(self, client: AsyncClient):
        """Cancelling non-existent job returns 404."""
        response = await client.delete(
            "/api/v1/jobs/00000000-0000-0000-0000-000000000000"
        )
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_cancelled_job_still_in_list(self, client: AsyncClient):
        """Soft delete — cancelled jobs still appear in list."""
        create = await client.post("/api/v1/jobs", json={
            "name": "Soft Delete Test",
            "job_type": "email",
            "payload": {},
        })
        job_id = create.json()["id"]
        await client.delete(f"/api/v1/jobs/{job_id}")

        # Still appears in list
        response = await client.get("/api/v1/jobs")
        job_ids = [j["id"] for j in response.json()["jobs"]]
        assert job_id in job_ids


# ── Health check ───────────────────────────────────────────────────────────────

class TestHealthCheck:
    """Tests /health endpoint."""

    @pytest.mark.asyncio
    async def test_health_returns_200(self, client: AsyncClient):
        response = await client.get("/health")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_health_response_shape(self, client: AsyncClient):
        response = await client.get("/health")
        data = response.json()
        assert data["status"] == "healthy"
        assert "version" in data
        assert "env" in data

    @pytest.mark.asyncio
    async def test_root_endpoint(self, client: AsyncClient):
        response = await client.get("/")
        assert response.status_code == 200
        assert "docs" in response.json()