import json
import random
from locust import HttpUser, task, between, events
from locust.runners import MasterRunner


# ── Job submission user ────────────────────────────────────────────────────────

class JobSubmissionUser(HttpUser):
    """
    Simulates a client submitting jobs to the scheduler.
    Waits 1-3 seconds between requests — realistic API usage.
    """
    wait_time = between(1, 3)
    host = "http://localhost:8000"

    # Track created job IDs for read operations
    job_ids: list[str] = []

    def on_start(self):
        """Called when a simulated user starts."""
        self.job_ids = []

    # ── Write operations ───────────────────────────────────────────────────────

    @task(5)  # weight 5 — most common operation
    def submit_email_job(self):
        """Submit a high-frequency email job."""
        response = self.client.post(
            "/api/v1/jobs",
            json={
                "name": f"Email Job {random.randint(1, 10000)}",
                "job_type": "email",
                "payload": {
                    "to": f"user{random.randint(1, 1000)}@example.com",
                    "subject": "Load test email",
                },
                "priority": random.choice(["high", "medium", "low"]),
            },
            name="/api/v1/jobs [POST email]",
        )
        if response.status_code == 201:
            self.job_ids.append(response.json()["id"])

    @task(2)  # weight 2 — less frequent
    def submit_report_job(self):
        """Submit a report generation job."""
        self.client.post(
            "/api/v1/jobs",
            json={
                "name": f"Report Job {random.randint(1, 10000)}",
                "job_type": "report",
                "payload": {
                    "report_type": random.choice(["monthly", "weekly", "daily"]),
                    "user_id": random.randint(1, 1000),
                },
                "priority": "low",
            },
            name="/api/v1/jobs [POST report]",
        )

    @task(1)  # weight 1 — least frequent
    def submit_data_pipeline_job(self):
        """Submit a data pipeline job."""
        self.client.post(
            "/api/v1/jobs",
            json={
                "name": f"Pipeline Job {random.randint(1, 10000)}",
                "job_type": "data_pipeline",
                "payload": {
                    "source": "postgres",
                    "destination": "s3",
                    "batch_size": 1000,
                },
                "priority": "medium",
            },
            name="/api/v1/jobs [POST pipeline]",
        )

    # ── Read operations ────────────────────────────────────────────────────────

    @task(8)  # weight 8 — reads are most frequent
    def list_jobs(self):
        """List jobs with pagination."""
        page = random.randint(1, 3)
        self.client.get(
            f"/api/v1/jobs?page={page}&page_size=20",
            name="/api/v1/jobs [GET list]",
        )

    @task(3)
    def get_job_by_id(self):
        """Fetch a specific job by ID."""
        if not self.job_ids:
            return
        job_id = random.choice(self.job_ids)
        self.client.get(
            f"/api/v1/jobs/{job_id}",
            name="/api/v1/jobs/{id} [GET]",
        )

    @task(2)
    def get_queue_stats(self):
        """Poll queue depth — simulates dashboard."""
        self.client.get(
            "/api/v1/queue/stats",
            name="/api/v1/queue/stats [GET]",
        )

    @task(1)
    def health_check(self):
        """Health check — simulates load balancer probe."""
        self.client.get("/health", name="/health")


# ── Heavy load user ────────────────────────────────────────────────────────────

class HeavyJobSubmitter(HttpUser):
    """
    Simulates a batch process submitting many jobs rapidly.
    No wait time — hammers the API continuously.
    Used to test rate limiting behavior.
    """
    wait_time = between(0.1, 0.5)
    host = "http://localhost:8000"
    weight = 1  # fewer of these users

    @task
    def rapid_job_submission(self):
        """Submit jobs as fast as possible — tests token bucket."""
        with self.client.post(
            "/api/v1/jobs",
            json={
                "name": "Batch Job",
                "job_type": "email",
                "payload": {"to": "batch@example.com"},
                "priority": "high",
            },
            name="/api/v1/jobs [POST batch]",
            catch_response=True,
        ) as response:
            if response.status_code == 201:
                response.success()
            elif response.status_code == 429:
                # 429 is expected behavior — not a failure
                response.success()
            else:
                response.failure(f"Unexpected status: {response.status_code}")


# ── Event hooks ────────────────────────────────────────────────────────────────

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    print("\n🔱 Norns load test starting...")
    print("   Testing: job submission, retrieval, rate limiting")
    print("   Target:  http://localhost:8000\n")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    stats = environment.stats.total
    print(f"\n🔱 Norns load test complete")
    print(f"   Total requests:  {stats.num_requests}")
    print(f"   Failed requests: {stats.num_failures}")
    print(f"   Avg response:    {stats.avg_response_time:.0f}ms")
    print(f"   Requests/sec:    {stats.current_rps:.1f}")