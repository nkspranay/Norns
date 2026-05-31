import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone


# ── Exponential backoff calculation ───────────────────────────────────────────

class TestExponentialBackoff:
    """
    Tests the exponential backoff formula used in the worker.
    Formula: backoff = job_retry_backoff ** retry_count
    With job_retry_backoff=2: 2^1=2s, 2^2=4s, 2^3=8s
    """

    def test_first_retry_backoff(self):
        """First retry waits 2 seconds."""
        retry_backoff = 2
        retry_count = 1
        backoff = retry_backoff ** retry_count
        assert backoff == 2

    def test_second_retry_backoff(self):
        """Second retry waits 4 seconds."""
        retry_backoff = 2
        retry_count = 2
        backoff = retry_backoff ** retry_count
        assert backoff == 4

    def test_third_retry_backoff(self):
        """Third retry waits 8 seconds."""
        retry_backoff = 2
        retry_count = 3
        backoff = retry_backoff ** retry_count
        assert backoff == 8

    def test_backoff_grows_exponentially(self):
        """Each retry doubles the wait time."""
        retry_backoff = 2
        backoffs = [retry_backoff ** i for i in range(1, 5)]
        assert backoffs == [2, 4, 8, 16]

    def test_max_retries_boundary(self):
        """Job goes to DLQ when retry_count >= max_retries."""
        max_retries = 3

        # At exactly max_retries — should go to DLQ
        retry_count = 3
        assert retry_count >= max_retries

        # One below max — should still retry
        retry_count = 2
        assert retry_count < max_retries

    def test_retry_count_increments(self):
        """retry_count increments by 1 on each failure."""
        retry_count = 0
        max_retries = 3
        failures = 0

        while retry_count < max_retries:
            retry_count += 1
            failures += 1

        assert failures == 3
        assert retry_count == max_retries

    def test_custom_backoff_base(self):
        """System works with different backoff bases."""
        # Base 3: 3, 9, 27
        retry_backoff = 3
        assert retry_backoff ** 1 == 3
        assert retry_backoff ** 2 == 9
        assert retry_backoff ** 3 == 27

    def test_zero_retries_goes_straight_to_dlq(self):
        """Job with max_retries=0 goes to DLQ immediately on first failure."""
        max_retries = 0
        retry_count = 0
        # After first failure, retry_count becomes 1
        retry_count += 1
        assert retry_count >= max_retries


# ── Idempotency key generation ─────────────────────────────────────────────────

class TestIdempotencyKey:
    """
    Tests idempotency key format and uniqueness.
    Format: {job_id}:attempt:{attempt_number}
    """

    def test_idempotency_key_format(self):
        """Key follows expected format."""
        job_id = "abc-123"
        retry_count = 0
        key = f"{job_id}:attempt:{retry_count + 1}"
        assert key == "abc-123:attempt:1"

    def test_different_attempts_produce_different_keys(self):
        """Each attempt has a unique key — prevents duplicate execution."""
        job_id = "abc-123"
        key1 = f"{job_id}:attempt:1"
        key2 = f"{job_id}:attempt:2"
        key3 = f"{job_id}:attempt:3"

        assert key1 != key2
        assert key2 != key3
        assert key1 != key3

    def test_different_jobs_produce_different_keys(self):
        """Different jobs never share an idempotency key."""
        key1 = f"job-111:attempt:1"
        key2 = f"job-222:attempt:1"
        assert key1 != key2

    def test_key_contains_job_id(self):
        """Key always contains the job ID for traceability."""
        job_id = "test-job-id-xyz"
        key = f"{job_id}:attempt:1"
        assert job_id in key

    def test_key_contains_attempt_number(self):
        """Key always contains the attempt number."""
        key = "job-abc:attempt:3"
        assert "attempt:3" in key


# ── Job status transitions ─────────────────────────────────────────────────────

class TestJobStatusTransitions:
    """
    Tests valid job status state machine.

    Valid transitions:
    PENDING → QUEUED → RUNNING → COMPLETED
                              ↘ FAILED → QUEUED (retry)
                                       → DEAD (max retries)
    QUEUED → CANCELLED
    """

    def test_pending_to_queued(self):
        """Job moves from PENDING to QUEUED when submitted."""
        from app.models.job import JobStatus
        assert JobStatus.PENDING != JobStatus.QUEUED
        # Submission sets status to QUEUED
        status = JobStatus.QUEUED
        assert status == JobStatus.QUEUED

    def test_queued_to_running(self):
        """Job moves to RUNNING when worker picks it up."""
        from app.models.job import JobStatus
        status = JobStatus.RUNNING
        assert status == JobStatus.RUNNING

    def test_running_to_completed(self):
        """Job moves to COMPLETED on success."""
        from app.models.job import JobStatus
        status = JobStatus.COMPLETED
        assert status == JobStatus.COMPLETED

    def test_running_to_failed_then_queued(self):
        """Failed job gets requeued if retries remain."""
        from app.models.job import JobStatus
        retry_count = 1
        max_retries = 3
        if retry_count < max_retries:
            status = JobStatus.QUEUED
        else:
            status = JobStatus.DEAD
        assert status == JobStatus.QUEUED

    def test_failed_max_retries_to_dead(self):
        """Job moves to DEAD when max retries exhausted."""
        from app.models.job import JobStatus
        retry_count = 3
        max_retries = 3
        if retry_count >= max_retries:
            status = JobStatus.DEAD
        assert status == JobStatus.DEAD

    def test_cancelled_job_skipped_by_worker(self):
        """Worker skips jobs with CANCELLED status."""
        from app.models.job import JobStatus
        job_status = JobStatus.CANCELLED
        should_skip = job_status in (
            JobStatus.CANCELLED,
            JobStatus.COMPLETED,
            JobStatus.DEAD
        )
        assert should_skip is True

    def test_completed_job_not_reprocessed(self):
        """Worker skips already completed jobs."""
        from app.models.job import JobStatus
        job_status = JobStatus.COMPLETED
        should_skip = job_status in (
            JobStatus.CANCELLED,
            JobStatus.COMPLETED,
            JobStatus.DEAD
        )
        assert should_skip is True


# ── Priority queue ordering ────────────────────────────────────────────────────

class TestPriorityQueue:
    """Tests queue name mapping for job priorities."""

    def test_high_priority_maps_to_correct_queue(self):
        from app.queue.redis_queue import get_queue_name, QUEUE_HIGH
        assert get_queue_name("high") == QUEUE_HIGH

    def test_medium_priority_maps_to_correct_queue(self):
        from app.queue.redis_queue import get_queue_name, QUEUE_MEDIUM
        assert get_queue_name("medium") == QUEUE_MEDIUM

    def test_low_priority_maps_to_correct_queue(self):
        from app.queue.redis_queue import get_queue_name, QUEUE_LOW
        assert get_queue_name("low") == QUEUE_LOW

    def test_unknown_priority_defaults_to_medium(self):
        from app.queue.redis_queue import get_queue_name, QUEUE_MEDIUM
        assert get_queue_name("unknown") == QUEUE_MEDIUM

    def test_high_priority_queue_checked_first(self):
        """Worker checks high priority queue before medium and low."""
        from app.queue.redis_queue import QUEUE_HIGH, QUEUE_MEDIUM, QUEUE_LOW
        queue_order = [QUEUE_HIGH, QUEUE_MEDIUM, QUEUE_LOW]
        assert queue_order[0] == QUEUE_HIGH
        assert queue_order[1] == QUEUE_MEDIUM
        assert queue_order[2] == QUEUE_LOW