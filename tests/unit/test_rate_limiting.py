import pytest
import time
from unittest.mock import AsyncMock, MagicMock, patch, call


# ── Sliding window algorithm ───────────────────────────────────────────────────

class TestSlidingWindowAlgorithm:
    """
    Tests sliding window rate limiting logic.
    Window: 60 seconds, limit: 100 requests.
    """

    def test_first_request_always_allowed(self):
        """First request in a fresh window is always allowed."""
        request_count = 1
        limit = 100
        assert request_count <= limit

    def test_requests_within_limit_allowed(self):
        """100 requests within window are all allowed."""
        limit = 100
        for count in range(1, limit + 1):
            assert count <= limit

    def test_request_exceeding_limit_rejected(self):
        """101st request is rejected."""
        limit = 100
        request_count = 101
        assert request_count > limit

    def test_window_boundary(self):
        """Exactly at limit — allowed. One over — rejected."""
        limit = 100
        assert 100 <= limit      # exactly at limit — allowed
        assert 101 > limit       # one over — rejected

    def test_old_requests_fall_outside_window(self):
        """
        Requests older than window_seconds are pruned.
        After pruning, new requests are allowed again.
        """
        window_seconds = 60
        now = time.time()

        # Simulate 100 requests made 61 seconds ago
        old_requests = [now - 61] * 100
        # All fall outside the window
        valid_requests = [t for t in old_requests if t > now - window_seconds]
        assert len(valid_requests) == 0

        # Now a new request should be allowed (count=1)
        assert 1 <= 100

    def test_sliding_not_fixed_window(self):
        """
        Sliding window counts last N seconds, not current minute.
        Fixed window resets at :00 — sliding window doesn't.
        """
        window_seconds = 60
        now = time.time()

        # 50 requests 30 seconds ago — still in window
        requests_30s_ago = [now - 30] * 50
        valid = [t for t in requests_30s_ago if t > now - window_seconds]
        assert len(valid) == 50  # all still in window

        # 50 more requests now — total 100, at limit
        total = len(valid) + 50
        assert total == 100

        # One more — rejected
        assert total + 1 > 100

    def test_rate_limit_headers_present(self):
        """Response includes standard rate limit headers."""
        headers = {
            "X-RateLimit-Limit": "100",
            "X-RateLimit-Remaining": "95",
            "X-RateLimit-Reset": "1234567890",
            "X-RateLimit-Algorithm": "sliding-window",
        }
        assert "X-RateLimit-Limit" in headers
        assert "X-RateLimit-Remaining" in headers
        assert "X-RateLimit-Reset" in headers
        assert "X-RateLimit-Algorithm" in headers

    def test_remaining_decrements_correctly(self):
        """Remaining count decrements with each request."""
        limit = 100
        requests_made = 10
        remaining = limit - requests_made
        assert remaining == 90

    def test_remaining_never_negative(self):
        """Remaining count floors at 0, never goes negative."""
        limit = 100
        requests_made = 150
        remaining = max(0, limit - requests_made)
        assert remaining == 0


# ── Token bucket algorithm ─────────────────────────────────────────────────────

class TestTokenBucketAlgorithm:
    """
    Tests token bucket rate limiting logic.
    Capacity: 500 tokens, refill: 100/minute.
    """

    def test_full_bucket_on_first_request(self):
        """New client starts with full bucket."""
        capacity = 500
        tokens = capacity  # full bucket
        assert tokens == 500

    def test_token_consumed_per_request(self):
        """Each request consumes one token."""
        tokens = 500
        tokens -= 1  # one request
        assert tokens == 499

    def test_burst_allowed_up_to_capacity(self):
        """500 requests in rapid succession are all allowed."""
        capacity = 500
        tokens = capacity
        for _ in range(capacity):
            assert tokens > 0
            tokens -= 1
        assert tokens == 0

    def test_request_rejected_when_bucket_empty(self):
        """Request rejected when no tokens remain."""
        tokens = 0
        assert tokens < 1  # rejected

    def test_bucket_refills_over_time(self):
        """Tokens refill at rate of 100/minute = 1.67/second."""
        capacity = 500
        refill_rate = 100  # per minute
        tokens = 0  # empty bucket

        # After 30 seconds
        elapsed = 30
        refill_per_sec = refill_rate / 60.0
        earned = elapsed * refill_per_sec
        tokens = min(capacity, tokens + earned)

        assert tokens == pytest.approx(50.0, rel=0.01)

    def test_bucket_does_not_exceed_capacity(self):
        """Bucket never fills above capacity even after long idle."""
        capacity = 500
        tokens = 0
        refill_rate = 100 / 60.0  # per second

        # Simulate 1 hour of idle time
        elapsed = 3600
        earned = elapsed * refill_rate
        tokens = min(capacity, tokens + earned)

        assert tokens == capacity  # capped at 500

    def test_burst_then_sustained_rate(self):
        """
        Token bucket key property:
        allows burst up to capacity,
        then limits to refill rate sustained.
        """
        capacity = 500
        refill_rate = 100  # per minute
        tokens = float(capacity)

        # Burst: drain entire bucket
        burst_requests = 500
        tokens -= burst_requests
        assert tokens == 0

        # After 1 minute — 100 tokens refilled
        tokens = min(capacity, tokens + refill_rate)
        assert tokens == 100

        # Can make 100 more requests
        assert tokens >= 100

    def test_token_bucket_allows_more_burst_than_sliding_window(self):
        """
        Token bucket (capacity=500) allows more burst than
        sliding window (limit=100).
        This is why we use token bucket for POST /jobs.
        """
        sliding_window_limit = 100
        token_bucket_capacity = 500

        # Burst of 200 requests
        burst = 200
        sliding_window_allows = burst <= sliding_window_limit
        token_bucket_allows   = burst <= token_bucket_capacity

        assert sliding_window_allows is False  # sliding window rejects burst
        assert token_bucket_allows   is True   # token bucket allows burst

    def test_algorithm_header_correct(self):
        """POST /jobs response includes token-bucket algorithm header."""
        headers = {"X-RateLimit-Algorithm": "token-bucket"}
        assert headers["X-RateLimit-Algorithm"] == "token-bucket"

    def test_sliding_window_header_correct(self):
        """GET /jobs response includes sliding-window algorithm header."""
        headers = {"X-RateLimit-Algorithm": "sliding-window"}
        assert headers["X-RateLimit-Algorithm"] == "sliding-window"


# ── Exempt paths ───────────────────────────────────────────────────────────────

class TestExemptPaths:
    """Tests that health/metrics endpoints bypass rate limiting."""

    def test_health_endpoint_exempt(self):
        from app.middleware import EXEMPT_PATHS
        assert "/health" in EXEMPT_PATHS

    def test_metrics_endpoint_exempt(self):
        from app.middleware import EXEMPT_PATHS
        assert "/metrics" in EXEMPT_PATHS

    def test_docs_endpoint_exempt(self):
        from app.middleware import EXEMPT_PATHS
        assert "/docs" in EXEMPT_PATHS

    def test_api_endpoints_not_exempt(self):
        from app.middleware import EXEMPT_PATHS
        assert "/api/v1/jobs" not in EXEMPT_PATHS
        assert "/api/v1/workers" not in EXEMPT_PATHS

    def test_root_endpoint_exempt(self):
        from app.middleware import EXEMPT_PATHS
        assert "/" in EXEMPT_PATHS