import time
import structlog
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from redis.asyncio import Redis

from app.config import settings

log = structlog.get_logger()

EXEMPT_PATHS = {"/health", "/metrics", "/", "/docs", "/openapi.json", "/redoc"}


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Hybrid rate limiter:
    - POST /jobs → token bucket (allows bursts, enforces sustained rate)
    - Everything else → sliding window (strict per-minute limit)

    Token bucket:
      capacity = 500 tokens (max burst)
      refill   = 100 tokens/minute
      cost     = 1 token per request

    Sliding window:
      100 requests per 60 seconds per IP
    """

    def __init__(
        self,
        app,
        # Token bucket config
        bucket_capacity: int = 500,
        bucket_refill_rate: int = 100,   # tokens per minute
        # Sliding window config
        window_limit: int = 100,
        window_seconds: int = 60,
    ):
        super().__init__(app)
        self.bucket_capacity   = bucket_capacity
        self.bucket_refill_rate = bucket_refill_rate
        self.window_limit      = window_limit
        self.window_seconds    = window_seconds

    async def dispatch(self, request: Request, call_next) -> Response:
        if request.url.path in EXEMPT_PATHS:
            return await call_next(request)

        client_ip = request.client.host

        try:
            redis = Redis.from_url(
                settings.redis_url,
                encoding="utf-8",
                decode_responses=True,
            )

            # Route to correct algorithm
            if request.method == "POST" and request.url.path == "/api/v1/jobs":
                allowed, headers = await self._token_bucket(redis, client_ip)
            else:
                allowed, headers = await self._sliding_window(redis, client_ip)

            await redis.aclose()

            if not allowed:
                log.warning(
                    "rate_limit_exceeded",
                    client_ip=client_ip,
                    path=request.url.path,
                    method=request.method,
                )
                return JSONResponse(
                    status_code=429,
                    content={
                        "error": "Too Many Requests",
                        "detail": "Rate limit exceeded. Check X-RateLimit headers.",
                        "retry_after": self.window_seconds,
                    },
                    headers=headers,
                )

            response = await call_next(request)
            for key, value in headers.items():
                response.headers[key] = value
            return response

        except Exception as e:
            log.error("rate_limit_error", error=str(e))
            return await call_next(request)

    # ── Token Bucket ───────────────────────────────────────────────────────────

    async def _token_bucket(
        self, redis: Redis, client_ip: str
    ) -> tuple[bool, dict]:
        """
        Token bucket algorithm using Redis hash.
        Stores: {tokens: float, last_refill: float}

        On each request:
        1. Calculate tokens earned since last refill
        2. Add earned tokens (capped at capacity)
        3. If tokens >= 1: consume 1, allow request
        4. Else: reject
        """
        key = f"tokenbucket:{client_ip}"
        now = time.time()

        # Get current bucket state
        bucket = await redis.hgetall(key)

        if not bucket:
            # First request — initialize full bucket
            tokens      = float(self.bucket_capacity) - 1
            last_refill = now
        else:
            tokens      = float(bucket["tokens"])
            last_refill = float(bucket["last_refill"])

            # Calculate tokens earned since last request
            elapsed        = now - last_refill
            refill_per_sec = self.bucket_refill_rate / 60.0
            earned         = elapsed * refill_per_sec
            tokens         = min(self.bucket_capacity, tokens + earned)

            if tokens >= 1:
                tokens -= 1  # consume one token
            else:
                # Bucket empty — reject
                await redis.hset(key, mapping={
                    "tokens": tokens,
                    "last_refill": now,
                })
                await redis.expire(key, 3600)
                headers = {
                    "X-RateLimit-Limit":     str(self.bucket_capacity),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Algorithm": "token-bucket",
                    "Retry-After":           "1",
                }
                return False, headers

        # Save updated bucket state
        await redis.hset(key, mapping={
            "tokens":      tokens,
            "last_refill": now,
        })
        await redis.expire(key, 3600)

        headers = {
            "X-RateLimit-Limit":     str(self.bucket_capacity),
            "X-RateLimit-Remaining": str(int(tokens)),
            "X-RateLimit-Algorithm": "token-bucket",
        }
        return True, headers

    # ── Sliding Window ─────────────────────────────────────────────────────────

    async def _sliding_window(
        self, redis: Redis, client_ip: str
    ) -> tuple[bool, dict]:
        """
        Sliding window using Redis sorted set.
        Score = timestamp, member = timestamp string.
        Count members within last window_seconds.
        """
        key          = f"ratelimit:{client_ip}"
        now          = time.time()
        window_start = now - self.window_seconds

        async with redis.pipeline(transaction=True) as pipe:
            pipe.zremrangebyscore(key, 0, window_start)
            pipe.zadd(key, {str(now): now})
            pipe.zcard(key)
            pipe.expire(key, self.window_seconds)
            results = await pipe.execute()

        request_count = results[2]
        remaining     = max(0, self.window_limit - request_count)
        reset_time    = int(now) + self.window_seconds

        headers = {
            "X-RateLimit-Limit":     str(self.window_limit),
            "X-RateLimit-Remaining": str(remaining),
            "X-RateLimit-Reset":     str(reset_time),
            "X-RateLimit-Algorithm": "sliding-window",
        }

        if request_count > self.window_limit:
            return False, headers

        return True, headers