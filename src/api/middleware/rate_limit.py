"""FastAPI rate limiting middleware using Redis."""

from functools import wraps
from typing import Callable

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from src.storage.redis_cache import RedisCache

logger = structlog.get_logger()


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Rate limiting middleware using Redis for distributed rate limiting.
    
    Uses a sliding window counter algorithm via Redis INCR with TTL.
    """

    # Paths to skip rate limiting
    SKIP_PATHS = frozenset(["/", "/health", "/metrics", "/docs", "/redoc", "/openapi.json"])
    SKIP_PREFIXES = ("/api/data/series/", "/api/events")

    def __init__(
        self,
        app,
        redis_cache: RedisCache,
        limit: int = 100,
        window: int = 60,
        enabled: bool = True,
    ):
        """Initialize rate limit middleware.
        
        Args:
            app: ASGI application
            redis_cache: Redis cache instance for rate limit storage
            limit: Maximum requests per window
            window: Time window in seconds
            enabled: Whether rate limiting is enabled
        """
        super().__init__(app)
        self.redis_cache = redis_cache
        self.limit = limit
        self.window = window
        self.enabled = enabled
        self.logger = logger.bind(component="RateLimitMiddleware")

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process the request with rate limiting.
        
        Args:
            request: Incoming HTTP request
            call_next: Next middleware/handler
            
        Returns:
            HTTP response
        """
        if not self.enabled:
            return await call_next(request)

        # Skip rate limiting for health checks, docs, and read-heavy data paths
        if request.url.path in self.SKIP_PATHS:
            return await call_next(request)
        if any(request.url.path.startswith(p) for p in self.SKIP_PREFIXES):
            return await call_next(request)

        client_ip = self._get_client_ip(request)
        rate_limit_key = f"ratelimit:api:{client_ip}"

        try:
            if not self.redis_cache.is_connected:
                # If Redis is down, allow request but log warning
                self.logger.warning("Redis not connected, skipping rate limit check")
                return await call_next(request)

            allowed, remaining = await self.redis_cache.check_rate_limit(
                key=rate_limit_key,
                max_requests=self.limit,
                window_seconds=self.window,
            )

            response = await call_next(request) if allowed else self._rate_limit_response(remaining)
            
            response.headers["X-RateLimit-Limit"] = str(self.limit)
            response.headers["X-RateLimit-Remaining"] = str(remaining)
            response.headers["X-RateLimit-Reset"] = str(self.window)

            if not allowed:
                self.logger.warning(
                    "Rate limit exceeded",
                    client_ip=client_ip,
                    limit=self.limit,
                    path=request.url.path,
                )

            return response

        except Exception as e:
            # On error, allow request but log
            self.logger.error("Rate limit check failed", error=str(e))
            return await call_next(request)

    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP from request, handling proxies.
        
        Args:
            request: HTTP request
            
        Returns:
            Client IP address
        """
        # Check for forwarded headers (when behind proxy/load balancer)
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            # Take the first IP (original client)
            return forwarded_for.split(",")[0].strip()

        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip.strip()

        if request.client:
            return request.client.host

        return "unknown"

    def _rate_limit_response(self, remaining: int) -> JSONResponse:
        """Create a 429 Too Many Requests response.
        
        Args:
            remaining: Remaining requests (should be 0)
            
        Returns:
            JSON response with error details
        """
        return JSONResponse(
            status_code=429,
            content={
                "detail": "Rate limit exceeded. Try again later.",
                "retry_after": self.window,
            },
            headers={"Retry-After": str(self.window)},
        )


# Endpoint-specific rate limit storage
_endpoint_limits: dict[str, tuple[int, int]] = {}


def rate_limit(limit: int, window: int = 60):
    """Decorator for custom rate limits on specific endpoints.
    
    This decorator marks an endpoint with custom rate limits that can be
    enforced by a more advanced rate limiting system.
    
    Args:
        limit: Maximum requests per window
        window: Time window in seconds
        
    Returns:
        Decorated function with rate limit metadata
        
    Example:
        @rate_limit(limit=10, window=60)
        @router.post("/forecasts/generate")
        async def generate_forecast(...):
            ...
    """
    def decorator(func: Callable) -> Callable:
        # Store rate limit config as function attribute
        func._rate_limit_config = (limit, window)  # type: ignore[attr-defined]
        
        # Also store in module-level dict for introspection
        endpoint_name = f"{func.__module__}.{func.__qualname__}"
        _endpoint_limits[endpoint_name] = (limit, window)
        
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # The actual rate limiting is handled by middleware
            # This decorator just marks the endpoint
            return await func(*args, **kwargs)
        
        # Preserve the rate limit config on wrapper
        wrapper._rate_limit_config = (limit, window)  # type: ignore[attr-defined]
        return wrapper
    
    return decorator


def get_endpoint_limits() -> dict[str, tuple[int, int]]:
    """Get all registered endpoint-specific rate limits.
    
    Returns:
        Dict mapping endpoint names to (limit, window) tuples
    """
    return _endpoint_limits.copy()
