"""Rate limiting for API requests using token bucket algorithm.

Includes:
- Token bucket rate limiting via aiolimiter
- Exponential backoff for 429 responses
- Registry for managing multiple limiters
"""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import wraps
from typing import Any, Callable, TypeVar

import httpx
import structlog
from aiolimiter import AsyncLimiter
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    RetryError,
)

logger = structlog.get_logger()

T = TypeVar("T")


class RateLimitExceeded(Exception):
    """Raised when an external API returns 429 Too Many Requests.
    
    Attributes:
        retry_after: Seconds to wait before retrying (from Retry-After header)
        api_name: Name of the API that rate limited us
    """
    
    def __init__(
        self,
        message: str = "Rate limit exceeded",
        retry_after: int | None = None,
        api_name: str | None = None,
    ):
        super().__init__(message)
        self.retry_after = retry_after
        self.api_name = api_name



def with_backoff(
    max_attempts: int = 5,
    min_wait: float = 1.0,
    max_wait: float = 60.0,
    retry_on: tuple[type[Exception], ...] = (RateLimitExceeded,),
):
    """Decorator for automatic retry with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts
        min_wait: Minimum wait time between retries (seconds)
        max_wait: Maximum wait time between retries (seconds)
        retry_on: Exception types to retry on
        
    Returns:
        Decorated async function with retry logic
        
    Example:
        @with_backoff(max_attempts=3, min_wait=2.0)
        async def fetch_data(url: str) -> dict:
            response = await client.get(url)
            await check_response_rate_limit(response)
            return response.json()
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        @retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
            retry=retry_if_exception_type(retry_on),
            reraise=True,
        )
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            return await func(*args, **kwargs)
        return wrapper
    return decorator


async def check_response_rate_limit(
    response: httpx.Response,
    api_name: str | None = None,
) -> None:
    """Check HTTP response for rate limiting and raise if limited.
    
    Call this after making an HTTP request to handle 429 responses properly.
    
    Args:
        response: HTTP response to check
        api_name: Name of the API for logging
        
    Raises:
        RateLimitExceeded: If response is 429
    """
    if response.status_code == 429:
        retry_after = None
        retry_header = response.headers.get("Retry-After")
        
        if retry_header:
            try:
                retry_after = int(retry_header)
            except ValueError:
                # Could be an HTTP-date, but we'll just use default backoff
                pass
        
        logger.warning(
            "External API rate limited",
            api=api_name,
            retry_after=retry_after,
            status_code=429,
        )
        
        raise RateLimitExceeded(
            message=f"Rate limited by {api_name or 'external API'}",
            retry_after=retry_after,
            api_name=api_name,
        )


async def wait_for_rate_limit(response: httpx.Response) -> bool:
    """Wait if rate limited and return whether we should retry.
    
    Args:
        response: HTTP response to check
        
    Returns:
        True if we waited and should retry, False if not rate limited
    """
    if response.status_code != 429:
        return False
    
    retry_after = response.headers.get("Retry-After")
    
    if retry_after:
        try:
            wait_seconds = int(retry_after)
        except ValueError:
            wait_seconds = 60  # Default if header is malformed
    else:
        wait_seconds = 60  # Default wait
    
    # Cap at 5 minutes
    wait_seconds = min(wait_seconds, 300)
    
    logger.info(f"Rate limited, waiting {wait_seconds} seconds")
    await asyncio.sleep(wait_seconds)
    return True


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""

    requests_per_minute: int
    burst_size: int | None = None  # None means same as requests_per_minute

    @property
    def effective_burst(self) -> int:
        """Get effective burst size."""
        return self.burst_size or self.requests_per_minute


class RateLimiter:
    """Rate limiter using token bucket algorithm with async support."""

    def __init__(self, name: str, config: RateLimitConfig):
        """Initialize the rate limiter.

        Args:
            name: Name for logging purposes
            config: Rate limit configuration
        """
        self.name = name
        self.config = config
        self.logger = logger.bind(rate_limiter=name)

        # Create the async limiter
        # aiolimiter uses time_period in seconds
        self._limiter = AsyncLimiter(
            max_rate=config.effective_burst,
            time_period=60.0 / config.requests_per_minute * config.effective_burst,
        )

        self._request_count = 0
        self._last_reset = datetime.now(UTC)

    async def acquire(self) -> None:
        """Acquire a rate limit token, waiting if necessary."""
        await self._limiter.acquire()
        self._request_count += 1

        if self._request_count % 100 == 0:
            self.logger.debug(
                "Rate limiter status",
                requests_made=self._request_count,
                since=self._last_reset.isoformat(),
            )

    async def __aenter__(self) -> "RateLimiter":
        """Context manager entry - acquires token."""
        await self.acquire()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        pass

    def reset_counter(self) -> None:
        """Reset the request counter (for logging purposes)."""
        self._request_count = 0
        self._last_reset = datetime.now(UTC)


class RateLimiterRegistry:
    """Registry for managing multiple rate limiters."""

    _instance: "RateLimiterRegistry | None" = None
    _limiters: dict[str, RateLimiter]

    def __new__(cls) -> "RateLimiterRegistry":
        """Singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._limiters = {}
        return cls._instance

    def get_or_create(self, name: str, config: RateLimitConfig) -> RateLimiter:
        """Get an existing rate limiter or create a new one.

        Args:
            name: Name of the rate limiter
            config: Configuration (used only if creating new)

        Returns:
            The rate limiter instance
        """
        if name not in self._limiters:
            self._limiters[name] = RateLimiter(name, config)
            logger.info(
                "Created rate limiter",
                name=name,
                requests_per_minute=config.requests_per_minute,
            )
        return self._limiters[name]

    def get(self, name: str) -> RateLimiter | None:
        """Get an existing rate limiter.

        Args:
            name: Name of the rate limiter

        Returns:
            The rate limiter or None if not found
        """
        return self._limiters.get(name)

    def reset(self, name: str | None = None) -> None:
        """Reset rate limiter(s) - useful for testing.

        This removes the limiter from the registry, allowing a fresh
        instance to be created on next access. This avoids the
        AsyncLimiter re-use across event loops warning.

        Args:
            name: Name of specific limiter to reset, or None to reset all
        """
        if name is None:
            self._limiters.clear()
        elif name in self._limiters:
            del self._limiters[name]

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance - for testing only.

        This completely clears the registry, ensuring fresh limiters
        are created in new event loops.
        """
        if cls._instance is not None:
            cls._instance._limiters.clear()
            cls._instance = None


API_RATE_LIMITS: dict[str, RateLimitConfig] = {
    "fred": RateLimitConfig(requests_per_minute=120, burst_size=10),
    "bls": RateLimitConfig(requests_per_minute=25, burst_size=5),
    "bea": RateLimitConfig(requests_per_minute=100, burst_size=10),
    "eia": RateLimitConfig(requests_per_minute=60, burst_size=10),
    "census": RateLimitConfig(requests_per_minute=50, burst_size=5),
    "sec_edgar": RateLimitConfig(requests_per_minute=10, burst_size=2),
    "gdelt": RateLimitConfig(requests_per_minute=30, burst_size=5),
    "google_trends": RateLimitConfig(requests_per_minute=10, burst_size=2),
    "nfib": RateLimitConfig(requests_per_minute=30, burst_size=5),
    "port_congestion": RateLimitConfig(requests_per_minute=10, burst_size=3),
}


def get_rate_limiter(api_name: str) -> RateLimiter:
    """Get a rate limiter for a specific API.

    Args:
        api_name: Name of the API (e.g., "fred", "bls")

    Returns:
        Configured rate limiter

    Raises:
        ValueError: If API name is not recognized
    """
    if api_name not in API_RATE_LIMITS:
        raise ValueError(f"Unknown API: {api_name}. Known APIs: {list(API_RATE_LIMITS.keys())}")

    registry = RateLimiterRegistry()
    return registry.get_or_create(api_name, API_RATE_LIMITS[api_name])


async def rate_limited_request(
    api_name: str,
    request_func: Any,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Execute a request with rate limiting.

    Args:
        api_name: Name of the API for rate limiting
        request_func: Async function to execute
        *args: Positional arguments for request_func
        **kwargs: Keyword arguments for request_func

    Returns:
        Result of request_func
    """
    limiter = get_rate_limiter(api_name)
    async with limiter:
        return await request_func(*args, **kwargs)


async def rate_limited_request_with_backoff(
    api_name: str,
    request_func: Callable[..., Any],
    *args: Any,
    max_attempts: int = 5,
    **kwargs: Any,
) -> Any:
    """Execute a request with rate limiting and automatic backoff on 429.
    
    Combines local rate limiting with handling of external 429 responses.

    Args:
        api_name: Name of the API for rate limiting
        request_func: Async function that returns an httpx.Response
        *args: Positional arguments for request_func
        max_attempts: Maximum retry attempts on 429
        **kwargs: Keyword arguments for request_func

    Returns:
        Result of request_func (should be httpx.Response)
        
    Raises:
        RateLimitExceeded: If max attempts exceeded
    """
    limiter = get_rate_limiter(api_name)
    attempts = 0
    
    while attempts < max_attempts:
        async with limiter:
            response = await request_func(*args, **kwargs)
            
            if response.status_code == 429:
                attempts += 1
                
                if attempts >= max_attempts:
                    raise RateLimitExceeded(
                        f"Rate limit exceeded after {max_attempts} attempts",
                        api_name=api_name,
                    )
                
                # Wait and retry
                await wait_for_rate_limit(response)
                continue
            
            return response
    
    raise RateLimitExceeded(f"Rate limit exceeded for {api_name}", api_name=api_name)
