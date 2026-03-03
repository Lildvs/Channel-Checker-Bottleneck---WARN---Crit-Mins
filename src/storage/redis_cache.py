"""Redis cache operations for hot data and pub/sub."""

import functools
import json
from collections.abc import Callable
from datetime import timedelta
from typing import Any

import redis.asyncio as redis
import structlog

from src.config.settings import get_settings

logger = structlog.get_logger()


def _redis_safe(default_return=None):
    """Decorator that catches Redis connection/timeout errors and returns a safe default.

    Logs the failure and returns default_return instead of propagating the exception.
    This ensures Redis downtime degrades gracefully rather than crashing callers.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except (redis.ConnectionError, redis.TimeoutError, ConnectionRefusedError, OSError) as e:
                logger.warning(
                    "Redis operation failed (degraded mode)",
                    operation=func.__name__,
                    error=str(e),
                )
                return default_return
        return wrapper
    return decorator


class RedisCache:
    """Async Redis cache for hot data and real-time updates."""

    def __init__(self, redis_url: str | None = None):
        """Initialize Redis connection.

        Args:
            redis_url: Redis URL (uses settings if not provided)
        """
        settings = get_settings()
        self.redis_url = redis_url or settings.redis_url
        self._client: redis.Redis | None = None
        self._pubsub: redis.client.PubSub | None = None

    async def connect(self) -> None:
        """Connect to Redis."""
        self._client = redis.from_url(
            self.redis_url,
            encoding="utf-8",
            decode_responses=True,
        )
        await self._client.ping()
        logger.info("Connected to Redis")

    async def close(self) -> None:
        """Close Redis connection."""
        if self._pubsub:
            await self._pubsub.close()
        if self._client:
            await self._client.close()

    @property
    def client(self) -> redis.Redis:
        """Get the Redis client."""
        if self._client is None:
            raise RuntimeError(
                "Redis not connected. Call connect() first or use init_cache() at startup."
            )
        return self._client

    @property
    def is_connected(self) -> bool:
        """Check if Redis is connected."""
        return self._client is not None

    @_redis_safe(default_return=None)
    async def get(self, key: str) -> str | None:
        """Get a value from cache."""
        return await self.client.get(key)

    async def set(
        self,
        key: str,
        value: str,
        expire: timedelta | None = None,
    ) -> None:
        """Set a value in cache."""
        if expire:
            await self.client.setex(key, expire, value)
        else:
            await self.client.set(key, value)

    @_redis_safe()
    async def delete(self, key: str) -> None:
        """Delete a key from cache."""
        await self.client.delete(key)

    async def exists(self, key: str) -> bool:
        """Check if a key exists."""
        return bool(await self.client.exists(key))

    @_redis_safe(default_return=None)
    async def get_json(self, key: str) -> dict[str, Any] | list[Any] | None:
        """Get a JSON value from cache."""
        value = await self.get(key)
        if value:
            return json.loads(value)
        return None

    async def set_json(
        self,
        key: str,
        value: dict[str, Any] | list[Any],
        expire: timedelta | None = None,
    ) -> None:
        """Set a JSON value in cache."""
        await self.set(key, json.dumps(value, default=str), expire)

    async def cache_series_latest(
        self,
        series_id: str,
        value: float,
        timestamp: str,
        expire: timedelta = timedelta(hours=1),
    ) -> None:
        """Cache the latest value for a series."""
        key = f"series:latest:{series_id}"
        await self.set_json(
            key,
            {"value": value, "timestamp": timestamp, "series_id": series_id},
            expire,
        )

    async def get_series_latest(self, series_id: str) -> dict[str, Any] | None:
        """Get the cached latest value for a series."""
        key = f"series:latest:{series_id}"
        return await self.get_json(key)

    async def cache_active_bottlenecks(
        self,
        bottlenecks: list[dict[str, Any]],
        expire: timedelta = timedelta(minutes=15),
    ) -> None:
        """Cache the list of active bottlenecks."""
        await self.set_json("bottlenecks:active", bottlenecks, expire)

    async def get_active_bottlenecks(self) -> list[dict[str, Any]]:
        """Get cached active bottlenecks."""
        result = await self.get_json("bottlenecks:active")
        return result if isinstance(result, list) else []

    @_redis_safe(default_return=0)
    async def publish(self, channel: str, message: dict[str, Any]) -> int:
        """Publish a message to a channel.

        Args:
            channel: Channel name
            message: Message to publish

        Returns:
            Number of subscribers that received the message
        """
        return await self.client.publish(channel, json.dumps(message, default=str))

    async def subscribe(self, *channels: str) -> redis.client.PubSub:
        """Subscribe to channels.

        Args:
            channels: Channel names to subscribe to

        Returns:
            PubSub instance
        """
        self._pubsub = self.client.pubsub()
        await self._pubsub.subscribe(*channels)
        return self._pubsub

    async def publish_bottleneck_alert(self, bottleneck: dict[str, Any]) -> None:
        """Publish a bottleneck alert.

        Args:
            bottleneck: Bottleneck data to publish
        """
        await self.publish("bottleneck_alerts", bottleneck)
        logger.info(
            "Published bottleneck alert",
            bottleneck_id=bottleneck.get("id"),
            category=bottleneck.get("category"),
        )

    async def publish_data_update(
        self,
        series_id: str,
        value: float,
        timestamp: str,
    ) -> None:
        """Publish a data update notification.

        Args:
            series_id: Series that was updated
            value: New value
            timestamp: Timestamp of the value
        """
        await self.publish(
            "data_updates",
            {"series_id": series_id, "value": value, "timestamp": timestamp},
        )

    async def check_rate_limit(
        self,
        key: str,
        max_requests: int,
        window_seconds: int,
    ) -> tuple[bool, int]:
        """Check if a rate limit has been exceeded.

        Args:
            key: Rate limit key
            max_requests: Maximum requests allowed
            window_seconds: Time window in seconds

        Returns:
            Tuple of (allowed, remaining_requests)
        """
        pipe = self.client.pipeline()
        pipe.incr(key)
        pipe.expire(key, window_seconds)
        results = await pipe.execute()

        current = results[0]
        remaining = max(0, max_requests - current)
        allowed = current <= max_requests

        return allowed, remaining


# Global cache instance
_cache: RedisCache | None = None
_cache_initialized: bool = False


def get_cache() -> RedisCache:
    """Get the global cache instance."""
    global _cache
    if _cache is None:
        _cache = RedisCache()
    return _cache


async def init_cache() -> None:
    """Initialize the cache connection."""
    global _cache_initialized
    cache = get_cache()
    await cache.connect()
    _cache_initialized = True


def is_cache_initialized() -> bool:
    """Check if cache has been initialized."""
    return _cache_initialized
