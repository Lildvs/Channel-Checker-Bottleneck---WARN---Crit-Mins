"""Pytest fixtures for collector tests."""

import pytest

from src.data_ingestion.rate_limiter import RateLimiterRegistry


@pytest.fixture(autouse=True)
def reset_rate_limiters():
    """Reset rate limiters before each test.

    This prevents the AsyncLimiter re-use across event loops warning
    by ensuring fresh limiter instances are created for each test.
    """
    RateLimiterRegistry.reset_instance()

    yield

    RateLimiterRegistry.reset_instance()
