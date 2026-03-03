"""Pytest configuration and fixtures."""

import asyncio
from datetime import datetime
from typing import AsyncGenerator, Generator
from uuid import uuid4

import pytest
import pytest_asyncio
from httpx import AsyncClient

from src.api.main import app
from src.data_ingestion.base_collector import DataPoint


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create an event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def async_client() -> AsyncGenerator[AsyncClient, None]:
    """Create an async test client."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client


@pytest.fixture
def sample_data_point() -> DataPoint:
    """Create a sample data point for testing."""
    return DataPoint(
        source_id="test",
        series_id="TEST_SERIES",
        timestamp=datetime(2024, 1, 15),
        value=100.5,
        unit="index",
        quality_score=0.95,
        is_preliminary=False,
        metadata={"test": True},
    )


@pytest.fixture
def sample_data_points() -> list[DataPoint]:
    """Create a list of sample data points."""
    base_date = datetime(2024, 1, 1)
    points = []
    for i in range(30):
        points.append(
            DataPoint(
                source_id="test",
                series_id="TEST_SERIES",
                timestamp=datetime(2024, 1, i + 1),
                value=100 + i * 0.5,
                unit="index",
            )
        )
    return points
