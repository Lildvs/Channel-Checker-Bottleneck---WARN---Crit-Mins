"""Tests for API endpoints."""

import pytest
from httpx import AsyncClient


class TestHealthEndpoints:
    """Tests for health check endpoints."""

    @pytest.mark.asyncio
    async def test_root_endpoint(self, async_client: AsyncClient):
        """Test root endpoint returns healthy status."""
        response = await async_client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "service" in data

    @pytest.mark.asyncio
    async def test_health_endpoint(self, async_client: AsyncClient):
        """Test health check endpoint."""
        response = await async_client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"


class TestBottleneckEndpoints:
    """Tests for bottleneck endpoints."""

    @pytest.mark.asyncio
    async def test_get_categories(self, async_client: AsyncClient):
        """Test getting bottleneck categories."""
        response = await async_client.get("/api/bottlenecks/categories")
        assert response.status_code == 200
        categories = response.json()
        assert isinstance(categories, list)
        assert len(categories) > 0
        assert all("value" in cat and "name" in cat for cat in categories)


class TestSectorEndpoints:
    """Tests for sector endpoints."""

    @pytest.mark.asyncio
    async def test_list_sectors(self, async_client: AsyncClient):
        """Test listing all sectors."""
        response = await async_client.get("/api/sectors/")
        assert response.status_code == 200
        sectors = response.json()
        assert isinstance(sectors, list)
        assert len(sectors) > 0

    @pytest.mark.asyncio
    async def test_get_dependency_graph(self, async_client: AsyncClient):
        """Test getting dependency graph."""
        response = await async_client.get("/api/sectors/graph/dependencies")
        assert response.status_code == 200
        graph = response.json()
        assert "nodes" in graph
        assert "links" in graph


class TestDataEndpoints:
    """Tests for data endpoints."""

    @pytest.mark.asyncio
    async def test_list_sources(self, async_client: AsyncClient):
        """Test listing data sources."""
        response = await async_client.get("/api/data/sources")
        assert response.status_code == 200
        sources = response.json()
        assert isinstance(sources, list)
        assert len(sources) > 0
        assert all("id" in s and "name" in s for s in sources)
