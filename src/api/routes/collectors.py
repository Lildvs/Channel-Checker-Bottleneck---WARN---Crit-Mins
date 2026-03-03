"""API routes for collector management (GoComet credits, etc.)."""

import json
from datetime import datetime, UTC

import structlog
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.storage.redis_cache import get_cache

logger = structlog.get_logger()
router = APIRouter()

GOCOMET_REDIS_KEY = "gocomet_credits"


class GoCometCreditStatus(BaseModel):
    remaining_credits: int
    total_credits: int = 3
    refresh_day: str = "tuesday"
    hours_until_refresh: int
    last_query: str | None = None
    queries_this_week: list[str] = []


class GoCometQueryResponse(BaseModel):
    success: bool
    credits_remaining: int
    message: str


@router.get("/collectors/gocomet/credits", response_model=GoCometCreditStatus)
async def get_gocomet_credits() -> GoCometCreditStatus:
    """Get current GoComet credit status."""
    cache = get_cache()
    raw = await cache.client.get(GOCOMET_REDIS_KEY)

    if raw:
        state = json.loads(raw)
    else:
        state = {
            "remaining_credits": 3,
            "last_query_timestamp": None,
            "credit_refresh_day": "tuesday",
            "queries_this_week": [],
        }

    now = datetime.now(UTC)
    refresh_day = state.get("credit_refresh_day", "tuesday")
    day_map = {
        "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
        "friday": 4, "saturday": 5, "sunday": 6,
    }
    target = day_map.get(refresh_day.lower(), 1)
    days_until = (target - now.weekday()) % 7
    if days_until == 0:
        days_until = 7
    hours_until = days_until * 24

    return GoCometCreditStatus(
        remaining_credits=state.get("remaining_credits", 3),
        refresh_day=refresh_day,
        hours_until_refresh=hours_until,
        last_query=state.get("last_query_timestamp"),
        queries_this_week=state.get("queries_this_week", []),
    )


@router.post("/collectors/gocomet/confirm-query", response_model=GoCometQueryResponse)
async def confirm_gocomet_query() -> GoCometQueryResponse:
    """User confirms spending a GoComet credit for a port congestion query.

    This is triggered by the frontend pop-up when 1-2 credits remain.
    """
    cache = get_cache()
    raw = await cache.client.get(GOCOMET_REDIS_KEY)

    if raw:
        state = json.loads(raw)
    else:
        state = {
            "remaining_credits": 3,
            "last_query_timestamp": None,
            "credit_refresh_day": "tuesday",
            "queries_this_week": [],
        }

    remaining = state.get("remaining_credits", 0)

    if remaining <= 0:
        raise HTTPException(
            status_code=429,
            detail="No GoComet credits remaining this week",
        )

    from src.data_ingestion.collectors.port_congestion_collector import (
        PortCongestionCollector,
    )

    collector = PortCongestionCollector()
    import httpx
    async with httpx.AsyncClient(timeout=60.0) as client:
        points = await collector._execute_gocomet_query(client)

    state["remaining_credits"] = remaining - 1
    state["last_query_timestamp"] = datetime.now(UTC).isoformat()
    state.setdefault("queries_this_week", []).append(
        datetime.now(UTC).isoformat()
    )
    await cache.client.set(GOCOMET_REDIS_KEY, json.dumps(state), ex=604800)

    if points:
        from src.storage.timescale import get_db
        db = get_db()
        for dp in points:
            await db.insert_data_point(dp)

    return GoCometQueryResponse(
        success=True,
        credits_remaining=state["remaining_credits"],
        message=f"Query executed. {len(points)} port data points collected. {state['remaining_credits']} credits remaining.",
    )


@router.post("/collectors/gocomet/decline-query", response_model=GoCometQueryResponse)
async def decline_gocomet_query() -> GoCometQueryResponse:
    """User declines spending a GoComet credit."""
    cache = get_cache()
    raw = await cache.client.get(GOCOMET_REDIS_KEY)

    remaining = 0
    if raw:
        state = json.loads(raw)
        remaining = state.get("remaining_credits", 0)

    return GoCometQueryResponse(
        success=True,
        credits_remaining=remaining,
        message="Query skipped. Credits preserved.",
    )
