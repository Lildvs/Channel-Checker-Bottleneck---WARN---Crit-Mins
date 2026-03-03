"""Trade Flow API endpoints for commodity flow visualization.

Provides endpoints for:
- Trade flow data for geographic visualization
- Critical mineral flows with country pairs
- Sankey diagram data format
- Port throughput and shipping routes
"""

from datetime import datetime, timedelta, UTC
from decimal import Decimal
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import func, select, and_, desc
from src.storage.models import MineralTradeFlow, MineralProduction, PortThroughput, ShippingIndex
from src.storage.timescale import get_db

logger = structlog.get_logger()
router = APIRouter(prefix="/trade", tags=["trade"])



class TradeFlowRecord(BaseModel):
    """Single trade flow record."""

    mineral: str
    reporter_country: str
    reporter_iso3: str | None
    partner_country: str
    partner_iso3: str | None
    flow_type: str
    value_usd: float
    quantity: float | None
    weight_kg: float | None
    period: str


class TradeFlowsResponse(BaseModel):
    """Response for trade flow data."""

    flows: list[TradeFlowRecord]
    total: int
    total_value_usd: float


class CountryTradeVolume(BaseModel):
    """Trade volume for a country."""

    country: str
    iso3: str | None
    import_value: float
    export_value: float
    total_value: float
    minerals: list[str]


class CountryListResponse(BaseModel):
    """Response for country trade volumes."""

    countries: list[CountryTradeVolume]
    total_countries: int


class SankeyNode(BaseModel):
    """Node in a Sankey diagram."""

    id: str
    name: str
    type: str  # country, mineral, stage


class SankeyLink(BaseModel):
    """Link in a Sankey diagram."""

    source: str
    target: str
    value: float


class SankeyResponse(BaseModel):
    """Response for Sankey diagram data."""

    nodes: list[SankeyNode]
    links: list[SankeyLink]
    mineral: str


class PortRecord(BaseModel):
    """Port throughput record."""

    port_name: str
    port_code: str | None
    country: str | None
    region: str | None
    metric_type: str
    value: float
    unit: str
    period: str
    change_percent: float | None


class PortsResponse(BaseModel):
    """Response for port throughput data."""

    ports: list[PortRecord]
    total_ports: int


class ShippingRoute(BaseModel):
    """Shipping route between ports."""

    origin_port: str
    origin_country: str | None
    dest_port: str
    dest_country: str | None
    volume: float
    mineral: str | None


class RoutesResponse(BaseModel):
    """Response for shipping routes."""

    routes: list[ShippingRoute]


class TradeStatsResponse(BaseModel):
    """Summary trade statistics."""

    total_trade_value: float
    total_flows: int
    minerals_tracked: list[str]
    top_exporters: list[dict[str, Any]]
    top_importers: list[dict[str, Any]]
    period_range: dict[str, str | None]



@router.get("/flows", response_model=TradeFlowsResponse)
async def get_trade_flows(
    mineral: str | None = Query(None, description="Filter by mineral type"),
    reporter: str | None = Query(None, description="Filter by reporter country ISO3"),
    partner: str | None = Query(None, description="Filter by partner country ISO3"),
    flow_type: str | None = Query(None, description="Filter by flow type: import/export"),
    period: str | None = Query(None, description="Filter by period (YYYYMM)"),
    top_n: int = Query(100, ge=1, le=500, description="Limit to top N by value"),
) -> TradeFlowsResponse:
    """Get trade flow data for visualization."""
    try:
        db = get_db()
        async with db.session() as session:
            conditions = []
            if mineral:
                conditions.append(MineralTradeFlow.mineral == mineral)
            if reporter:
                conditions.append(MineralTradeFlow.reporter_iso3 == reporter)
            if partner:
                conditions.append(MineralTradeFlow.partner_iso3 == partner)
            if flow_type:
                conditions.append(MineralTradeFlow.flow_type == flow_type)
            if period:
                conditions.append(MineralTradeFlow.period == period)

            where_clause = and_(*conditions) if conditions else True

            query = (
                select(MineralTradeFlow)
                .where(where_clause)
                .order_by(desc(MineralTradeFlow.value_usd))
                .limit(top_n)
            )

            result = await session.execute(query)
            flows = result.scalars().all()

            total_query = select(
                func.count(MineralTradeFlow.id),
                func.sum(MineralTradeFlow.value_usd),
            ).where(where_clause)

            total_result = await session.execute(total_query)
            row = total_result.one()
            total_count = row[0] or 0
            total_value = float(row[1] or 0)

            return TradeFlowsResponse(
                flows=[
                    TradeFlowRecord(
                        mineral=f.mineral,
                        reporter_country=f.reporter_country,
                        reporter_iso3=f.reporter_iso3,
                        partner_country=f.partner_country,
                        partner_iso3=f.partner_iso3,
                        flow_type=f.flow_type,
                        value_usd=float(f.value_usd),
                        quantity=float(f.quantity) if f.quantity else None,
                        weight_kg=float(f.weight_kg) if f.weight_kg else None,
                        period=f.period,
                    )
                    for f in flows
                ],
                total=total_count,
                total_value_usd=total_value,
            )

    except Exception as e:
        logger.error("Failed to get trade flows", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/flows/minerals", response_model=TradeFlowsResponse)
async def get_mineral_flows(
    mineral: str = Query(..., description="Mineral type (required)"),
    period: str | None = Query(None, description="Period filter"),
    top_n: int = Query(50, ge=1, le=200),
) -> TradeFlowsResponse:
    """Get trade flows for a specific mineral."""
    return await get_trade_flows(mineral=mineral, period=period, top_n=top_n)


@router.get("/flows/sankey", response_model=SankeyResponse)
async def get_sankey_data(
    mineral: str = Query(..., description="Mineral type for Sankey diagram"),
    period: str | None = Query(None, description="Period filter"),
    top_n: int = Query(20, ge=5, le=50, description="Top N flows to include"),
) -> SankeyResponse:
    """Get data formatted for D3 Sankey diagram.

    Creates a supply chain visualization showing:
    Producer countries -> Processing countries -> Consumer countries
    """
    try:
        db = get_db()
        async with db.session() as session:
            conditions = [MineralTradeFlow.mineral == mineral]
            if period:
                conditions.append(MineralTradeFlow.period == period)

            query = (
                select(MineralTradeFlow)
                .where(and_(*conditions))
                .order_by(desc(MineralTradeFlow.value_usd))
                .limit(top_n)
            )

            result = await session.execute(query)
            flows = result.scalars().all()

            nodes_dict: dict[str, SankeyNode] = {}
            links: list[SankeyLink] = []

            for flow in flows:
                exp_id = f"exp_{flow.reporter_iso3 or flow.reporter_country}"
                if exp_id not in nodes_dict:
                    nodes_dict[exp_id] = SankeyNode(
                        id=exp_id,
                        name=flow.reporter_country,
                        type="exporter",
                    )

                imp_id = f"imp_{flow.partner_iso3 or flow.partner_country}"
                if imp_id not in nodes_dict:
                    nodes_dict[imp_id] = SankeyNode(
                        id=imp_id,
                        name=flow.partner_country,
                        type="importer",
                    )

                if flow.flow_type == "export":
                    links.append(
                        SankeyLink(
                            source=exp_id,
                            target=imp_id,
                            value=float(flow.value_usd),
                        )
                    )

            return SankeyResponse(
                nodes=list(nodes_dict.values()),
                links=links,
                mineral=mineral,
            )

    except Exception as e:
        logger.error("Failed to get Sankey data", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/countries", response_model=CountryListResponse)
async def get_countries(
    mineral: str | None = Query(None, description="Filter by mineral"),
    period: str | None = Query(None, description="Filter by period"),
) -> CountryListResponse:
    """Get list of countries with their trade volumes."""
    try:
        db = get_db()
        async with db.session() as session:
            conditions = []
            if mineral:
                conditions.append(MineralTradeFlow.mineral == mineral)
            if period:
                conditions.append(MineralTradeFlow.period == period)

            where_clause = and_(*conditions) if conditions else True

            query = select(MineralTradeFlow).where(where_clause)
            result = await session.execute(query)
            flows = result.scalars().all()

            country_stats: dict[str, dict[str, Any]] = {}

            for flow in flows:
                # Use reporter as the country
                key = flow.reporter_iso3 or flow.reporter_country
                if key not in country_stats:
                    country_stats[key] = {
                        "country": flow.reporter_country,
                        "iso3": flow.reporter_iso3,
                        "import_value": 0.0,
                        "export_value": 0.0,
                        "minerals": set(),
                    }

                stats = country_stats[key]
                stats["minerals"].add(flow.mineral)

                if flow.flow_type == "import":
                    stats["import_value"] += float(flow.value_usd)
                elif flow.flow_type == "export":
                    stats["export_value"] += float(flow.value_usd)

            # Format response
            countries = [
                CountryTradeVolume(
                    country=s["country"],
                    iso3=s["iso3"],
                    import_value=s["import_value"],
                    export_value=s["export_value"],
                    total_value=s["import_value"] + s["export_value"],
                    minerals=list(s["minerals"]),
                )
                for s in country_stats.values()
            ]

            countries.sort(key=lambda x: x.total_value, reverse=True)

            return CountryListResponse(
                countries=countries,
                total_countries=len(countries),
            )

    except Exception as e:
        logger.error("Failed to get countries", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ports", response_model=PortsResponse)
async def get_ports(
    region: str | None = Query(None, description="Filter by region"),
    metric_type: str = Query("teu_total", description="Metric type"),
    limit: int = Query(50, ge=1, le=100),
) -> PortsResponse:
    """Get port throughput data."""
    try:
        db = get_db()
        async with db.session() as session:
            conditions = [PortThroughput.metric_type == metric_type]
            if region:
                conditions.append(PortThroughput.region == region)

            query = (
                select(PortThroughput)
                .where(and_(*conditions))
                .order_by(desc(PortThroughput.value))
                .limit(limit)
            )

            result = await session.execute(query)
            ports = result.scalars().all()

            return PortsResponse(
                ports=[
                    PortRecord(
                        port_name=p.port_name,
                        port_code=p.port_code,
                        country=p.country,
                        region=p.region,
                        metric_type=p.metric_type,
                        value=float(p.value),
                        unit=p.unit,
                        period=p.period_start.strftime("%Y-%m") if p.period_start else "",
                        change_percent=p.change_percent,
                    )
                    for p in ports
                ],
                total_ports=len(ports),
            )

    except Exception as e:
        logger.error("Failed to get ports", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/routes", response_model=RoutesResponse)
async def get_shipping_routes(
    mineral: str | None = Query(None, description="Filter by mineral"),
    top_n: int = Query(20, ge=1, le=50),
) -> RoutesResponse:
    """Get shipping routes between major ports.

    Note: Routes are inferred from trade flow data since
    we don't have explicit shipping route information.
    """
    try:
        db = get_db()
        async with db.session() as session:
            conditions = [MineralTradeFlow.flow_type == "export"]
            if mineral:
                conditions.append(MineralTradeFlow.mineral == mineral)

            query = (
                select(MineralTradeFlow)
                .where(and_(*conditions))
                .order_by(desc(MineralTradeFlow.value_usd))
                .limit(top_n)
            )

            result = await session.execute(query)
            flows = result.scalars().all()

            routes = [
                ShippingRoute(
                    origin_port=f.reporter_country,
                    origin_country=f.reporter_iso3,
                    dest_port=f.partner_country,
                    dest_country=f.partner_iso3,
                    volume=float(f.value_usd),
                    mineral=f.mineral,
                )
                for f in flows
            ]

            return RoutesResponse(routes=routes)

    except Exception as e:
        logger.error("Failed to get routes", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats", response_model=TradeStatsResponse)
async def get_trade_stats(
    period: str | None = Query(None, description="Filter by period"),
) -> TradeStatsResponse:
    """Get summary trade statistics."""
    try:
        db = get_db()
        async with db.session() as session:
            conditions = []
            if period:
                conditions.append(MineralTradeFlow.period == period)

            where_clause = and_(*conditions) if conditions else True

            result = await session.execute(select(MineralTradeFlow).where(where_clause))
            flows = list(result.scalars().all())

            if not flows:
                return TradeStatsResponse(
                    total_trade_value=0,
                    total_flows=0,
                    minerals_tracked=[],
                    top_exporters=[],
                    top_importers=[],
                    period_range={"start": None, "end": None},
                )

            total_value = sum(float(f.value_usd) for f in flows)
            minerals = set(f.mineral for f in flows)

            exporter_values: dict[str, float] = {}
            importer_values: dict[str, float] = {}

            for flow in flows:
                if flow.flow_type == "export":
                    key = flow.reporter_country
                    exporter_values[key] = exporter_values.get(key, 0) + float(flow.value_usd)
                elif flow.flow_type == "import":
                    key = flow.reporter_country
                    importer_values[key] = importer_values.get(key, 0) + float(flow.value_usd)

            top_exporters = sorted(
                [{"country": k, "value": v} for k, v in exporter_values.items()],
                key=lambda x: x["value"],
                reverse=True,
            )[:10]

            top_importers = sorted(
                [{"country": k, "value": v} for k, v in importer_values.items()],
                key=lambda x: x["value"],
                reverse=True,
            )[:10]

            periods = [f.period for f in flows]
            min_period = min(periods) if periods else None
            max_period = max(periods) if periods else None

            return TradeStatsResponse(
                total_trade_value=total_value,
                total_flows=len(flows),
                minerals_tracked=sorted(minerals),
                top_exporters=top_exporters,
                top_importers=top_importers,
                period_range={"start": min_period, "end": max_period},
            )

    except Exception as e:
        logger.error("Failed to get trade stats", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/minerals")
async def list_minerals() -> list[str]:
    """Get list of all tracked minerals."""
    try:
        db = get_db()
        async with db.session() as session:
            query = select(func.distinct(MineralTradeFlow.mineral)).order_by(MineralTradeFlow.mineral)
            result = await session.execute(query)
            minerals = [r[0] for r in result.all()]

            return minerals

    except Exception as e:
        logger.error("Failed to list minerals", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
