"""Sector-related API endpoints."""

from typing import Any

import structlog
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.analysis.sector_mapper import get_sector_mapper
from src.config.sectors import SECTOR_DEFINITIONS, SectorCategory
from src.storage.redis_cache import get_cache

logger = structlog.get_logger()
router = APIRouter()


class SectorInfo(BaseModel):
    """Response model for sector information."""

    code: str
    name: str
    category: str
    description: str | None
    naics_codes: list[str]
    key_indicators: list[str]


class SectorExposure(BaseModel):
    """Response model for sector exposure."""

    sector_code: str
    sector_name: str
    category: str
    upstream_dependencies: list[dict[str, Any]]
    downstream_dependents: list[dict[str, Any]]
    total_upstream_exposure: float
    total_downstream_exposure: float


class SystemicRiskResponse(BaseModel):
    """Response model for systemic risk scores."""

    risk_scores: dict[str, float]
    highest_risk_sector: str | None
    lowest_risk_sector: str | None
    average_risk: float


@router.get("/", response_model=list[SectorInfo])
async def list_sectors() -> list[SectorInfo]:
    """Get all tracked sectors.

    Returns:
        List of sector information
    """
    sectors = []

    for category, definition in SECTOR_DEFINITIONS.items():
        sectors.append(
            SectorInfo(
                code=definition.code,
                name=definition.name,
                category=category.value,
                description=definition.description,
                naics_codes=list(definition.naics_codes),
                key_indicators=list(definition.key_indicators),
            )
        )

    return sectors


@router.get("/{sector_code}", response_model=SectorInfo)
async def get_sector(sector_code: str) -> SectorInfo:
    """Get details for a specific sector.

    Args:
        sector_code: Code of the sector

    Returns:
        Sector information
    """
    for category, definition in SECTOR_DEFINITIONS.items():
        if definition.code == sector_code:
            return SectorInfo(
                code=definition.code,
                name=definition.name,
                category=category.value,
                description=definition.description,
                naics_codes=list(definition.naics_codes),
                key_indicators=list(definition.key_indicators),
            )

    raise HTTPException(status_code=404, detail=f"Sector {sector_code} not found")


@router.get("/{sector_code}/exposure", response_model=SectorExposure)
async def get_sector_exposure(sector_code: str) -> SectorExposure:
    """Get exposure analysis for a sector.

    Args:
        sector_code: Code of the sector

    Returns:
        Sector exposure information
    """
    mapper = get_sector_mapper()
    exposure = mapper.get_sector_exposure(sector_code)

    if not exposure:
        raise HTTPException(status_code=404, detail=f"Sector {sector_code} not found")

    return SectorExposure(**exposure)


@router.get("/risk/systemic", response_model=SystemicRiskResponse)
async def get_systemic_risk() -> SystemicRiskResponse:
    """Calculate systemic risk scores for all sectors based on active bottlenecks.

    Returns:
        Risk scores by sector
    """
    from src.analysis.signals import BottleneckCategory, BottleneckSignalData
    from src.storage.redis_cache import get_cache

    try:
        cache = get_cache()
        cached_bottlenecks = await cache.get_active_bottlenecks()

        # Convert to signal objects
        signals = []
        for b in cached_bottlenecks:
            try:
                signal = BottleneckSignalData(
                    category=BottleneckCategory(b["category"]),
                    severity=b["severity"],
                    confidence=b["confidence"],
                    affected_sectors=b.get("affected_sectors", []),
                )
                signals.append(signal)
            except Exception:
                continue

        mapper = get_sector_mapper()
        risk_scores = mapper.calculate_systemic_risk(signals)

        # Find highest and lowest
        sorted_risks = sorted(risk_scores.items(), key=lambda x: x[1], reverse=True)
        highest = sorted_risks[0][0] if sorted_risks else None
        lowest = sorted_risks[-1][0] if sorted_risks else None
        average = sum(risk_scores.values()) / len(risk_scores) if risk_scores else 0.0

        return SystemicRiskResponse(
            risk_scores=risk_scores,
            highest_risk_sector=highest,
            lowest_risk_sector=lowest,
            average_risk=average,
        )

    except Exception as e:
        logger.error("Failed to calculate systemic risk", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/graph/dependencies")
async def get_dependency_graph() -> dict[str, Any]:
    """Get the sector dependency graph for visualization.

    Returns:
        Graph data in node-link format
    """
    mapper = get_sector_mapper()

    nodes = []
    links = []

    for sector in mapper.sectors.values():
        nodes.append({
            "id": sector.code,
            "name": sector.name,
            "category": sector.category.value,
        })

        for downstream_code, weight in sector.downstream:
            links.append({
                "source": sector.code,
                "target": downstream_code,
                "weight": weight,
            })

    return {
        "nodes": nodes,
        "links": links,
    }



class GraphNode(BaseModel):
    """Response model for graph node."""

    id: str
    name: str
    category: str
    riskScore: float
    size: float


class GraphEdge(BaseModel):
    """Response model for graph edge."""

    source: str
    target: str
    weight: float
    dependencyType: str


class PropagationStep(BaseModel):
    """A single step in propagation path."""

    step: int
    sector: str
    impactLevel: float
    fromSector: str | None


class PropagationDataResponse(BaseModel):
    """Response model for propagation animation data."""

    bottleneckId: str
    originSector: str
    steps: list[PropagationStep]
    totalImpact: float


@router.get("/graph/nodes", response_model=list[GraphNode])
async def get_graph_nodes() -> list[GraphNode]:
    """Get sector nodes for D3 visualization.

    Returns nodes with:
    - id: Sector code
    - name: Display name
    - category: utilities/goods/services
    - riskScore: Current risk level (0-1)
    - size: GDP contribution or weight
    """
    mapper = get_sector_mapper()

    cache = get_cache()
    risk_scores = {}

    try:
        cached_bottlenecks = await cache.get("bottlenecks:active")
        if cached_bottlenecks:
            # Calculate risk from active bottlenecks
            for b in cached_bottlenecks.get("bottlenecks", []):
                for sector in b.get("affected_sectors", []):
                    current = risk_scores.get(sector, 0)
                    risk_scores[sector] = max(current, b.get("severity", 0) * 0.8)
    except Exception:
        pass

    GDP_WEIGHTS = {
        "ENERGY": 500,
        "MANUFACTURING": 800,
        "TRANSPORTATION": 400,
        "RETAIL": 600,
        "HEALTHCARE": 700,
        "FINANCE": 900,
        "TECHNOLOGY": 1000,
        "CONSTRUCTION": 450,
        "AGRICULTURE": 200,
        "MINING": 150,
        "UTILITIES": 300,
        "WHOLESALE": 500,
        "INFORMATION": 550,
        "PROFESSIONAL": 650,
        "EDUCATION": 350,
        "ACCOMMODATION": 400,
        "GOVERNMENT": 800,
    }

    nodes = []
    for sector in mapper.sectors.values():
        # Map internal categories to visualization categories
        cat_map = {
            "utilities": "utilities",
            "mining": "utilities",
            "manufacturing": "goods",
            "construction": "goods",
            "agriculture": "goods",
            "transportation": "services",
            "retail": "services",
            "wholesale": "services",
            "information": "services",
            "finance": "services",
            "professional": "services",
            "education": "services",
            "healthcare": "services",
            "accommodation": "services",
            "government": "services",
        }

        category = cat_map.get(sector.category.value.lower(), "services")

        nodes.append(
            GraphNode(
                id=sector.code,
                name=sector.name,
                category=category,
                riskScore=risk_scores.get(sector.code, 0.3),
                size=GDP_WEIGHTS.get(sector.code, 300),
            )
        )

    return nodes


@router.get("/graph/edges", response_model=list[GraphEdge])
async def get_graph_edges() -> list[GraphEdge]:
    """Get sector dependency edges for D3 visualization.

    Returns edges with:
    - source: Source sector code
    - target: Target sector code
    - weight: Dependency strength (0-1)
    - dependencyType: supply/demand/both
    """
    mapper = get_sector_mapper()

    edges = []
    seen_pairs = set()

    for sector in mapper.sectors.values():
        for downstream_code, weight in sector.downstream:
            pair = (sector.code, downstream_code)
            reverse_pair = (downstream_code, sector.code)

            # Check if we have a bidirectional relationship
            if reverse_pair in seen_pairs:
                # Update existing to 'both'
                for edge in edges:
                    if edge.source == downstream_code and edge.target == sector.code:
                        edge.dependencyType = "both"
                        break
            else:
                edges.append(
                    GraphEdge(
                        source=sector.code,
                        target=downstream_code,
                        weight=weight,
                        dependencyType="supply",
                    )
                )
                seen_pairs.add(pair)

    return edges


@router.get("/graph/propagation/{bottleneck_id}", response_model=PropagationDataResponse)
async def get_propagation_animation(bottleneck_id: str) -> PropagationDataResponse:
    """Get propagation path for animation.

    Simulates how a bottleneck propagates through the supply chain,
    returning step-by-step data for animated visualization.

    Args:
        bottleneck_id: ID or type of bottleneck (e.g., 'energy-crisis')

    Returns:
        Propagation steps with impact levels
    """
    from src.analysis.propagation_engine import get_propagation_engine
    from src.analysis.signals import BottleneckSignalData, BottleneckCategory

    mapper = get_sector_mapper()

    # Map bottleneck_id to origin sector and category
    BOTTLENECK_ORIGINS = {
        "energy-crisis": ("ENERGY", BottleneckCategory.ENERGY_CRUNCH),
        "supply-chain": ("MANUFACTURING", BottleneckCategory.SUPPLY_DISRUPTION),
        "labor-shortage": ("TRANSPORTATION", BottleneckCategory.LABOR_TIGHTNESS),
        "tech-disruption": ("TECHNOLOGY", BottleneckCategory.SUPPLY_DISRUPTION),
        "financial-stress": ("FINANCE", BottleneckCategory.CREDIT_TIGHTENING),
    }

    origin_sector, category = BOTTLENECK_ORIGINS.get(
        bottleneck_id, ("ENERGY", BottleneckCategory.SUPPLY_DISRUPTION)
    )

    try:
        engine = get_propagation_engine()

        # Build a BottleneckSignalData for the propagation engine
        signal = BottleneckSignalData(
            category=category,
            severity=1.0,
            confidence=0.8,
            affected_sectors=[origin_sector],
        )

        # Get propagation using the engine
        result = await engine.propagate_bottleneck(signal=signal)

        # Convert to animation steps
        steps = []
        step_num = 0
        current_level = {origin_sector}
        visited = {origin_sector}

        # Step 0: Origin
        steps.append(
            PropagationStep(
                step=0,
                sector=origin_sector,
                impactLevel=1.0,
                fromSector=None,
            )
        )

        # Build steps by propagation depth
        for hop in range(1, 5):
            next_level = set()
            for sector_code in current_level:
                sector = mapper.sectors.get(sector_code)
                if not sector:
                    continue

                for downstream_code, weight in sector.downstream:
                    if downstream_code not in visited:
                        # Calculate impact decay
                        parent_impact = next(
                            (s.impactLevel for s in steps if s.sector == sector_code),
                            1.0,
                        )
                        impact = parent_impact * weight * 0.8  # Decay factor

                        if impact >= 0.1:  # Threshold for visibility
                            steps.append(
                                PropagationStep(
                                    step=hop,
                                    sector=downstream_code,
                                    impactLevel=round(impact, 3),
                                    fromSector=sector_code,
                                )
                            )
                            next_level.add(downstream_code)
                            visited.add(downstream_code)

            current_level = next_level
            if not current_level:
                break

        # Calculate total impact
        total_impact = sum(s.impactLevel for s in steps) / len(steps) if steps else 0

        return PropagationDataResponse(
            bottleneckId=bottleneck_id,
            originSector=origin_sector,
            steps=steps,
            totalImpact=round(total_impact, 3),
        )

    except Exception as e:
        logger.warning("Propagation calculation failed, using fallback", error=str(e))

        # Fallback: Simple propagation based on mapper
        steps = [
            PropagationStep(step=0, sector=origin_sector, impactLevel=1.0, fromSector=None)
        ]

        sector = mapper.sectors.get(origin_sector)
        if sector:
            for downstream_code, weight in sector.downstream[:5]:
                steps.append(
                    PropagationStep(
                        step=1,
                        sector=downstream_code,
                        impactLevel=round(weight * 0.8, 3),
                        fromSector=origin_sector,
                    )
                )

        return PropagationDataResponse(
            bottleneckId=bottleneck_id,
            originSector=origin_sector,
            steps=steps,
            totalImpact=0.7,
        )
