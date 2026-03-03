"""Sector impact analysis using Input-Output data."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import numpy as np
import structlog

from src.config.sectors import SectorCategory
from src.config.bea_industry_mapping import map_bea_to_sector, CRITICAL_INDUSTRIES
from src.analysis.io_processor import (
    build_io_matrix,
    calculate_forward_linkages,
    calculate_backward_linkages,
    calculate_output_multipliers,
)

logger = structlog.get_logger()


@dataclass
class DependencyScore:
    """Represents a dependency relationship with scoring."""

    industry_code: str
    industry_name: str
    sector_category: SectorCategory | None
    coefficient: float
    normalized_score: float
    rank: int
    is_critical: bool


@dataclass
class UpstreamExposure:
    """Analysis of what an industry/sector depends on."""

    target: str
    target_name: str
    year: int
    total_upstream_coefficient: float
    concentration_index: float
    top_dependencies: list[DependencyScore]
    critical_dependencies: list[DependencyScore]
    by_sector: dict[SectorCategory, float]


@dataclass
class DownstreamImpact:
    """Analysis of what depends on an industry/sector."""

    source: str
    source_name: str
    year: int
    total_downstream_coefficient: float
    reach_index: float
    top_dependents: list[DependencyScore]
    critical_dependents: list[DependencyScore]
    by_sector: dict[SectorCategory, float]


@dataclass
class ShockImpact:
    """Result of a supply shock simulation."""

    affected_industry: str
    shock_magnitude: float
    year: int
    direct_output_loss: float
    total_output_loss: float
    output_multiplier: float
    sector_impacts: dict[SectorCategory, float]
    top_affected: list[tuple[str, str, float]]
    severity_score: float
    cascading_risk: str
    analysis_timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))


async def _get_latest_year() -> int | None:
    """Get the latest year of I-O data available."""
    from src.storage.timescale import get_db
    from src.storage.models import IOCoefficient
    from sqlalchemy import select, func

    db = get_db()
    async with db.session() as session:
        result = await session.execute(select(func.max(IOCoefficient.year)))
        return result.scalar()


async def calculate_upstream_exposure(
    target: str,
    year: int | None = None,
    threshold: float = 0.01,
    top_n: int = 20,
) -> UpstreamExposure | None:
    """Calculate what industries an industry depends on.

    Args:
        target: BEA industry code to analyze
        year: Data year (uses latest if None)
        threshold: Minimum coefficient to include
        top_n: Number of top dependencies to return

    Returns:
        UpstreamExposure analysis or None if data unavailable
    """
    if year is None:
        year = await _get_latest_year()
        if not year:
            return None

    matrix = await build_io_matrix(
        year=year, table_type="total_requirements", detail_level="summary"
    )
    if matrix is None or target not in matrix.industries:
        return None

    upstream = matrix.get_column(target)
    if not upstream:
        return None

    filtered = {k: v for k, v in upstream.items() if v >= threshold}
    sorted_deps = sorted(filtered.items(), key=lambda x: x[1], reverse=True)
    total = sum(upstream.values())
    concentration = (
        sum((v / total) ** 2 for v in upstream.values()) if total > 0 else 0.0
    )
    max_coef = max(upstream.values()) if upstream else 1.0

    top_deps: list[DependencyScore] = []
    critical_deps: list[DependencyScore] = []
    sector_totals: dict[SectorCategory, float] = {}

    for rank, (code, coef) in enumerate(sorted_deps[:top_n], 1):
        sector = map_bea_to_sector(code)
        is_critical = code in CRITICAL_INDUSTRIES
        score = DependencyScore(
            industry_code=code,
            industry_name=matrix.industry_names.get(code, ""),
            sector_category=sector,
            coefficient=coef,
            normalized_score=coef / max_coef if max_coef > 0 else 0,
            rank=rank,
            is_critical=is_critical,
        )
        top_deps.append(score)
        if is_critical:
            critical_deps.append(score)
        if sector:
            sector_totals[sector] = sector_totals.get(sector, 0) + coef

    return UpstreamExposure(
        target=target,
        target_name=matrix.industry_names.get(target, ""),
        year=year,
        total_upstream_coefficient=total,
        concentration_index=concentration,
        top_dependencies=top_deps,
        critical_dependencies=critical_deps,
        by_sector=sector_totals,
    )


async def calculate_downstream_impact(
    source: str,
    year: int | None = None,
    threshold: float = 0.01,
    top_n: int = 20,
) -> DownstreamImpact | None:
    """Calculate what industries depend on an industry.

    Args:
        source: BEA industry code to analyze
        year: Data year (uses latest if None)
        threshold: Minimum coefficient to include
        top_n: Number of top dependents to return

    Returns:
        DownstreamImpact analysis or None if data unavailable
    """
    if year is None:
        year = await _get_latest_year()
        if not year:
            return None

    matrix = await build_io_matrix(
        year=year, table_type="total_requirements", detail_level="summary"
    )
    if matrix is None or source not in matrix.industries:
        return None

    downstream = matrix.get_row(source)
    if not downstream:
        return None

    filtered = {k: v for k, v in downstream.items() if v >= threshold}
    sorted_deps = sorted(filtered.items(), key=lambda x: x[1], reverse=True)
    reach = sum(1 for v in downstream.values() if v >= 0.05)
    reach_index = reach / len(downstream) if downstream else 0
    total = sum(downstream.values())
    max_coef = max(downstream.values()) if downstream else 1.0

    top_deps: list[DependencyScore] = []
    critical_deps: list[DependencyScore] = []
    sector_totals: dict[SectorCategory, float] = {}

    for rank, (code, coef) in enumerate(sorted_deps[:top_n], 1):
        sector = map_bea_to_sector(code)
        is_critical = code in CRITICAL_INDUSTRIES
        score = DependencyScore(
            industry_code=code,
            industry_name=matrix.industry_names.get(code, ""),
            sector_category=sector,
            coefficient=coef,
            normalized_score=coef / max_coef if max_coef > 0 else 0,
            rank=rank,
            is_critical=is_critical,
        )
        top_deps.append(score)
        if is_critical:
            critical_deps.append(score)
        if sector:
            sector_totals[sector] = sector_totals.get(sector, 0) + coef

    return DownstreamImpact(
        source=source,
        source_name=matrix.industry_names.get(source, ""),
        year=year,
        total_downstream_coefficient=total,
        reach_index=reach_index,
        top_dependents=top_deps,
        critical_dependents=critical_deps,
        by_sector=sector_totals,
    )


async def simulate_supply_shock(
    affected_industry: str,
    shock_magnitude: float,
    year: int | None = None,
) -> ShockImpact | None:
    """Simulate the economic impact of a supply disruption.

    Args:
        affected_industry: BEA industry code experiencing the shock
        shock_magnitude: Fraction of output reduction (e.g., 0.10 = 10%)
        year: Data year (uses latest if None)

    Returns:
        ShockImpact analysis or None if data unavailable
    """
    if year is None:
        year = await _get_latest_year()
        if not year:
            return None

    matrix = await build_io_matrix(
        year=year, table_type="total_requirements", detail_level="summary"
    )
    if matrix is None or affected_industry not in matrix.industries:
        return None

    idx = matrix.industries.index(affected_industry)
    output_mults = calculate_output_multipliers(matrix.matrix)
    multiplier = float(output_mults[idx])

    downstream = matrix.get_row(affected_industry)
    sector_impacts: dict[SectorCategory, float] = {}
    for code, coef in downstream.items():
        sector = map_bea_to_sector(code)
        if sector:
            sector_impacts[sector] = (
                sector_impacts.get(sector, 0) + shock_magnitude * coef
            )

    top_affected = [
        (code, matrix.industry_names.get(code, ""), shock_magnitude * coef)
        for code, coef in sorted(
            downstream.items(), key=lambda x: x[1], reverse=True
        )[:10]
    ]

    affected_count = sum(
        1 for v in downstream.values() if v * shock_magnitude > 0.01
    )
    critical_affected = sum(
        1
        for code in downstream.keys()
        if code in CRITICAL_INDUSTRIES
        and downstream[code] * shock_magnitude > 0.01
    )

    severity = min(
        1.0,
        0.3 * min(1.0, multiplier / 3.0)
        + 0.3 * min(1.0, affected_count / 20)
        + 0.4 * min(1.0, critical_affected / 5),
    )

    if severity >= 0.8 or critical_affected >= 3:
        risk_level = "critical"
    elif severity >= 0.6 or critical_affected >= 2:
        risk_level = "high"
    elif severity >= 0.4:
        risk_level = "medium"
    else:
        risk_level = "low"

    return ShockImpact(
        affected_industry=affected_industry,
        shock_magnitude=shock_magnitude,
        year=year,
        direct_output_loss=shock_magnitude,
        total_output_loss=shock_magnitude * multiplier,
        output_multiplier=multiplier,
        sector_impacts=sector_impacts,
        top_affected=top_affected,
        severity_score=severity,
        cascading_risk=risk_level,
    )


async def identify_critical_vulnerabilities(
    year: int | None = None,
    top_n: int = 20,
) -> list[dict[str, Any]]:
    """Identify industries that are critical supply chain vulnerabilities.

    Args:
        year: Data year (uses latest if None)
        top_n: Number of top vulnerabilities to return

    Returns:
        List of vulnerability assessments
    """
    if year is None:
        year = await _get_latest_year()
        if not year:
            return []

    matrix = await build_io_matrix(
        year=year, table_type="total_requirements", detail_level="summary"
    )
    if matrix is None:
        return []

    forward = calculate_forward_linkages(matrix.matrix)
    backward = calculate_backward_linkages(matrix.matrix)
    multipliers = calculate_output_multipliers(matrix.matrix)

    vulnerabilities = []
    for i, industry in enumerate(matrix.industries):
        sector = map_bea_to_sector(industry)
        is_critical = industry in CRITICAL_INDUSTRIES
        score = (
            0.5 * min(1.0, float(forward[i]) / 2.0)
            + 0.2 * min(1.0, float(backward[i]) / 2.0)
            + 0.2 * min(1.0, float(multipliers[i]) / 3.0)
            + 0.1 * (1.0 if is_critical else 0.0)
        )
        vulnerabilities.append(
            {
                "industry_code": industry,
                "industry_name": matrix.industry_names.get(industry, ""),
                "sector_category": sector.value if sector else None,
                "is_critical_industry": is_critical,
                "forward_linkage": float(forward[i]),
                "backward_linkage": float(backward[i]),
                "output_multiplier": float(multipliers[i]),
                "vulnerability_score": score,
                "year": year,
            }
        )

    vulnerabilities.sort(key=lambda x: x["vulnerability_score"], reverse=True)
    return vulnerabilities[:top_n]


async def get_sector_dependency_matrix(
    year: int | None = None,
) -> dict[str, dict[str, float]]:
    """Get a simplified sector-to-sector dependency matrix.

    Args:
        year: Data year (uses latest if None)

    Returns:
        Nested dict: from_sector -> to_sector -> coefficient
    """
    from src.analysis.io_processor import aggregate_by_sector

    if year is None:
        year = await _get_latest_year()
        if not year:
            return {}

    matrix = await build_io_matrix(
        year=year, table_type="total_requirements", detail_level="summary"
    )
    if matrix is None:
        return {}

    aggregated = await aggregate_by_sector(matrix)
    return {
        from_sector.value: {
            to_sector.value: coef for to_sector, coef in to_sectors.items()
        }
        for from_sector, to_sectors in aggregated.items()
    }
