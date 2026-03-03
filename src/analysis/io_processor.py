"""Input-Output matrix processing and Leontief inverse calculations.

This module provides functions for working with BEA Input-Output tables,
including:
- Building coefficient matrices from database records
- Calculating Leontief inverse (total requirements from direct requirements)
- Converting between different matrix representations
- Aggregating coefficients by sector category
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import numpy as np
import structlog

from src.config.bea_industry_mapping import (
    BEA_SUMMARY_INDUSTRIES,
    BEA_SECTOR_INDUSTRIES,
    map_bea_to_sector,
    get_bea_industry,
)
from src.config.sectors import SectorCategory

logger = structlog.get_logger()


@dataclass
class IOMatrix:
    """Represents an Input-Output coefficient matrix.

    Attributes:
        matrix: The coefficient matrix (from_industry x to_industry)
        industries: List of industry codes (row/column order)
        industry_names: Dict of code -> name
        year: Data year
        table_type: 'direct_requirements' or 'total_requirements'
        detail_level: 'sector', 'summary', or 'detail'
    """

    matrix: np.ndarray
    industries: list[str]
    industry_names: dict[str, str]
    year: int
    table_type: str
    detail_level: str

    @property
    def n_industries(self) -> int:
        """Number of industries in the matrix."""
        return len(self.industries)

    def get_coefficient(self, from_industry: str, to_industry: str) -> float:
        """Get a specific coefficient.

        Args:
            from_industry: Source industry code
            to_industry: Consuming industry code

        Returns:
            Coefficient value (0 if industries not found)
        """
        try:
            from_idx = self.industries.index(from_industry)
            to_idx = self.industries.index(to_industry)
            return float(self.matrix[from_idx, to_idx])
        except ValueError:
            return 0.0

    def get_row(self, industry: str) -> dict[str, float]:
        """Get all coefficients where industry is the source (downstream impacts).

        Args:
            industry: Source industry code

        Returns:
            Dict of to_industry -> coefficient
        """
        try:
            idx = self.industries.index(industry)
            row = self.matrix[idx, :]
            return {
                self.industries[i]: float(row[i])
                for i in range(len(self.industries))
                if row[i] != 0
            }
        except ValueError:
            return {}

    def get_column(self, industry: str) -> dict[str, float]:
        """Get all coefficients where industry is the consumer (upstream dependencies).

        Args:
            industry: Consuming industry code

        Returns:
            Dict of from_industry -> coefficient
        """
        try:
            idx = self.industries.index(industry)
            col = self.matrix[:, idx]
            return {
                self.industries[i]: float(col[i])
                for i in range(len(self.industries))
                if col[i] != 0
            }
        except ValueError:
            return {}


async def build_io_matrix(
    year: int,
    table_type: str = "total_requirements",
    detail_level: str = "summary",
) -> IOMatrix | None:
    """Build an I-O matrix from database coefficients.

    Args:
        year: Data year
        table_type: 'direct_requirements' or 'total_requirements'
        detail_level: 'sector', 'summary', or 'detail'

    Returns:
        IOMatrix object or None if data not available
    """
    from src.storage.timescale import get_db
    from src.storage.models import IOCoefficient
    from sqlalchemy import select

    db = get_db()

    async with db.session() as session:
        query = select(IOCoefficient).where(
            IOCoefficient.year == year,
            IOCoefficient.table_type == table_type,
            IOCoefficient.detail_level == detail_level,
        )

        result = await session.execute(query)
        coefficients = result.scalars().all()

        if not coefficients:
            logger.warning(
                "No I-O coefficients found",
                year=year,
                table_type=table_type,
                detail_level=detail_level,
            )
            return None

    industry_set: set[str] = set()
    industry_names: dict[str, str] = {}

    for coef in coefficients:
        industry_set.add(coef.from_industry)
        industry_set.add(coef.to_industry)
        if coef.from_industry_name:
            industry_names[coef.from_industry] = coef.from_industry_name
        if coef.to_industry_name:
            industry_names[coef.to_industry] = coef.to_industry_name

    industries = sorted(list(industry_set))
    n = len(industries)

    idx_map = {ind: i for i, ind in enumerate(industries)}
    matrix = np.zeros((n, n), dtype=np.float64)

    for coef in coefficients:
        from_idx = idx_map.get(coef.from_industry)
        to_idx = idx_map.get(coef.to_industry)
        if from_idx is not None and to_idx is not None:
            matrix[from_idx, to_idx] = float(coef.coefficient)

    logger.info(
        "Built I-O matrix",
        year=year,
        table_type=table_type,
        detail_level=detail_level,
        industries=n,
        non_zero=np.count_nonzero(matrix),
    )

    return IOMatrix(
        matrix=matrix,
        industries=industries,
        industry_names=industry_names,
        year=year,
        table_type=table_type,
        detail_level=detail_level,
    )


def calculate_leontief_inverse(direct_requirements: np.ndarray) -> np.ndarray:
    """Calculate the Leontief inverse (total requirements matrix).

    The Leontief inverse L = (I - A)^-1 gives the total (direct + indirect)
    output required from each industry to deliver one dollar of output
    to final demand.

    Args:
        direct_requirements: The direct requirements (A) matrix

    Returns:
        Total requirements (Leontief inverse) matrix

    Raises:
        ValueError: If matrix is singular
    """
    n = direct_requirements.shape[0]
    identity = np.eye(n)

    try:
        leontief = np.linalg.inv(identity - direct_requirements)
        return leontief
    except np.linalg.LinAlgError as e:
        logger.error("Failed to calculate Leontief inverse", error=str(e))
        raise ValueError(f"Matrix is singular: {e}")


def calculate_output_multipliers(leontief_matrix: np.ndarray) -> np.ndarray:
    """Calculate output multipliers from Leontief inverse.

    The output multiplier for an industry is the column sum of the
    Leontief inverse, representing the total economy-wide output
    generated by one dollar of final demand for that industry.

    Args:
        leontief_matrix: Total requirements matrix

    Returns:
        Array of output multipliers (one per industry)
    """
    return np.sum(leontief_matrix, axis=0)


def calculate_backward_linkages(leontief_matrix: np.ndarray) -> np.ndarray:
    """Calculate backward linkage indices.

    Backward linkages measure how much an industry depends on inputs
    from other industries. High backward linkage = heavily dependent
    on supply chain.

    Args:
        leontief_matrix: Total requirements matrix

    Returns:
        Array of backward linkage indices (normalized by mean)
    """
    column_sums = np.sum(leontief_matrix, axis=0)
    mean_linkage = np.mean(column_sums)
    return column_sums / mean_linkage if mean_linkage > 0 else column_sums


def calculate_forward_linkages(leontief_matrix: np.ndarray) -> np.ndarray:
    """Calculate forward linkage indices.

    Forward linkages measure how much other industries depend on an
    industry's output. High forward linkage = critical supplier
    to the economy.

    Args:
        leontief_matrix: Total requirements matrix

    Returns:
        Array of forward linkage indices (normalized by mean)
    """
    row_sums = np.sum(leontief_matrix, axis=1)
    mean_linkage = np.mean(row_sums)
    return row_sums / mean_linkage if mean_linkage > 0 else row_sums


@dataclass
class IndustryAnalysis:
    """Analysis results for an industry."""

    industry_code: str
    industry_name: str
    sector_category: SectorCategory | None
    output_multiplier: float
    backward_linkage: float  # Dependency on others
    forward_linkage: float   # Others' dependency on this industry
    is_key_industry: bool    # High in both linkages
    top_upstream: list[tuple[str, float]]  # What it depends on
    top_downstream: list[tuple[str, float]]  # What depends on it


async def analyze_industry(
    industry_code: str,
    year: int | None = None,
    detail_level: str = "summary",
    top_n: int = 10,
) -> IndustryAnalysis | None:
    """Perform comprehensive analysis of an industry's I-O relationships.

    Args:
        industry_code: BEA industry code
        year: Data year (uses latest if None)
        detail_level: 'sector' or 'summary'
        top_n: Number of top dependencies to return

    Returns:
        IndustryAnalysis or None if data unavailable
    """
    if year is None:
        from src.storage.timescale import get_db
        from src.storage.models import IOCoefficient
        from sqlalchemy import select, func

        db = get_db()
        async with db.session() as session:
            query = select(func.max(IOCoefficient.year)).where(
                IOCoefficient.detail_level == detail_level
            )
            result = await session.execute(query)
            year = result.scalar()

        if not year:
            return None

    matrix = await build_io_matrix(
        year=year,
        table_type="total_requirements",
        detail_level=detail_level,
    )

    if matrix is None or industry_code not in matrix.industries:
        return None

    output_mults = calculate_output_multipliers(matrix.matrix)
    backward = calculate_backward_linkages(matrix.matrix)
    forward = calculate_forward_linkages(matrix.matrix)

    idx = matrix.industries.index(industry_code)

    upstream = matrix.get_column(industry_code)
    top_upstream = sorted(
        upstream.items(), key=lambda x: x[1], reverse=True
    )[:top_n]

    downstream = matrix.get_row(industry_code)
    top_downstream = sorted(
        downstream.items(), key=lambda x: x[1], reverse=True
    )[:top_n]

    is_key = backward[idx] > 1.0 and forward[idx] > 1.0

    return IndustryAnalysis(
        industry_code=industry_code,
        industry_name=matrix.industry_names.get(industry_code, ""),
        sector_category=map_bea_to_sector(industry_code),
        output_multiplier=float(output_mults[idx]),
        backward_linkage=float(backward[idx]),
        forward_linkage=float(forward[idx]),
        is_key_industry=is_key,
        top_upstream=top_upstream,
        top_downstream=top_downstream,
    )


async def aggregate_by_sector(
    io_matrix: IOMatrix,
) -> dict[SectorCategory, dict[SectorCategory, float]]:
    """Aggregate I-O coefficients by internal sector category.

    This maps the detailed BEA industry coefficients to our higher-level
    sector categories (ENERGY, MANUFACTURING, etc.) for bottleneck analysis.

    Args:
        io_matrix: The I-O matrix to aggregate

    Returns:
        Nested dict: from_sector -> to_sector -> aggregated coefficient
    """
    sector_industries: dict[SectorCategory, list[str]] = {}

    for industry in io_matrix.industries:
        sector = map_bea_to_sector(industry)
        if sector:
            if sector not in sector_industries:
                sector_industries[sector] = []
            sector_industries[sector].append(industry)

    result: dict[SectorCategory, dict[SectorCategory, float]] = {}

    for from_sector, from_industries in sector_industries.items():
        result[from_sector] = {}

        for to_sector, to_industries in sector_industries.items():
            total = 0.0
            count = 0

            for from_ind in from_industries:
                for to_ind in to_industries:
                    coef = io_matrix.get_coefficient(from_ind, to_ind)
                    if coef > 0:
                        total += coef
                        count += 1

            # Store average (to normalize for sector size)
            if count > 0:
                result[from_sector][to_sector] = total / count

    return result


async def update_sector_dependencies(year: int) -> int:
    """Update sector_dependencies table from I-O coefficients.

    This translates the detailed I-O data into our simplified sector
    dependency graph used for bottleneck detection.

    Args:
        year: Data year to process

    Returns:
        Number of dependencies updated
    """
    from src.storage.timescale import get_db
    from src.storage.models import SectorDependency
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    matrix = await build_io_matrix(
        year=year,
        table_type="total_requirements",
        detail_level="summary",
    )

    if matrix is None:
        logger.warning("No I-O matrix available for sector dependency update")
        return 0

    sector_deps = await aggregate_by_sector(matrix)

    records = []
    for from_sector, to_sectors in sector_deps.items():
        for to_sector, weight in to_sectors.items():
            # Normalize weight to 0-1 range (coefficients can exceed 1)
            normalized_weight = min(1.0, weight)

            records.append({
                "upstream_sector": from_sector.value,
                "downstream_sector": to_sector.value,
                "weight": normalized_weight,
                "dependency_type": "supply",
                "source": "BEA_IO",
                "year": year,
                "extra_data": {"raw_coefficient": weight},
            })

    if not records:
        return 0

    db = get_db()
    async with db.session() as session:
        stmt = pg_insert(SectorDependency).values(records)
        stmt = stmt.on_conflict_do_update(
            constraint="sector_dependencies_upstream_sector_downstream_sector_source_key",
            set_={
                "weight": stmt.excluded.weight,
                "extra_data": stmt.excluded.extra_data,
            },
        )

        await session.execute(stmt)
        await session.commit()

    logger.info(
        "Updated sector dependencies from I-O data",
        year=year,
        dependencies=len(records),
    )

    return len(records)
