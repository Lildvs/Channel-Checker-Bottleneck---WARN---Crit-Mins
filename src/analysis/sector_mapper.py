"""Sector mapping and impact propagation using BEA Input-Output tables."""

from dataclasses import dataclass, field
from typing import Any

import structlog

from src.analysis.signals import BottleneckSignalData, SectorImpact
from src.config.sectors import (
    SECTOR_DEFINITIONS,
    SECTOR_DEPENDENCIES,
    SectorCategory,
)

logger = structlog.get_logger()


@dataclass
class SectorNode:
    """Node in the sector dependency graph."""

    code: str
    name: str
    category: SectorCategory
    upstream: list[tuple[str, float]] = field(default_factory=list)  # (sector_code, weight)
    downstream: list[tuple[str, float]] = field(default_factory=list)


class SectorMapper:
    """Maps bottlenecks to sector impacts using dependency graphs.

    Can use either hardcoded dependencies or load from I-O coefficient tables.
    """

    def __init__(self, use_io_coefficients: bool = False):
        """Initialize sector mapper with dependency data.

        Args:
            use_io_coefficients: If True, attempt to load dependencies from
                I-O coefficient tables. Falls back to hardcoded if unavailable.
        """
        self.logger = logger.bind(component="sector_mapper")
        self.sectors: dict[str, SectorNode] = {}
        self._use_io_coefficients = use_io_coefficients
        self._io_dependencies_loaded = False
        self._build_graph()

    def _build_graph(self) -> None:
        """Build the sector dependency graph."""
        for category, definition in SECTOR_DEFINITIONS.items():
            node = SectorNode(
                code=definition.code,
                name=definition.name,
                category=category,
            )
            self.sectors[definition.code] = node

        self._build_from_hardcoded()

        self.logger.info(
            "Sector graph built",
            sectors=len(self.sectors),
            edges=sum(len(s.downstream) for s in self.sectors.values()),
            source="hardcoded",
        )

    def _build_from_hardcoded(self) -> None:
        """Build graph edges from hardcoded sector dependencies."""
        for upstream_cat, dependencies in SECTOR_DEPENDENCIES.items():
            upstream_def = SECTOR_DEFINITIONS.get(upstream_cat)
            if not upstream_def:
                continue

            upstream_code = upstream_def.code

            for downstream_cat, weight in dependencies:
                downstream_def = SECTOR_DEFINITIONS.get(downstream_cat)
                if not downstream_def:
                    continue

                downstream_code = downstream_def.code

                if upstream_code in self.sectors:
                    self.sectors[upstream_code].downstream.append((downstream_code, weight))

                if downstream_code in self.sectors:
                    self.sectors[downstream_code].upstream.append((upstream_code, weight))

    async def load_io_dependencies(self, year: int | None = None) -> bool:
        """Load sector dependencies from I-O coefficient tables.

        This replaces the hardcoded dependencies with real economic
        relationships derived from BEA Input-Output tables.

        Args:
            year: I-O data year (uses latest if None)

        Returns:
            True if successfully loaded, False otherwise
        """
        try:
            from src.analysis.sector_impact import get_sector_dependency_matrix

            io_matrix = await get_sector_dependency_matrix(year)

            if not io_matrix:
                self.logger.warning(
                    "No I-O data available, keeping hardcoded dependencies"
                )
                return False

            for sector in self.sectors.values():
                sector.upstream = []
                sector.downstream = []

            for from_sector_str, to_sectors in io_matrix.items():
                from_code = from_sector_str

                if from_code not in self.sectors:
                    continue

                for to_sector_str, coefficient in to_sectors.items():
                    to_code = to_sector_str

                    if to_code not in self.sectors:
                        continue

                    # Normalize coefficient to 0-1 range (I-O coefficients can exceed 1)
                    weight = min(1.0, coefficient)

                    if weight < 0.01:
                        continue

                    self.sectors[from_code].downstream.append((to_code, weight))

                    self.sectors[to_code].upstream.append((from_code, weight))

            self._io_dependencies_loaded = True

            self.logger.info(
                "Loaded I-O dependencies",
                sectors=len(self.sectors),
                edges=sum(len(s.downstream) for s in self.sectors.values()),
                year=year,
            )

            return True

        except Exception as e:
            self.logger.error(
                "Failed to load I-O dependencies",
                error=str(e),
            )
            return False

    def reload_hardcoded(self) -> None:
        """Reload hardcoded dependencies (discards I-O loaded data)."""
        for sector in self.sectors.values():
            sector.upstream = []
            sector.downstream = []

        self._build_from_hardcoded()
        self._io_dependencies_loaded = False

        self.logger.info(
            "Reloaded hardcoded dependencies",
            sectors=len(self.sectors),
            edges=sum(len(s.downstream) for s in self.sectors.values()),
        )

    @property
    def is_using_io_data(self) -> bool:
        """Check if mapper is using I-O derived dependencies."""
        return self._io_dependencies_loaded

    def propagate_impact(
        self,
        bottleneck: BottleneckSignalData,
        max_hops: int = 3,
        min_impact: float = 0.1,
    ) -> list[SectorImpact]:
        """Propagate bottleneck impact through the sector graph.

        Args:
            bottleneck: The bottleneck signal to propagate
            max_hops: Maximum propagation depth
            min_impact: Minimum impact score to include

        Returns:
            List of sector impacts
        """
        impacts: list[SectorImpact] = []
        visited: set[str] = set()

        initial_sectors = bottleneck.affected_sectors or []

        if not initial_sectors:
            initial_sectors = self._infer_sectors_from_category(bottleneck.category)

        queue: list[tuple[str, float, list[str], int]] = []

        for sector_code in initial_sectors:
            if sector_code in self.sectors:
                impacts.append(
                    SectorImpact(
                        sector_code=sector_code,
                        sector_name=self.sectors[sector_code].name,
                        impact_score=bottleneck.severity,
                        impact_type="direct",
                        propagation_path=[sector_code],
                        lag_days=0,
                    )
                )
                visited.add(sector_code)
                queue.append((sector_code, bottleneck.severity, [sector_code], 0))

        while queue:
            current_code, current_impact, path, hops = queue.pop(0)

            if hops >= max_hops:
                continue

            current_node = self.sectors.get(current_code)
            if not current_node:
                continue

            for downstream_code, weight in current_node.downstream:
                if downstream_code in visited:
                    continue

                propagated_impact = current_impact * weight * 0.8

                if propagated_impact < min_impact:
                    continue

                downstream_node = self.sectors.get(downstream_code)
                if not downstream_node:
                    continue

                new_path = path + [downstream_code]
                visited.add(downstream_code)

                impacts.append(
                    SectorImpact(
                        sector_code=downstream_code,
                        sector_name=downstream_node.name,
                        impact_score=propagated_impact,
                        impact_type="downstream" if hops > 0 else "indirect",
                        propagation_path=new_path,
                        lag_days=(hops + 1) * 30,  # Rough estimate: 30 days per hop
                    )
                )

                queue.append((downstream_code, propagated_impact, new_path, hops + 1))

        impacts.sort(key=lambda i: i.impact_score, reverse=True)

        self.logger.debug(
            "Impact propagation complete",
            bottleneck_category=bottleneck.category.value,
            initial_sectors=initial_sectors,
            total_impacts=len(impacts),
        )

        return impacts

    def _infer_sectors_from_category(
        self,
        category: Any,  # BottleneckCategory
    ) -> list[str]:
        """Infer affected sectors from bottleneck category."""
        from src.analysis.signals import BottleneckCategory

        category_to_sectors: dict[BottleneckCategory, list[str]] = {
            BottleneckCategory.INVENTORY_SQUEEZE: ["MANUFACTURING", "CONSUMER"],
            BottleneckCategory.PRICE_SPIKE: ["CONSUMER", "MANUFACTURING"],
            BottleneckCategory.SHIPPING_CONGESTION: ["TRANSPORTATION", "MANUFACTURING", "CONSUMER"],
            BottleneckCategory.LABOR_TIGHTNESS: ["MANUFACTURING", "CONSUMER", "HEALTHCARE"],
            BottleneckCategory.CAPACITY_CEILING: ["MANUFACTURING", "ENERGY"],
            BottleneckCategory.DEMAND_SURGE: ["CONSUMER", "MANUFACTURING"],
            BottleneckCategory.SUPPLY_DISRUPTION: ["MANUFACTURING", "TECHNOLOGY"],
            BottleneckCategory.ENERGY_CRUNCH: ["ENERGY", "TRANSPORTATION", "MANUFACTURING"],
            BottleneckCategory.CREDIT_TIGHTENING: ["HOUSING", "CONSUMER"],
            BottleneckCategory.SENTIMENT_SHIFT: ["CONSUMER"],
        }

        return category_to_sectors.get(category, ["MANUFACTURING"])

    def get_sector_exposure(
        self,
        sector_code: str,
    ) -> dict[str, Any]:
        """Get a sector's exposure to various bottleneck sources.

        Args:
            sector_code: Code of the sector to analyze

        Returns:
            Dictionary with exposure information
        """
        sector = self.sectors.get(sector_code)
        if not sector:
            return {}

        exposure = {
            "sector_code": sector_code,
            "sector_name": sector.name,
            "category": sector.category.value,
            "upstream_dependencies": [],
            "downstream_dependents": [],
            "total_upstream_exposure": 0.0,
            "total_downstream_exposure": 0.0,
        }

        for upstream_code, weight in sector.upstream:
            upstream = self.sectors.get(upstream_code)
            if upstream:
                exposure["upstream_dependencies"].append({
                    "sector_code": upstream_code,
                    "sector_name": upstream.name,
                    "weight": weight,
                })
                exposure["total_upstream_exposure"] += weight

        for downstream_code, weight in sector.downstream:
            downstream = self.sectors.get(downstream_code)
            if downstream:
                exposure["downstream_dependents"].append({
                    "sector_code": downstream_code,
                    "sector_name": downstream.name,
                    "weight": weight,
                })
                exposure["total_downstream_exposure"] += weight

        return exposure

    def get_all_sectors(self) -> list[dict[str, Any]]:
        """Get information about all sectors.

        Returns:
            List of sector information dictionaries
        """
        return [
            {
                "code": sector.code,
                "name": sector.name,
                "category": sector.category.value,
                "upstream_count": len(sector.upstream),
                "downstream_count": len(sector.downstream),
            }
            for sector in self.sectors.values()
        ]

    def calculate_systemic_risk(
        self,
        bottlenecks: list[BottleneckSignalData],
    ) -> dict[str, float]:
        """Calculate systemic risk scores for each sector based on active bottlenecks.

        Args:
            bottlenecks: List of active bottleneck signals

        Returns:
            Dictionary mapping sector codes to risk scores
        """
        risk_scores: dict[str, float] = {code: 0.0 for code in self.sectors}

        for bottleneck in bottlenecks:
            impacts = self.propagate_impact(bottleneck)

            for impact in impacts:
                if impact.sector_code in risk_scores:
                    # Accumulate risk (capped at 1.0)
                    risk_scores[impact.sector_code] = min(
                        1.0,
                        risk_scores[impact.sector_code] + impact.impact_score * bottleneck.confidence,
                    )

        return risk_scores


# Global sector mapper instance
_mapper: SectorMapper | None = None


def get_sector_mapper() -> SectorMapper:
    """Get the global sector mapper instance."""
    global _mapper
    if _mapper is None:
        _mapper = SectorMapper()
    return _mapper
