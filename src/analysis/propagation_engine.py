"""Bottleneck propagation engine using I-O-based dependencies.

This module provides supply chain cascade logic that propagates detected
bottlenecks through the economy using I-O-derived dependencies from BEA
Input-Output tables.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import UUID

import numpy as np
import structlog

from src.analysis.signals import BottleneckSignalData, SectorImpact
from src.config.sectors import SectorCategory, SECTOR_DEFINITIONS
from src.config.bea_industry_mapping import map_bea_to_sector

logger = structlog.get_logger()


class PropagationSeverity(str, Enum):
    """Severity classification for propagated impacts."""

    CRITICAL = "critical"
    HIGH = "high"
    MODERATE = "moderate"
    LOW = "low"


@dataclass
class PropagationPath:
    """A single path through the supply chain."""

    nodes: list[str]  # Sector/industry codes
    node_names: list[str]  # Human-readable names
    coefficients: list[float]  # I-O coefficient at each edge
    cumulative_impact: float  # Product of initial impact and coefficients
    hop_count: int
    has_cycle: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for API/storage."""
        return {
            "nodes": self.nodes,
            "node_names": self.node_names,
            "coefficients": self.coefficients,
            "cumulative_impact": self.cumulative_impact,
            "hop_count": self.hop_count,
            "has_cycle": self.has_cycle,
        }


@dataclass
class PropagationResult:
    """Result of bottleneck propagation through supply chain."""

    origin_bottleneck: BottleneckSignalData
    affected_sectors: list[SectorImpact]
    total_economic_impact: float  # Weighted sum of all impacts
    propagation_paths: list[PropagationPath]
    propagation_rounds: int  # How many rounds occurred
    convergence_reached: bool  # Did impacts decay below threshold
    amplification_detected: list[str]  # Sectors with feedback loops
    severity_classification: PropagationSeverity
    analysis_timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for API/storage."""
        return {
            "origin_bottleneck_id": str(self.origin_bottleneck.id),
            "origin_category": self.origin_bottleneck.category.value,
            "origin_severity": self.origin_bottleneck.severity,
            "affected_sectors": [
                {
                    "sector_code": s.sector_code,
                    "sector_name": s.sector_name,
                    "impact_score": s.impact_score,
                    "impact_type": s.impact_type,
                    "propagation_path": s.propagation_path,
                    "lag_days": s.lag_days,
                }
                for s in self.affected_sectors
            ],
            "total_economic_impact": self.total_economic_impact,
            "propagation_paths": [p.to_dict() for p in self.propagation_paths],
            "propagation_rounds": self.propagation_rounds,
            "convergence_reached": self.convergence_reached,
            "amplification_detected": self.amplification_detected,
            "severity_classification": self.severity_classification.value,
            "analysis_timestamp": self.analysis_timestamp.isoformat(),
        }


@dataclass
class PropagationConfig:
    """Configuration for propagation engine."""

    max_rounds: int = 5
    decay_factor: float = 0.85  # Per-round decay to prevent infinite propagation
    impact_threshold: float = 0.01  # Minimum impact to continue propagating
    use_total_requirements: bool = True  # Use Leontief inverse vs direct requirements
    include_paths: bool = True  # Build propagation paths for visualization
    damping_on_cycle: float = 0.5  # Additional damping when cycle detected


class PropagationEngine:
    """Engine for propagating bottleneck impacts through the supply chain.

    Uses I-O coefficients from BEA Input-Output tables to calculate
    realistic economic cascade effects.
    """

    def __init__(self, config: PropagationConfig | None = None):
        """Initialize propagation engine.

        Args:
            config: Propagation configuration (uses defaults if None)
        """
        self.config = config or PropagationConfig()
        self.logger = logger.bind(component="propagation_engine")

        self._io_matrix_cache: dict[tuple[int, str], Any] = {}
        self._sector_matrix_cache: dict[int, dict[str, dict[str, float]]] | None = None
        self._propagation_cache: dict[str, PropagationResult] = {}

    async def propagate_bottleneck(
        self,
        signal: BottleneckSignalData,
        config_override: PropagationConfig | None = None,
    ) -> PropagationResult:
        """Propagate a bottleneck signal through the supply chain.

        Args:
            signal: The bottleneck signal to propagate
            config_override: Optional config override for this propagation

        Returns:
            PropagationResult with all affected sectors and paths
        """
        config = config_override or self.config

        cache_key = f"{signal.id}:{config.max_rounds}:{config.use_total_requirements}"
        if cache_key in self._propagation_cache:
            cached = self._propagation_cache[cache_key]
            # Only use cache if less than 1 hour old
            if (datetime.now(UTC) - cached.analysis_timestamp).total_seconds() < 3600:
                return cached

        sector_matrix = await self._get_sector_matrix()

        if not sector_matrix:
            # Fall back to hardcoded dependencies if no I-O data
            self.logger.warning("No I-O data available, using hardcoded dependencies")
            return await self._propagate_with_hardcoded(signal, config)

        result = await self._propagate_iterative(signal, sector_matrix, config)
        self._propagation_cache[cache_key] = result

        return result

    async def _get_sector_matrix(self) -> dict[str, dict[str, float]] | None:
        """Get sector-to-sector dependency matrix from I-O tables.

        Returns:
            Nested dict: from_sector -> to_sector -> coefficient
        """
        if self._sector_matrix_cache is not None:
            return self._sector_matrix_cache

        try:
            from src.analysis.sector_impact import get_sector_dependency_matrix

            matrix = await get_sector_dependency_matrix()

            if matrix:
                self._sector_matrix_cache = matrix
                self.logger.info(
                    "Loaded sector dependency matrix from I-O tables",
                    sectors=len(matrix),
                )

            return matrix

        except Exception as e:
            self.logger.error("Failed to load sector matrix", error=str(e))
            return None

    async def _propagate_iterative(
        self,
        signal: BottleneckSignalData,
        sector_matrix: dict[str, dict[str, float]],
        config: PropagationConfig,
    ) -> PropagationResult:
        """Run iterative cascade propagation using I-O coefficients.

        Args:
            signal: Bottleneck signal to propagate
            sector_matrix: Sector-to-sector I-O coefficients
            config: Propagation configuration

        Returns:
            PropagationResult with all affected sectors
        """
        all_impacts: list[SectorImpact] = []
        all_paths: list[PropagationPath] = []
        amplification_sectors: list[str] = []

        sector_impacts: dict[str, float] = {}
        visited_in_round: dict[int, set[str]] = {}

        initial_sectors = self._get_initial_sectors(signal, sector_matrix)

        if not initial_sectors:
            self.logger.warning(
                "No initial sectors for propagation",
                category=signal.category.value,
            )
            return PropagationResult(
                origin_bottleneck=signal,
                affected_sectors=[],
                total_economic_impact=0.0,
                propagation_paths=[],
                propagation_rounds=0,
                convergence_reached=True,
                amplification_detected=[],
                severity_classification=PropagationSeverity.LOW,
            )

        current_round: list[tuple[str, float, list[str], list[float]]] = []
        # (sector, impact, path, coefficients)

        for sector_code in initial_sectors:
            impact = signal.severity
            sector_impacts[sector_code] = impact

            sector_name = self._get_sector_name(sector_code)
            all_impacts.append(
                SectorImpact(
                    sector_code=sector_code,
                    sector_name=sector_name,
                    impact_score=impact,
                    impact_type="direct",
                    propagation_path=[sector_code],
                    lag_days=0,
                )
            )

            current_round.append((sector_code, impact, [sector_code], []))

        visited_in_round[0] = set(initial_sectors)
        rounds_completed = 0
        convergence = False

        for round_num in range(1, config.max_rounds + 1):
            next_round: list[tuple[str, float, list[str], list[float]]] = []
            visited_in_round[round_num] = set()
            round_had_propagation = False

            for source_sector, source_impact, path, path_coeffs in current_round:
                downstream = sector_matrix.get(source_sector, {})

                for target_sector, coefficient in downstream.items():
                    propagated_impact = (
                        source_impact * coefficient * config.decay_factor
                    )

                    # Check for cycles (feedback loops)
                    has_cycle = target_sector in path
                    if has_cycle:
                        # Apply additional damping
                        propagated_impact *= config.damping_on_cycle
                        if target_sector not in amplification_sectors:
                            amplification_sectors.append(target_sector)

                    if propagated_impact < config.impact_threshold:
                        continue

                    round_had_propagation = True
                    visited_in_round[round_num].add(target_sector)

                    if target_sector in sector_impacts:
                        # Take maximum (not cumulative to avoid over-counting)
                        sector_impacts[target_sector] = max(
                            sector_impacts[target_sector], propagated_impact
                        )
                    else:
                        sector_impacts[target_sector] = propagated_impact

                    new_path = path + [target_sector]
                    new_coeffs = path_coeffs + [coefficient]

                    if round_num == 1:
                        impact_type = "indirect"
                    else:
                        impact_type = "downstream"

                    existing = next(
                        (
                            i
                            for i in all_impacts
                            if i.sector_code == target_sector
                            and i.impact_type == impact_type
                        ),
                        None,
                    )

                    if existing is None or existing.impact_score < propagated_impact:
                        if existing:
                            all_impacts.remove(existing)

                        all_impacts.append(
                            SectorImpact(
                                sector_code=target_sector,
                                sector_name=self._get_sector_name(target_sector),
                                impact_score=propagated_impact,
                                impact_type=impact_type,
                                propagation_path=new_path,
                                lag_days=round_num * 30,  # Rough estimate
                                metadata={
                                    "round": round_num,
                                    "has_cycle": has_cycle,
                                },
                            )
                        )

                    if config.include_paths:
                        all_paths.append(
                            PropagationPath(
                                nodes=new_path,
                                node_names=[
                                    self._get_sector_name(n) for n in new_path
                                ],
                                coefficients=new_coeffs,
                                cumulative_impact=propagated_impact,
                                hop_count=round_num,
                                has_cycle=has_cycle,
                            )
                        )

                    # Only continue propagating if not in a cycle
                    if not has_cycle:
                        next_round.append(
                            (target_sector, propagated_impact, new_path, new_coeffs)
                        )

            rounds_completed = round_num

            if not round_had_propagation:
                convergence = True
                break

            current_round = next_round

        total_impact = sum(sector_impacts.values())
        severity = self._classify_severity(
            total_impact, len(sector_impacts), len(amplification_sectors)
        )

        all_impacts.sort(key=lambda i: i.impact_score, reverse=True)
        all_paths.sort(key=lambda p: p.cumulative_impact, reverse=True)

        self.logger.info(
            "Propagation complete",
            bottleneck_id=str(signal.id),
            category=signal.category.value,
            initial_sectors=len(initial_sectors),
            total_affected=len(sector_impacts),
            rounds=rounds_completed,
            total_impact=total_impact,
            amplifications=len(amplification_sectors),
        )

        return PropagationResult(
            origin_bottleneck=signal,
            affected_sectors=all_impacts,
            total_economic_impact=total_impact,
            propagation_paths=all_paths[:50],  # Limit paths returned
            propagation_rounds=rounds_completed,
            convergence_reached=convergence,
            amplification_detected=amplification_sectors,
            severity_classification=severity,
        )

    async def _propagate_with_hardcoded(
        self,
        signal: BottleneckSignalData,
        config: PropagationConfig,
    ) -> PropagationResult:
        """No fallback - returns empty result when I-O data unavailable.

        Args:
            signal: Bottleneck signal to propagate
            config: Propagation configuration

        Returns:
            Empty PropagationResult indicating data unavailable
        """
        self.logger.error(
            "Cannot propagate bottleneck: I-O sector dependency data not available",
            bottleneck_id=str(signal.id),
            category=signal.category.value,
        )

        return PropagationResult(
            origin_bottleneck=signal,
            affected_sectors=[],
            total_economic_impact=0.0,
            propagation_paths=[],
            propagation_rounds=0,
            convergence_reached=True,
            amplification_detected=[],
            severity_classification=PropagationSeverity.LOW,
        )

    def _get_initial_sectors(
        self,
        signal: BottleneckSignalData,
        sector_matrix: dict[str, dict[str, float]],
    ) -> list[str]:
        """Get initial sectors from bottleneck signal.

        Args:
            signal: The bottleneck signal
            sector_matrix: Available sectors

        Returns:
            List of sector codes to start propagation from
        """
        if signal.affected_sectors:
            return [s for s in signal.affected_sectors if s in sector_matrix]

        from src.analysis.signals import BottleneckCategory

        category_to_sectors: dict[BottleneckCategory, list[str]] = {
            BottleneckCategory.INVENTORY_SQUEEZE: ["MANUFACTURING", "CONSUMER"],
            BottleneckCategory.PRICE_SPIKE: ["CONSUMER", "MANUFACTURING"],
            BottleneckCategory.SHIPPING_CONGESTION: [
                "TRANSPORTATION",
                "MANUFACTURING",
                "CONSUMER",
            ],
            BottleneckCategory.LABOR_TIGHTNESS: [
                "MANUFACTURING",
                "CONSUMER",
                "HEALTHCARE",
            ],
            BottleneckCategory.CAPACITY_CEILING: ["MANUFACTURING", "ENERGY"],
            BottleneckCategory.DEMAND_SURGE: ["CONSUMER", "MANUFACTURING"],
            BottleneckCategory.SUPPLY_DISRUPTION: ["MANUFACTURING", "TECHNOLOGY"],
            BottleneckCategory.ENERGY_CRUNCH: [
                "ENERGY",
                "TRANSPORTATION",
                "MANUFACTURING",
            ],
            BottleneckCategory.CREDIT_TIGHTENING: ["HOUSING", "CONSUMER"],
            BottleneckCategory.SENTIMENT_SHIFT: ["CONSUMER"],
        }

        inferred = category_to_sectors.get(signal.category, ["MANUFACTURING"])
        return [s for s in inferred if s in sector_matrix]

    def _get_sector_name(self, sector_code: str) -> str:
        """Get human-readable name for a sector.

        Args:
            sector_code: Sector code

        Returns:
            Sector name
        """
        try:
            category = SectorCategory(sector_code)
            definition = SECTOR_DEFINITIONS.get(category)
            if definition:
                return definition.name
        except ValueError:
            pass

        return sector_code.replace("_", " ").title()

    def _classify_severity(
        self,
        total_impact: float,
        sectors_affected: int,
        amplifications: int,
    ) -> PropagationSeverity:
        """Classify overall severity of propagation.

        Args:
            total_impact: Sum of all impact scores
            sectors_affected: Number of sectors affected
            amplifications: Number of feedback loops detected

        Returns:
            PropagationSeverity classification
        """
        impact_score = min(1.0, total_impact / 3.0)  # Normalize to ~3.0 max
        breadth_score = min(1.0, sectors_affected / 6.0)  # Normalize to 6 sectors
        amplification_score = min(1.0, amplifications / 3.0)

        combined = 0.5 * impact_score + 0.3 * breadth_score + 0.2 * amplification_score

        if combined >= 0.7 or amplifications >= 3:
            return PropagationSeverity.CRITICAL
        elif combined >= 0.4 or amplifications >= 2:
            return PropagationSeverity.HIGH
        elif combined >= 0.2:
            return PropagationSeverity.MODERATE
        else:
            return PropagationSeverity.LOW

    def detect_amplification(
        self,
        paths: list[PropagationPath],
    ) -> list[str]:
        """Detect sectors with feedback loops (amplification risk).

        Args:
            paths: List of propagation paths

        Returns:
            List of sector codes with detected amplification
        """
        amplified: list[str] = []

        for path in paths:
            if len(path.nodes) != len(set(path.nodes)):
                seen: set[str] = set()
                for node in path.nodes:
                    if node in seen and node not in amplified:
                        amplified.append(node)
                    seen.add(node)

        return amplified

    def clear_cache(self) -> None:
        """Clear all cached data."""
        self._io_matrix_cache.clear()
        self._sector_matrix_cache = None
        self._propagation_cache.clear()
        self.logger.info("Propagation engine cache cleared")


# Global propagation engine instance
_engine: PropagationEngine | None = None


def get_propagation_engine() -> PropagationEngine:
    """Get the global propagation engine instance."""
    global _engine
    if _engine is None:
        _engine = PropagationEngine()
    return _engine
