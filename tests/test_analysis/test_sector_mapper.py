"""Tests for sector mapper."""

import pytest

from src.analysis.sector_mapper import SectorMapper, get_sector_mapper
from src.analysis.signals import BottleneckCategory, BottleneckSignalData


class TestSectorMapper:
    """Tests for sector mapping and impact propagation."""

    def test_mapper_initialization(self):
        """Test mapper initializes with sectors."""
        mapper = SectorMapper()
        assert len(mapper.sectors) > 0
        assert "ENERGY" in mapper.sectors
        assert "MANUFACTURING" in mapper.sectors

    def test_sector_has_dependencies(self):
        """Test that sectors have dependency relationships."""
        mapper = SectorMapper()

        energy = mapper.sectors.get("ENERGY")
        assert energy is not None
        assert len(energy.downstream) > 0  # Energy affects other sectors

    def test_impact_propagation(self):
        """Test impact propagation through sectors."""
        mapper = SectorMapper()

        bottleneck = BottleneckSignalData(
            category=BottleneckCategory.ENERGY_CRUNCH,
            severity=0.8,
            confidence=0.9,
            affected_sectors=["ENERGY"],
        )

        impacts = mapper.propagate_impact(bottleneck, max_hops=2)

        assert len(impacts) > 0

        energy_impact = next((i for i in impacts if i.sector_code == "ENERGY"), None)
        assert energy_impact is not None
        assert energy_impact.impact_type == "direct"

    def test_impact_decay(self):
        """Test that impact decays with propagation."""
        mapper = SectorMapper()

        bottleneck = BottleneckSignalData(
            category=BottleneckCategory.ENERGY_CRUNCH,
            severity=0.8,
            confidence=0.9,
            affected_sectors=["ENERGY"],
        )

        impacts = mapper.propagate_impact(bottleneck, max_hops=3)

        direct = [i for i in impacts if i.impact_type == "direct"]
        downstream = [i for i in impacts if i.impact_type == "downstream"]

        if direct and downstream:
            avg_direct = sum(i.impact_score for i in direct) / len(direct)
            avg_downstream = sum(i.impact_score for i in downstream) / len(downstream)
            assert avg_direct >= avg_downstream

    def test_sector_exposure(self):
        """Test sector exposure calculation."""
        mapper = SectorMapper()

        exposure = mapper.get_sector_exposure("MANUFACTURING")

        assert exposure is not None
        assert "sector_code" in exposure
        assert "upstream_dependencies" in exposure
        assert "downstream_dependents" in exposure

    def test_all_sectors_list(self):
        """Test getting all sectors."""
        mapper = SectorMapper()
        sectors = mapper.get_all_sectors()

        assert len(sectors) > 0
        for sector in sectors:
            assert "code" in sector
            assert "name" in sector

    def test_systemic_risk_calculation(self):
        """Test systemic risk calculation."""
        mapper = SectorMapper()

        bottlenecks = [
            BottleneckSignalData(
                category=BottleneckCategory.ENERGY_CRUNCH,
                severity=0.7,
                confidence=0.8,
                affected_sectors=["ENERGY"],
            ),
            BottleneckSignalData(
                category=BottleneckCategory.LABOR_TIGHTNESS,
                severity=0.5,
                confidence=0.7,
                affected_sectors=["MANUFACTURING"],
            ),
        ]

        risk_scores = mapper.calculate_systemic_risk(bottlenecks)

        assert len(risk_scores) > 0
        assert risk_scores.get("ENERGY", 0) > 0

    def test_singleton_instance(self):
        """Test that get_sector_mapper returns singleton."""
        mapper1 = get_sector_mapper()
        mapper2 = get_sector_mapper()
        assert mapper1 is mapper2
