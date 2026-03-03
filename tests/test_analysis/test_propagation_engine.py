"""Tests for the bottleneck propagation engine."""

import pytest
from datetime import datetime
from uuid import uuid4

from src.analysis.propagation_engine import (
    PropagationEngine,
    PropagationConfig,
    PropagationResult,
    PropagationPath,
    PropagationSeverity,
    get_propagation_engine,
)
from src.analysis.signals import (
    BottleneckSignalData,
    BottleneckCategory,
    SectorImpact,
)


@pytest.fixture
def propagation_config() -> PropagationConfig:
    """Create a test propagation config."""
    return PropagationConfig(
        max_rounds=3,
        decay_factor=0.8,
        impact_threshold=0.05,
        use_total_requirements=True,
        include_paths=True,
        damping_on_cycle=0.5,
    )


@pytest.fixture
def sample_bottleneck() -> BottleneckSignalData:
    """Create a sample bottleneck signal for testing."""
    return BottleneckSignalData(
        id=uuid4(),
        detected_at=datetime.utcnow(),
        category=BottleneckCategory.ENERGY_CRUNCH,
        subcategory="oil_price_spike",
        severity=0.8,
        confidence=0.75,
        affected_sectors=["ENERGY"],
        source_series=["DCOILWTICO"],
        description="Energy price spike detected",
    )


@pytest.fixture
def sample_sector_matrix() -> dict[str, dict[str, float]]:
    """Create a sample sector dependency matrix for testing."""
    return {
        "ENERGY": {
            "MANUFACTURING": 0.6,
            "TRANSPORTATION": 0.7,
            "AGRICULTURE": 0.4,
        },
        "MANUFACTURING": {
            "CONSUMER": 0.5,
            "TECHNOLOGY": 0.4,
            "ENERGY": 0.3,  # Feedback loop
        },
        "TRANSPORTATION": {
            "CONSUMER": 0.4,
            "MANUFACTURING": 0.3,
        },
        "AGRICULTURE": {
            "CONSUMER": 0.6,
        },
        "CONSUMER": {
            "MANUFACTURING": 0.2,
        },
        "TECHNOLOGY": {
            "MANUFACTURING": 0.3,
        },
    }


class TestPropagationConfig:
    """Tests for PropagationConfig dataclass."""

    def test_default_config(self):
        """Test default configuration values."""
        config = PropagationConfig()
        assert config.max_rounds == 5
        assert config.decay_factor == 0.85
        assert config.impact_threshold == 0.01
        assert config.use_total_requirements is True
        assert config.include_paths is True
        assert config.damping_on_cycle == 0.5

    def test_custom_config(self, propagation_config: PropagationConfig):
        """Test custom configuration values."""
        assert propagation_config.max_rounds == 3
        assert propagation_config.decay_factor == 0.8
        assert propagation_config.impact_threshold == 0.05


class TestPropagationPath:
    """Tests for PropagationPath dataclass."""

    def test_path_creation(self):
        """Test creating a propagation path."""
        path = PropagationPath(
            nodes=["ENERGY", "MANUFACTURING", "CONSUMER"],
            node_names=["Energy", "Manufacturing", "Consumer"],
            coefficients=[0.6, 0.5],
            cumulative_impact=0.24,  # 0.8 * 0.6 * 0.5
            hop_count=2,
            has_cycle=False,
        )
        assert path.hop_count == 2
        assert len(path.nodes) == 3
        assert path.has_cycle is False

    def test_path_to_dict(self):
        """Test path serialization."""
        path = PropagationPath(
            nodes=["ENERGY", "MANUFACTURING"],
            node_names=["Energy", "Manufacturing"],
            coefficients=[0.6],
            cumulative_impact=0.48,
            hop_count=1,
            has_cycle=False,
        )
        result = path.to_dict()
        assert result["nodes"] == ["ENERGY", "MANUFACTURING"]
        assert result["cumulative_impact"] == 0.48
        assert result["has_cycle"] is False


class TestPropagationEngine:
    """Tests for PropagationEngine class."""

    def test_engine_initialization(self, propagation_config: PropagationConfig):
        """Test engine initialization with config."""
        engine = PropagationEngine(config=propagation_config)
        assert engine.config.max_rounds == 3
        assert engine._io_matrix_cache == {}
        assert engine._propagation_cache == {}

    def test_global_engine_singleton(self):
        """Test that get_propagation_engine returns singleton."""
        engine1 = get_propagation_engine()
        engine2 = get_propagation_engine()
        assert engine1 is engine2

    def test_get_initial_sectors_from_signal(
        self,
        sample_bottleneck: BottleneckSignalData,
        sample_sector_matrix: dict[str, dict[str, float]],
    ):
        """Test extracting initial sectors from bottleneck signal."""
        engine = PropagationEngine()
        sectors = engine._get_initial_sectors(sample_bottleneck, sample_sector_matrix)
        assert "ENERGY" in sectors

    def test_get_initial_sectors_from_category(
        self,
        sample_sector_matrix: dict[str, dict[str, float]],
    ):
        """Test inferring sectors from category when not specified."""
        signal = BottleneckSignalData(
            category=BottleneckCategory.LABOR_TIGHTNESS,
            severity=0.7,
            confidence=0.6,
        )
        engine = PropagationEngine()
        sectors = engine._get_initial_sectors(signal, sample_sector_matrix)
        assert "MANUFACTURING" in sectors or "CONSUMER" in sectors

    def test_classify_severity_critical(self):
        """Test severity classification for critical impacts."""
        engine = PropagationEngine()
        severity = engine._classify_severity(
            total_impact=3.5,
            sectors_affected=7,
            amplifications=3,
        )
        assert severity == PropagationSeverity.CRITICAL

    def test_classify_severity_high(self):
        """Test severity classification for high impacts."""
        engine = PropagationEngine()
        severity = engine._classify_severity(
            total_impact=1.5,
            sectors_affected=4,
            amplifications=2,
        )
        assert severity == PropagationSeverity.HIGH

    def test_classify_severity_moderate(self):
        """Test severity classification for moderate impacts."""
        engine = PropagationEngine()
        severity = engine._classify_severity(
            total_impact=0.8,
            sectors_affected=2,
            amplifications=0,
        )
        assert severity == PropagationSeverity.MODERATE

    def test_classify_severity_low(self):
        """Test severity classification for low impacts."""
        engine = PropagationEngine()
        severity = engine._classify_severity(
            total_impact=0.2,
            sectors_affected=1,
            amplifications=0,
        )
        assert severity == PropagationSeverity.LOW

    def test_detect_amplification(self):
        """Test detection of feedback loops in paths."""
        engine = PropagationEngine()

        paths = [
            PropagationPath(
                nodes=["ENERGY", "MANUFACTURING", "ENERGY"],  # Cycle
                node_names=["Energy", "Manufacturing", "Energy"],
                coefficients=[0.6, 0.3],
                cumulative_impact=0.14,
                hop_count=2,
                has_cycle=True,
            ),
            PropagationPath(
                nodes=["ENERGY", "TRANSPORTATION", "CONSUMER"],  # No cycle
                node_names=["Energy", "Transportation", "Consumer"],
                coefficients=[0.7, 0.4],
                cumulative_impact=0.22,
                hop_count=2,
                has_cycle=False,
            ),
        ]

        amplified = engine.detect_amplification(paths)
        assert "ENERGY" in amplified
        assert len(amplified) == 1

    def test_clear_cache(self):
        """Test cache clearing."""
        engine = PropagationEngine()
        engine._propagation_cache["test"] = None  # type: ignore
        engine._sector_matrix_cache = {"test": 1.0}  # type: ignore

        engine.clear_cache()

        assert engine._propagation_cache == {}
        assert engine._sector_matrix_cache is None

    def test_get_sector_name(self):
        """Test sector name retrieval."""
        engine = PropagationEngine()

        assert engine._get_sector_name("ENERGY") == "Energy"
        assert engine._get_sector_name("MANUFACTURING") == "Manufacturing"
        assert engine._get_sector_name("UNKNOWN_SECTOR") == "Unknown Sector"


class TestPropagationResult:
    """Tests for PropagationResult dataclass."""

    def test_result_to_dict(self, sample_bottleneck: BottleneckSignalData):
        """Test result serialization."""
        result = PropagationResult(
            origin_bottleneck=sample_bottleneck,
            affected_sectors=[
                SectorImpact(
                    sector_code="MANUFACTURING",
                    sector_name="Manufacturing",
                    impact_score=0.48,
                    impact_type="indirect",
                    propagation_path=["ENERGY", "MANUFACTURING"],
                    lag_days=30,
                ),
            ],
            total_economic_impact=1.28,
            propagation_paths=[],
            propagation_rounds=3,
            convergence_reached=True,
            amplification_detected=[],
            severity_classification=PropagationSeverity.MODERATE,
        )

        result_dict = result.to_dict()

        assert result_dict["origin_category"] == "energy_crunch"
        assert result_dict["origin_severity"] == 0.8
        assert result_dict["total_economic_impact"] == 1.28
        assert result_dict["propagation_rounds"] == 3
        assert result_dict["convergence_reached"] is True
        assert result_dict["severity_classification"] == "moderate"
        assert len(result_dict["affected_sectors"]) == 1


@pytest.mark.asyncio
class TestAsyncPropagation:
    """Async tests for propagation engine."""

    async def test_propagate_with_hardcoded(
        self,
        sample_bottleneck: BottleneckSignalData,
        propagation_config: PropagationConfig,
    ):
        """Test propagation using hardcoded dependencies."""
        engine = PropagationEngine(config=propagation_config)

        result = await engine._propagate_with_hardcoded(
            sample_bottleneck, propagation_config
        )

        assert result is not None
        assert result.origin_bottleneck == sample_bottleneck
        assert result.propagation_rounds > 0
        assert len(result.affected_sectors) > 0

        sector_codes = [s.sector_code for s in result.affected_sectors]
        assert "ENERGY" in sector_codes  # Direct impact

    async def test_propagate_bottleneck_caching(
        self,
        sample_bottleneck: BottleneckSignalData,
    ):
        """Test that propagation results are cached."""
        engine = PropagationEngine()

        result1 = await engine.propagate_bottleneck(sample_bottleneck)
        result2 = await engine.propagate_bottleneck(sample_bottleneck)

        assert result1.analysis_timestamp == result2.analysis_timestamp

    async def test_propagate_iterative_with_matrix(
        self,
        sample_bottleneck: BottleneckSignalData,
        sample_sector_matrix: dict[str, dict[str, float]],
        propagation_config: PropagationConfig,
    ):
        """Test iterative propagation with a sector matrix."""
        engine = PropagationEngine(config=propagation_config)

        result = await engine._propagate_iterative(
            sample_bottleneck,
            sample_sector_matrix,
            propagation_config,
        )

        assert result is not None
        assert result.propagation_rounds >= 1

        direct_impacts = [
            s for s in result.affected_sectors if s.impact_type == "direct"
        ]
        indirect_impacts = [
            s for s in result.affected_sectors if s.impact_type == "indirect"
        ]

        for impact in direct_impacts:
            assert impact.impact_score == sample_bottleneck.severity

        for impact in indirect_impacts:
            assert impact.impact_score < sample_bottleneck.severity

    async def test_propagation_paths_included(
        self,
        sample_bottleneck: BottleneckSignalData,
        sample_sector_matrix: dict[str, dict[str, float]],
    ):
        """Test that propagation paths are included when configured."""
        config = PropagationConfig(include_paths=True, max_rounds=2)
        engine = PropagationEngine(config=config)

        result = await engine._propagate_iterative(
            sample_bottleneck,
            sample_sector_matrix,
            config,
        )

        assert len(result.propagation_paths) > 0

        for path in result.propagation_paths:
            assert len(path.nodes) >= 2
            assert len(path.node_names) == len(path.nodes)
            assert path.hop_count >= 1

    async def test_convergence_detection(
        self,
        sample_sector_matrix: dict[str, dict[str, float]],
    ):
        """Test that propagation converges when impacts fall below threshold."""
        low_severity_signal = BottleneckSignalData(
            category=BottleneckCategory.SENTIMENT_SHIFT,
            severity=0.2,  # Low severity
            confidence=0.5,
            affected_sectors=["CONSUMER"],
        )

        config = PropagationConfig(
            max_rounds=10,  # High max rounds
            impact_threshold=0.05,  # Moderate threshold
            decay_factor=0.5,  # Strong decay
        )

        engine = PropagationEngine(config=config)
        result = await engine._propagate_iterative(
            low_severity_signal,
            sample_sector_matrix,
            config,
        )

        assert result.convergence_reached is True
        assert result.propagation_rounds < 10
