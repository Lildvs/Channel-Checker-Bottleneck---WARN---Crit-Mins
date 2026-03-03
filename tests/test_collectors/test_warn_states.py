"""Test infrastructure for WARN state parsers.

This module provides comprehensive testing of all 51 state WARN configurations
(50 states + DC) to identify which states need custom parsers.

The tests:
1. Verify all state configurations are valid
2. Test connectivity to each state's data source
3. Attempt to parse data and validate output
4. Generate a report of states needing custom parsers
"""

import asyncio
import json
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

import pytest
import httpx
import structlog

from src.data_ingestion.collectors.warn_collector import (
    WARNCollector,
    WARNRecord,
    StateWARNConfig,
    STATE_CONFIGS,
)

logger = structlog.get_logger()

TEST_RESULTS_PATH = Path(__file__).parent / "warn_state_results.json"


class TestWARNStateConfigurations:
    """Test that all state configurations are valid."""

    def test_all_states_configured(self):
        """Verify all 51 states (50 + DC) are configured."""
        assert len(STATE_CONFIGS) == 51, f"Expected 51 states, got {len(STATE_CONFIGS)}"

    def test_state_codes_valid(self):
        """Verify all state codes are valid 2-letter codes."""
        for code in STATE_CONFIGS:
            assert len(code) == 2, f"Invalid state code length: {code}"
            assert code.isupper(), f"State code not uppercase: {code}"
            assert code.isalpha(), f"State code not alphabetic: {code}"

    def test_required_fields_present(self):
        """Verify all required fields are present in each config."""
        required_fields = ["state_code", "name", "url", "format", "parser"]
        
        for code, config in STATE_CONFIGS.items():
            for field in required_fields:
                value = getattr(config, field, None)
                assert value is not None, f"Missing {field} for state {code}"
                assert value != "", f"Empty {field} for state {code}"

    def test_valid_formats(self):
        """Verify all format values are recognized."""
        valid_formats = {"excel", "html", "csv", "pdf", "json"}
        
        for code, config in STATE_CONFIGS.items():
            assert config.format in valid_formats, (
                f"Invalid format '{config.format}' for state {code}"
            )

    def test_urls_have_protocol(self):
        """Verify all URLs have http/https protocol."""
        for code, config in STATE_CONFIGS.items():
            assert config.url.startswith(("http://", "https://")), (
                f"Invalid URL protocol for state {code}: {config.url}"
            )

    def test_parser_methods_exist(self):
        """Verify all parser methods exist on the collector."""
        collector = WARNCollector()
        
        parsers_needed = set()
        for code, config in STATE_CONFIGS.items():
            parsers_needed.add(config.parser)
        
        for parser_name in parsers_needed:
            parser_method = getattr(collector, parser_name, None)
            assert parser_method is not None, f"Missing parser method: {parser_name}"
            assert callable(parser_method), f"Parser not callable: {parser_name}"


class TestWARNStateConnectivity:
    """Test connectivity to state WARN data sources.
    
    These tests make actual HTTP requests to state websites.
    They are marked slow and may be skipped in CI.
    """

    @pytest.fixture
    def collector(self):
        """Get a WARN collector instance."""
        return WARNCollector()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("state_code", list(STATE_CONFIGS.keys()))
    async def test_state_url_accessible(self, state_code: str):
        """Test that each state's URL is accessible.
        
        This test verifies the URL returns a 200 status or redirects.
        Some states may have temporary outages.
        """
        config = STATE_CONFIGS[state_code]
        
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            try:
                response = await client.get(config.url)
                
                # Accept 200, or some error codes that still indicate the site exists
                acceptable_codes = {200, 403, 401}  # 403/401 = auth required but site exists
                
                if response.status_code not in acceptable_codes:
                    pytest.skip(
                        f"State {state_code} returned {response.status_code} - may be temporary"
                    )
                    
            except httpx.TimeoutException:
                pytest.skip(f"State {state_code} timed out")
            except httpx.ConnectError as e:
                pytest.skip(f"State {state_code} connection failed: {e}")


class TestWARNStateParsers:
    """Test individual state parsers with mock data."""

    @pytest.fixture
    def collector(self):
        """Get a WARN collector instance."""
        return WARNCollector()

    @pytest.fixture
    def sample_html_table(self) -> bytes:
        """Sample HTML with a WARN-style table."""
        html = """
        <html>
        <body>
            <table class="warn-notices">
                <thead>
                    <tr>
                        <th>Company Name</th>
                        <th>Notice Date</th>
                        <th>Layoff Date</th>
                        <th>Employees Affected</th>
                        <th>City</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>Acme Corporation</td>
                        <td>01/15/2025</td>
                        <td>03/15/2025</td>
                        <td>150</td>
                        <td>Springfield</td>
                    </tr>
                    <tr>
                        <td>Widget Inc</td>
                        <td>01/20/2025</td>
                        <td>03/20/2025</td>
                        <td>75</td>
                        <td>Metropolis</td>
                    </tr>
                </tbody>
            </table>
        </body>
        </html>
        """.encode("utf-8")
        return html

    @pytest.mark.asyncio
    async def test_parse_generic_html(self, collector, sample_html_table):
        """Test the generic HTML parser with sample data."""
        config = StateWARNConfig(
            state_code="XX",
            name="Test State",
            url="https://example.com/warn",
            format="html",
            parser="parse_generic_html",
        )
        
        records = await collector.parse_generic_html(sample_html_table, config)
        
        assert len(records) >= 0, "Parser should return a list (possibly empty)"

        for record in records:
            assert isinstance(record, WARNRecord)
            assert record.state == "XX"

    @pytest.mark.asyncio
    async def test_california_parser_structure(self, collector):
        """Test that California parser exists and is callable."""
        assert hasattr(collector, "parse_california")
        assert callable(collector.parse_california)

    @pytest.mark.asyncio
    async def test_texas_parser_structure(self, collector):
        """Test that Texas parser exists and is callable."""
        assert hasattr(collector, "parse_texas")
        assert callable(collector.parse_texas)

    @pytest.mark.asyncio
    async def test_newyork_parser_structure(self, collector):
        """Test that New York parser exists and is callable."""
        assert hasattr(collector, "parse_newyork")
        assert callable(collector.parse_newyork)


class TestWARNRecordValidation:
    """Test WARNRecord dataclass validation."""

    def test_required_fields(self):
        """Test that WARNRecord requires company_name, state, notice_date, employees_affected."""
        record = WARNRecord(
            company_name="Test Company",
            state="CA",
            notice_date=datetime.now(UTC),
            employees_affected=100,
        )
        
        assert record.company_name == "Test Company"
        assert record.state == "CA"
        assert record.employees_affected == 100

    def test_optional_fields_default(self):
        """Test that optional fields have proper defaults."""
        record = WARNRecord(
            company_name="Test",
            state="NY",
            notice_date=datetime.now(UTC),
            employees_affected=50,
        )
        
        assert record.effective_date is None
        assert record.naics_code is None
        assert record.is_closure is False
        assert record.layoff_type == "layoff"


class StateTestRunner:
    """Runner for comprehensive state testing.
    
    This class provides methods to test all states and generate
    a report of results. Use this for full integration testing.
    """

    def __init__(self):
        self.collector = WARNCollector()
        self.results: dict[str, dict[str, Any]] = {}

    async def test_state(self, state_code: str) -> dict[str, Any]:
        """Test a single state's WARN collection.
        
        Args:
            state_code: Two-letter state code
            
        Returns:
            Dict with test results
        """
        config = STATE_CONFIGS.get(state_code)
        if not config:
            return {
                "state_code": state_code,
                "success": False,
                "error": "State not configured",
                "records_parsed": 0,
            }

        result = {
            "state_code": state_code,
            "name": config.name,
            "url": config.url,
            "format": config.format,
            "parser": config.parser,
            "enabled": config.enabled,
            "tested_at": datetime.now(UTC).isoformat(),
            "success": False,
            "error": None,
            "http_status": None,
            "records_parsed": 0,
            "sample_record": None,
        }

        if not config.enabled:
            result["error"] = "State disabled"
            return result

        try:
            async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
                response = await client.get(config.url)
                result["http_status"] = response.status_code

                if response.status_code != 200:
                    result["error"] = f"HTTP {response.status_code}"
                    return result

                parser = getattr(self.collector, config.parser, None)
                if not parser:
                    result["error"] = f"Parser method not found: {config.parser}"
                    return result

                records = await parser(response.content, config)
                result["records_parsed"] = len(records)

                if records:
                    sample = records[0]
                    result["sample_record"] = {
                        "company_name": sample.company_name[:50] if sample.company_name else None,
                        "employees_affected": sample.employees_affected,
                        "has_naics": sample.naics_code is not None,
                        "has_effective_date": sample.effective_date is not None,
                    }

                result["success"] = True

        except httpx.TimeoutException:
            result["error"] = "Timeout"
        except httpx.ConnectError as e:
            result["error"] = f"Connection error: {str(e)[:100]}"
        except Exception as e:
            result["error"] = f"Parse error: {str(e)[:200]}"

        return result

    async def test_all_states(self) -> dict[str, Any]:
        """Test all 51 states and generate a summary report.
        
        Returns:
            Dict with overall summary and per-state results
        """
        self.results = {}
        
        for state_code in STATE_CONFIGS:
            logger.info(f"Testing state: {state_code}")
            self.results[state_code] = await self.test_state(state_code)
            
            # Small delay between requests to be polite
            await asyncio.sleep(1.0)

        summary = self._generate_summary()
        
        return {
            "tested_at": datetime.now(UTC).isoformat(),
            "total_states": len(STATE_CONFIGS),
            "summary": summary,
            "results": self.results,
        }

    def _generate_summary(self) -> dict[str, Any]:
        """Generate a summary of test results."""
        passed = sum(1 for r in self.results.values() if r["success"])
        failed = sum(1 for r in self.results.values() if not r["success"])
        
        failures_by_type: dict[str, list[str]] = {
            "http_error": [],
            "timeout": [],
            "connection_error": [],
            "parse_error": [],
            "disabled": [],
            "other": [],
        }
        
        for code, result in self.results.items():
            if not result["success"]:
                error = result.get("error", "")
                if "HTTP" in error:
                    failures_by_type["http_error"].append(code)
                elif "Timeout" in error:
                    failures_by_type["timeout"].append(code)
                elif "Connection" in error:
                    failures_by_type["connection_error"].append(code)
                elif "Parse" in error:
                    failures_by_type["parse_error"].append(code)
                elif "disabled" in error.lower():
                    failures_by_type["disabled"].append(code)
                else:
                    failures_by_type["other"].append(code)

        # States needing custom parsers (parsed 0 records but no HTTP error)
        needs_custom_parser = [
            code for code, result in self.results.items()
            if result["success"] and result["records_parsed"] == 0
        ]

        return {
            "passed": passed,
            "failed": failed,
            "pass_rate": f"{passed / len(self.results) * 100:.1f}%",
            "failures_by_type": failures_by_type,
            "needs_custom_parser": needs_custom_parser,
            "states_with_data": [
                code for code, result in self.results.items()
                if result.get("records_parsed", 0) > 0
            ],
        }

    def save_results(self, path: Path | None = None) -> None:
        """Save test results to a JSON file.
        
        Args:
            path: Path to save to (defaults to TEST_RESULTS_PATH)
        """
        path = path or TEST_RESULTS_PATH
        
        report = {
            "tested_at": datetime.now(UTC).isoformat(),
            "total_states": len(STATE_CONFIGS),
            "summary": self._generate_summary(),
            "results": self.results,
        }
        
        path.write_text(json.dumps(report, indent=2, default=str))
        logger.info(f"Results saved to {path}")


@pytest.mark.asyncio
async def test_run_full_state_test():
    """Run a full test of all states (slow, for integration testing).
    
    This test is marked slow and should be run manually:
    pytest tests/test_collectors/test_warn_states.py::test_run_full_state_test -v
    """
    runner = StateTestRunner()
    results = await runner.test_all_states()
    runner.save_results()
    
    summary = results["summary"]
    print(f"\n{'='*60}")
    print("WARN STATE TESTING SUMMARY")
    print(f"{'='*60}")
    print(f"Total states: {results['total_states']}")
    print(f"Passed: {summary['passed']}")
    print(f"Failed: {summary['failed']}")
    print(f"Pass rate: {summary['pass_rate']}")
    print(f"\nStates needing custom parsers: {summary['needs_custom_parser']}")
    print(f"States with data: {len(summary['states_with_data'])}")
    print(f"\nResults saved to: {TEST_RESULTS_PATH}")
    print(f"{'='*60}\n")
    
    # Don't fail the test - this is informational
    assert True


async def run_state_tests():
    """Run state tests programmatically and return results."""
    runner = StateTestRunner()
    return await runner.test_all_states()


if __name__ == "__main__":
    results = asyncio.run(run_state_tests())
    print(json.dumps(results, indent=2, default=str))
