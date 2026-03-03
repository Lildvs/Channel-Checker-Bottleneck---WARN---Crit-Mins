"""AISI Steel Production Collector.

Scrapes weekly steel production data from the American Iron and Steel Institute (AISI).
Data includes:
- Weekly raw steel production (net tons)
- Capability utilization rate (%)
- Regional breakdown (Northeast, Great Lakes, Midwest, Southern, Western)
- Year-over-year comparisons

Source: https://www.steel.org/industry-data/
Frequency: Weekly (published Monday)
Free: Yes (publicly available)
"""

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import httpx
import structlog
from bs4 import BeautifulSoup

from src.data_ingestion.base_collector import (
    BaseCollector,
    DataFrequency,
    DataPoint,
)

logger = structlog.get_logger()


@dataclass
class SteelProductionRecord:
    """A single steel production data record."""

    week_ending: datetime
    region: str
    production_tons: float | None
    utilization_rate: float | None
    yoy_change: float | None


STEEL_REGIONS = [
    "Total",
    "Northeast",
    "Great Lakes",
    "Midwest",
    "Southern",
    "Western",
]


class SteelProductionCollector(BaseCollector):
    """Collector for AISI weekly steel production data.

    Scrapes the AISI industry data page for weekly raw steel production
    estimates and capability utilization rates.
    """

    URL = "https://www.steel.org/industry-data/"

    @property
    def user_agent(self) -> str:
        """Get user-agent from settings."""
        from src.config.settings import get_settings
        return get_settings().scraper_user_agent

    def __init__(self):
        """Initialize Steel Production collector."""
        super().__init__(name="AISI Steel Production", source_id="aisi_steel")

    @property
    def frequency(self) -> DataFrequency:
        """Weekly data releases."""
        return DataFrequency.WEEKLY

    def get_schedule(self) -> str:
        """Run on Mondays at 3:00 PM UTC (after AISI publishes)."""
        return "0 15 * * 1"

    def get_default_series(self) -> list[str]:
        """Get default series to collect."""
        series = []
        for region in STEEL_REGIONS:
            region_key = region.upper().replace(" ", "_")
            series.extend([
                f"AISI_STEEL_PRODUCTION_{region_key}",
                f"AISI_STEEL_UTILIZATION_{region_key}",
            ])
        return series

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect steel production data from AISI website.

        Args:
            series_ids: List of series IDs to collect (not used for scraping)
            start_date: Start date for observations (filters results)
            end_date: End date for observations (filters results)

        Returns:
            List of collected data points
        """
        try:
            html = await self._fetch_page()
            if not html:
                return []

            records = self._parse_page(html)
            data_points = self._records_to_datapoints(records, start_date, end_date)

            self.logger.info(
                "Collected steel production data",
                records=len(records),
                data_points=len(data_points),
            )

            return data_points

        except Exception as e:
            self.logger.error("Failed to collect steel production data", error=str(e))
            return []

    async def _fetch_page(self) -> str | None:
        """Fetch the AISI industry data page.

        Returns:
            HTML content or None if request fails
        """
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(self.URL, headers=headers, follow_redirects=True)

                if response.status_code != 200:
                    self.logger.warning(
                        "AISI page request failed",
                        status_code=response.status_code,
                    )
                    return None

                return response.text

        except Exception as e:
            self.logger.error("Failed to fetch AISI page", error=str(e))
            return None

    def _parse_page(self, html: str) -> list[SteelProductionRecord]:
        """Parse the AISI industry data page.

        Args:
            html: HTML content of the page

        Returns:
            List of parsed steel production records
        """
        soup = BeautifulSoup(html, "lxml")
        records: list[SteelProductionRecord] = []

        tables = soup.find_all("table")
        for table in tables:
            table_records = self._parse_table(table)
            records.extend(table_records)

        if not records:
            records = self._parse_structured_divs(soup)

        if not records:
            records = self._parse_text_patterns(soup)

        return records

    def _parse_table(self, table) -> list[SteelProductionRecord]:
        """Parse a table element for steel production data.

        Args:
            table: BeautifulSoup table element

        Returns:
            List of parsed records
        """
        records: list[SteelProductionRecord] = []

        rows = table.find_all("tr")
        if len(rows) < 2:
            return records

        header_row = rows[0]
        headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]

        steel_keywords = ["production", "tons", "utilization", "capacity", "steel", "region"]
        if not any(kw in " ".join(headers) for kw in steel_keywords):
            return records

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if len(cells) < 2:
                continue

            record = self._parse_row(cells, headers)
            if record:
                records.append(record)

        return records

    def _parse_row(
        self,
        cells: list[str],
        headers: list[str],
    ) -> SteelProductionRecord | None:
        """Parse a single row of data.

        Args:
            cells: List of cell values
            headers: List of header names

        Returns:
            SteelProductionRecord or None
        """
        try:
            data: dict[str, str] = {}
            for i, header in enumerate(headers):
                if i < len(cells):
                    data[header] = cells[i]

            region = None
            for key in ["region", "area", "district"]:
                if key in data:
                    region = data[key]
                    break
            if not region and cells:
                # First column might be region
                region = cells[0]

            if not region or not any(r.lower() in region.lower() for r in STEEL_REGIONS):
                # Default to Total if region not recognized
                if "total" in " ".join(cells).lower():
                    region = "Total"
                else:
                    return None

            production_tons = None
            for key in ["production", "net tons", "tons", "output"]:
                if key in data:
                    production_tons = self._parse_number(data[key])
                    break

            utilization_rate = None
            for key in ["utilization", "capacity", "rate", "%"]:
                if key in data:
                    utilization_rate = self._parse_number(data[key])
                    break

            yoy_change = None
            for key in ["yoy", "change", "vs", "prior"]:
                if key in data:
                    yoy_change = self._parse_number(data[key])
                    break

            week_ending = None
            for key in ["week", "date", "ending", "period"]:
                if key in data:
                    week_ending = self._parse_date(data[key])
                    break

            if not week_ending:
                # AISI data is for the week ending Saturday
                today = datetime.now()
                days_since_saturday = (today.weekday() + 2) % 7
                week_ending = today - timedelta(days=days_since_saturday)

            return SteelProductionRecord(
                week_ending=week_ending,
                region=region,
                production_tons=production_tons,
                utilization_rate=utilization_rate,
                yoy_change=yoy_change,
            )

        except Exception as e:
            self.logger.debug("Failed to parse row", error=str(e))
            return None

    def _parse_structured_divs(self, soup) -> list[SteelProductionRecord]:
        """Parse steel data from structured div elements.

        Args:
            soup: BeautifulSoup object

        Returns:
            List of parsed records
        """
        records: list[SteelProductionRecord] = []

        # Look for divs with steel-related content
        steel_divs = soup.find_all(
            "div",
            class_=re.compile(r"(steel|production|data|stats)", re.I),
        )

        for div in steel_divs:
            text = div.get_text()

            production_match = re.search(
                r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:thousand\s+)?(?:net\s+)?tons?",
                text,
                re.I,
            )
            utilization_match = re.search(
                r"(\d+\.?\d*)\s*%?\s*(?:utilization|capacity)",
                text,
                re.I,
            )

            if production_match or utilization_match:
                production_tons = None
                if production_match:
                    production_tons = self._parse_number(production_match.group(1))

                utilization_rate = None
                if utilization_match:
                    utilization_rate = self._parse_number(utilization_match.group(1))

                # Get the most recent Saturday
                today = datetime.now()
                days_since_saturday = (today.weekday() + 2) % 7
                week_ending = today - timedelta(days=days_since_saturday)

                records.append(
                    SteelProductionRecord(
                        week_ending=week_ending,
                        region="Total",
                        production_tons=production_tons,
                        utilization_rate=utilization_rate,
                        yoy_change=None,
                    )
                )

        return records

    def _parse_text_patterns(self, soup) -> list[SteelProductionRecord]:
        """Parse steel data from text patterns in the page.

        Args:
            soup: BeautifulSoup object

        Returns:
            List of parsed records
        """
        records: list[SteelProductionRecord] = []
        text = soup.get_text()

        # Pattern: "X,XXX,XXX net tons" or "X.X million tons"
        production_patterns = [
            r"(?P<value>\d{1,3}(?:,\d{3})*)\s*(?:net\s+)?tons",
            r"(?P<value>\d+\.?\d*)\s*million\s*tons",
        ]

        for pattern in production_patterns:
            matches = re.findall(pattern, text, re.I)
            for match in matches:
                value = self._parse_number(match)
                if value and value > 100000:  # Reasonable production amount
                    today = datetime.now()
                    days_since_saturday = (today.weekday() + 2) % 7
                    week_ending = today - timedelta(days=days_since_saturday)

                    records.append(
                        SteelProductionRecord(
                            week_ending=week_ending,
                            region="Total",
                            production_tons=value,
                            utilization_rate=None,
                            yoy_change=None,
                        )
                    )
                    break  # Only take first match

        # Pattern: "XX.X% utilization" or "capacity utilization of XX.X%"
        utilization_pattern = r"(\d+\.?\d*)\s*%?\s*(?:utilization|capacity)"
        utilization_matches = re.findall(utilization_pattern, text, re.I)
        for match in utilization_matches:
            value = self._parse_number(match)
            if value and 50 <= value <= 100:  # Reasonable utilization rate
                if records:
                    records[0].utilization_rate = value
                else:
                    today = datetime.now()
                    days_since_saturday = (today.weekday() + 2) % 7
                    week_ending = today - timedelta(days=days_since_saturday)

                    records.append(
                        SteelProductionRecord(
                            week_ending=week_ending,
                            region="Total",
                            production_tons=None,
                            utilization_rate=value,
                            yoy_change=None,
                        )
                    )
                break

        return records

    def _parse_number(self, text: str) -> float | None:
        """Parse a number from text, handling commas and percentages.

        Args:
            text: Text containing a number

        Returns:
            Parsed float or None
        """
        if not text:
            return None

        cleaned = text.replace(",", "").replace("%", "").strip()

        # Handle parentheses for negative numbers
        if cleaned.startswith("(") and cleaned.endswith(")"):
            cleaned = "-" + cleaned[1:-1]

        try:
            return float(cleaned)
        except ValueError:
            return None

    def _parse_date(self, text: str) -> datetime | None:
        """Parse a date from text.

        Args:
            text: Text containing a date

        Returns:
            Parsed datetime or None
        """
        if not text:
            return None

        formats = [
            "%m/%d/%Y",
            "%m-%d-%Y",
            "%Y-%m-%d",
            "%B %d, %Y",
            "%b %d, %Y",
            "%d %B %Y",
            "%d %b %Y",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(text.strip(), fmt)
            except ValueError:
                continue

        return None

    def _records_to_datapoints(
        self,
        records: list[SteelProductionRecord],
        start_date: datetime | None,
        end_date: datetime | None,
    ) -> list[DataPoint]:
        """Convert steel production records to DataPoints.

        Args:
            records: List of steel production records
            start_date: Filter start date
            end_date: Filter end date

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        for record in records:
            if start_date and record.week_ending < start_date:
                continue
            if end_date and record.week_ending > end_date:
                continue

            region_key = record.region.upper().replace(" ", "_")

            if record.production_tons is not None:
                data_points.append(
                    DataPoint(
                        source_id=self.source_id,
                        series_id=f"AISI_STEEL_PRODUCTION_{region_key}",
                        timestamp=record.week_ending,
                        value=record.production_tons,
                        unit="net tons",
                        metadata={
                            "region": record.region,
                            "metric": "production",
                            "yoy_change": record.yoy_change,
                        },
                    )
                )

            if record.utilization_rate is not None:
                data_points.append(
                    DataPoint(
                        source_id=self.source_id,
                        series_id=f"AISI_STEEL_UTILIZATION_{region_key}",
                        timestamp=record.week_ending,
                        value=record.utilization_rate,
                        unit="percent",
                        metadata={
                            "region": record.region,
                            "metric": "utilization",
                        },
                    )
                )

        return data_points


_collector: SteelProductionCollector | None = None


def get_steel_production_collector() -> SteelProductionCollector:
    """Get the SteelProductionCollector singleton instance.

    Returns:
        SteelProductionCollector instance
    """
    global _collector
    if _collector is None:
        _collector = SteelProductionCollector()
    return _collector
