"""BNPL (Buy Now Pay Later) SEC filing collector.

Extracts credit-risk metrics from Affirm (10-Q/10-K) and Klarna (20-F)
via two complementary strategies:

1. **XBRL CompanyFacts API** (primary for US-GAAP filers like Affirm):
   Structured financial data -- no parsing needed.

2. **HTML filing parsing** (fallback, and primary for IFRS filers like
   Klarna whose XBRL coverage is limited):
   BeautifulSoup table extraction with regex on cleaned text.

API: https://data.sec.gov (no key required, User-Agent required)
Filing archive: https://www.sec.gov/Archives/edgar/data/...
Rate Limit: 10 requests/second
"""

import re
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import structlog
from bs4 import BeautifulSoup

from src.config.settings import get_settings
from src.data_ingestion.base_collector import (
    BaseCollector,
    DataFrequency,
    DataPoint,
)
from src.data_ingestion.rate_limiter import get_rate_limiter

logger = structlog.get_logger()

BNPL_COMPANIES: dict[str, dict[str, str]] = {
    "affirm": {
        "cik": "0001820953",
        "name": "Affirm Holdings Inc",
        "ticker": "AFRM",
        "accounting": "us-gaap",
    },
    "klarna": {
        "cik": "0002003292",
        "name": "Klarna Group plc",
        "ticker": "KLAR",
        "accounting": "ifrs-full",
    },
}

XBRL_FACTS_OF_INTEREST = [
    "ProvisionForLoanLossesExpensed",
    "FinancingReceivableAllowanceForCreditLosses",
    "FinancingReceivableRecordedInvestment90DaysPastDueAndStillAccruing",
    "FinancingReceivableAllowanceForCreditLossesWriteOffs",
    "FinancingReceivableAllowanceForCreditLossesRecovery",
]

IFRS_FACTS_OF_INTEREST = [
    "ImpairmentLossImpairmentGainAndReversalOfImpairmentLossDeterminedInAccordanceWithIFRS9",
    "Provisions",
    "ChangesInOtherProvisions",
]

DELINQUENCY_PATTERNS = [
    re.compile(
        r"(?:30|thirty)\s*[\u2010\u2011\u2012\u2013\u2014\u2015\u2212+\-]+\s*"
        r"(?:day|calendar).*?past\s+due.*?"
        r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?)",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"(?:delinquency|delinquent|past\s+due)\s+rate.*?"
        r"(\d{1,3}(?:\.\d{1,2})?)\s*%",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"(\d{1,3}(?:\.\d{1,2})?)\s*%\s*(?:of\s+)?"
        r"(?:loan|receivable).*?"
        r"(?:delinquent|past\s+due)",
        re.IGNORECASE | re.DOTALL,
    ),
]

PROVISION_PATTERNS = [
    re.compile(
        r"(?:provision\s+for\s+(?:credit|loan)\s+loss\w*)"
        r".*?\$?\s*(\d[\d,]*(?:\.\d+)?)\s*(?:million|billion)?",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"(?:allowance\s+for\s+(?:credit|loan)\s+loss\w*)"
        r".*?\$?\s*(\d[\d,]*(?:\.\d+)?)\s*(?:million|billion)?",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"(?:impairment\s+loss).*?IFRS\s*9"
        r".*?\$?\s*(\d[\d,]*(?:\.\d+)?)\s*(?:million|billion)?",
        re.IGNORECASE | re.DOTALL,
    ),
]


class BNPLSECCollector(BaseCollector):
    """Collector for BNPL company SEC filings."""

    API_BASE = "https://data.sec.gov"
    ARCHIVE_BASE = "https://www.sec.gov/Archives/edgar/data"

    def __init__(self) -> None:
        super().__init__(name="BNPL SEC", source_id="bnpl_sec")
        self.settings = get_settings()
        self.rate_limiter = get_rate_limiter("sec_edgar")
        self._user_agent = self.settings.sec_user_agent

    @property
    def frequency(self) -> DataFrequency:
        return DataFrequency.WEEKLY

    def get_schedule(self) -> str:
        return "0 1 * * 1"

    def get_default_series(self) -> list[str]:
        series: list[str] = []
        for company in BNPL_COMPANIES:
            prefix = f"BNPL_{company.upper()}"
            series.extend([
                f"{prefix}_PROVISION",
                f"{prefix}_ALLOWANCE",
                f"{prefix}_90PLUS_PAST_DUE",
                f"{prefix}_WRITE_OFFS",
            ])
        return series

    async def validate_api_key(self) -> bool:
        try:
            async with httpx.AsyncClient(
                timeout=30.0,
                headers={"User-Agent": self._user_agent},
                follow_redirects=True,
            ) as client:
                async with self.rate_limiter:
                    cik = BNPL_COMPANIES["affirm"]["cik"]
                    response = await client.get(
                        f"{self.API_BASE}/submissions/CIK{cik}.json",
                    )
                return response.status_code == 200
        except Exception as e:
            self.logger.error("BNPL SEC validation failed", error=str(e))
            return False

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        if start_date is None:
            start_date = datetime.now(UTC) - timedelta(days=730)
        if end_date is None:
            end_date = datetime.now(UTC)

        data_points: list[DataPoint] = []

        async with httpx.AsyncClient(
            timeout=120.0,
            headers={
                "User-Agent": self._user_agent,
                "Accept-Encoding": "gzip, deflate",
            },
            follow_redirects=True,
        ) as client:
            for company_key, company_info in BNPL_COMPANIES.items():
                try:
                    points = await self._collect_company(
                        client, company_key, company_info, start_date, end_date,
                    )
                    data_points.extend(points)
                except Exception as e:
                    self.logger.error(
                        "Failed to collect BNPL data",
                        company=company_key,
                        error=str(e),
                    )

        self.logger.info(
            "BNPL SEC collection complete", total_points=len(data_points),
        )
        return data_points

    async def _collect_company(
        self,
        client: httpx.AsyncClient,
        company_key: str,
        company_info: dict[str, str],
        start_date: datetime,
        end_date: datetime,
    ) -> list[DataPoint]:
        cik = company_info["cik"]
        accounting = company_info.get("accounting", "us-gaap")

        xbrl_points = await self._collect_via_xbrl(
            client, company_key, cik, accounting, start_date, end_date,
        )

        if xbrl_points:
            self.logger.info(
                "Collected BNPL data via XBRL",
                company=company_key,
                records=len(xbrl_points),
            )
            return xbrl_points

        self.logger.info(
            "XBRL data unavailable, falling back to filing HTML",
            company=company_key,
        )
        return await self._collect_via_filings(
            client, company_key, cik, start_date, end_date,
        )

    async def _collect_via_xbrl(
        self,
        client: httpx.AsyncClient,
        company_key: str,
        cik: str,
        accounting: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[DataPoint]:
        async with self.rate_limiter:
            response = await client.get(
                f"{self.API_BASE}/api/xbrl/companyfacts/CIK{cik}.json",
            )

        if response.status_code != 200:
            return []

        try:
            data = response.json()
        except Exception:
            return []

        namespace = data.get("facts", {}).get(accounting, {})
        if not namespace:
            return []

        fact_list = (
            XBRL_FACTS_OF_INTEREST if accounting == "us-gaap"
            else IFRS_FACTS_OF_INTEREST
        )

        data_points: list[DataPoint] = []
        prefix = f"BNPL_{company_key.upper()}"

        for fact_name in fact_list:
            fact_data = namespace.get(fact_name)
            if not fact_data:
                continue

            for unit_key, records in fact_data.get("units", {}).items():
                seen_ends: set[str] = set()
                for rec in records:
                    end_date_str = rec.get("end", "")
                    if not end_date_str or end_date_str in seen_ends:
                        continue

                    try:
                        ts = datetime.strptime(end_date_str, "%Y-%m-%d").replace(
                            tzinfo=UTC,
                        )
                    except ValueError:
                        continue

                    if ts < start_date or ts > end_date:
                        continue

                    seen_ends.add(end_date_str)
                    val = rec.get("val")
                    if val is None:
                        continue

                    form_type = rec.get("form", "")
                    if form_type not in ("10-Q", "10-K", "20-F"):
                        continue

                    series_suffix = self._xbrl_fact_to_series(fact_name)
                    unit = "USD" if unit_key == "USD" else unit_key

                    data_points.append(
                        DataPoint(
                            source_id=self.source_id,
                            series_id=f"{prefix}_{series_suffix}",
                            timestamp=ts,
                            value=float(val),
                            unit=unit,
                            metadata={
                                "company": company_key,
                                "form_type": form_type,
                                "xbrl_fact": fact_name,
                                "fiscal_year": rec.get("fy"),
                                "fiscal_period": rec.get("fp"),
                            },
                        )
                    )

        return data_points

    @staticmethod
    def _xbrl_fact_to_series(fact_name: str) -> str:
        mapping = {
            "ProvisionForLoanLossesExpensed": "PROVISION",
            "FinancingReceivableAllowanceForCreditLosses": "ALLOWANCE",
            "FinancingReceivableRecordedInvestment90DaysPastDueAndStillAccruing": "90PLUS_PAST_DUE",
            "FinancingReceivableAllowanceForCreditLossesWriteOffs": "WRITE_OFFS",
            "FinancingReceivableAllowanceForCreditLossesRecovery": "RECOVERIES",
            "ImpairmentLossImpairmentGainAndReversalOfImpairmentLossDeterminedInAccordanceWithIFRS9": "IMPAIRMENT_IFRS9",
            "Provisions": "PROVISIONS",
            "ChangesInOtherProvisions": "PROVISION_CHANGES",
        }
        return mapping.get(fact_name, fact_name.upper())

    async def _collect_via_filings(
        self,
        client: httpx.AsyncClient,
        company_key: str,
        cik: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[DataPoint]:
        async with self.rate_limiter:
            response = await client.get(
                f"{self.API_BASE}/submissions/CIK{cik}.json",
            )

        if response.status_code != 200:
            self.logger.warning(
                "Failed to fetch submissions",
                company=company_key,
                status=response.status_code,
            )
            return []

        submissions = response.json()
        recent = submissions.get("filings", {}).get("recent", {})

        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])

        data_points: list[DataPoint] = []

        for i, form in enumerate(forms):
            if form not in ("10-Q", "10-K", "20-F"):
                continue

            try:
                filing_date = datetime.strptime(dates[i], "%Y-%m-%d").replace(
                    tzinfo=UTC,
                )
            except (ValueError, IndexError):
                continue

            if filing_date < start_date or filing_date > end_date:
                continue

            accession = accessions[i].replace("-", "")
            primary_doc = primary_docs[i]
            cik_stripped = cik.lstrip("0")
            file_url = (
                f"{self.ARCHIVE_BASE}/{cik_stripped}/{accession}/{primary_doc}"
            )

            try:
                metrics = await self._extract_from_html(client, file_url)
            except Exception as e:
                self.logger.warning(
                    "Failed to extract metrics from filing",
                    company=company_key,
                    filing_date=dates[i],
                    url=file_url,
                    error=str(e),
                )
                continue

            if not any(v is not None for v in metrics.values()):
                self.logger.info(
                    "No metrics extracted from filing",
                    company=company_key,
                    filing_date=dates[i],
                    form=form,
                )
                continue

            prefix = f"BNPL_{company_key.upper()}"

            if metrics.get("provision") is not None:
                data_points.append(
                    DataPoint(
                        source_id=self.source_id,
                        series_id=f"{prefix}_PROVISION",
                        timestamp=filing_date,
                        value=metrics["provision"],
                        unit="USD",
                        metadata={
                            "company": company_key,
                            "form_type": form,
                            "filing_date": dates[i],
                            "source": "html_parsing",
                        },
                    )
                )

            if metrics.get("delinquency_pct") is not None:
                data_points.append(
                    DataPoint(
                        source_id=self.source_id,
                        series_id=f"{prefix}_DELINQUENCY_PCT",
                        timestamp=filing_date,
                        value=metrics["delinquency_pct"],
                        unit="percent",
                        metadata={
                            "company": company_key,
                            "form_type": form,
                            "filing_date": dates[i],
                            "source": "html_parsing",
                        },
                    )
                )

        self.logger.info(
            "Collected BNPL data via filing HTML",
            company=company_key,
            records=len(data_points),
        )
        return data_points

    async def _extract_from_html(
        self,
        client: httpx.AsyncClient,
        file_url: str,
    ) -> dict[str, float | None]:
        async with self.rate_limiter:
            response = await client.get(file_url)

        if response.status_code != 200:
            return {"provision": None, "delinquency_pct": None}

        soup = BeautifulSoup(response.content, "html.parser")

        provision = self._extract_provision_from_tables(soup)
        delinquency = self._extract_delinquency_pct(soup)

        return {"provision": provision, "delinquency_pct": delinquency}

    def _extract_provision_from_tables(
        self, soup: BeautifulSoup,
    ) -> float | None:
        """Search HTML tables for provision / allowance values."""
        for table in soup.find_all("table"):
            text = table.get_text(" ", strip=True)
            if not re.search(
                r"provision|allowance|impairment", text, re.IGNORECASE,
            ):
                continue

            for pattern in PROVISION_PATTERNS:
                match = pattern.search(text)
                if match:
                    try:
                        val_str = match.group(1).replace(",", "")
                        val = float(val_str)
                        if val > 0:
                            return val
                    except (ValueError, IndexError):
                        continue
        return None

    @staticmethod
    def _extract_delinquency_pct(soup: BeautifulSoup) -> float | None:
        """Search HTML tables for delinquency percentage."""
        for table in soup.find_all("table"):
            text = table.get_text(" ", strip=True)
            if not re.search(r"delinquen|past.due", text, re.IGNORECASE):
                continue

            for pattern in DELINQUENCY_PATTERNS:
                match = pattern.search(text)
                if match:
                    try:
                        val_str = match.group(1).replace(",", "")
                        val = float(val_str)
                        if 0.0 < val < 100.0:
                            return val
                    except (ValueError, IndexError):
                        continue
        return None
