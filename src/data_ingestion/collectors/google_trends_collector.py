"""Google Trends collector for search interest data.

Collects two keyword sets:
1. TRENDS_DEFAULT_KEYWORDS -- general economic bottleneck terms (28 keywords)
2. TRENDS_STRESS_KEYWORDS -- revealed-preference financial stress signals (250 keywords)

The stress keywords produce a composite TRENDS_STRESS_INDEX that feeds into
the Sentiment Shift detector. Rate limiting is aggressive to avoid Google
blocking: 30-second delays between batches, exponential backoff on 429s.
"""

import asyncio
import time
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import structlog
from pytrends.request import TrendReq

from src.data_ingestion.base_collector import (
    BaseCollector,
    DataFrequency,
    DataPoint,
)
from src.data_ingestion.rate_limiter import get_rate_limiter

logger = structlog.get_logger()

TRENDS_DEFAULT_KEYWORDS = [
    "supply chain",
    "shortage",
    "out of stock",
    "backorder",
    "shipping delay",
    "hiring",
    "job openings",
    "layoffs",
    "unemployment",
    "quit job",
    "inflation",
    "price increase",
    "cost of living",
    "expensive",
    "buy house",
    "rent increase",
    "mortgage rates",
    "housing market",
    "gas prices",
    "electricity cost",
    "energy bill",
    "oil prices",
    "recession",
    "save money",
    "budget",
    "credit card debt",
]

# Financial stress keywords (250 total) -- revealed-preference behavioral signals
# These capture what people actually DO when under financial pressure,
# not what they tell a surveyor. Each category has direct search terms
# plus question-style "how do I..." queries. Organized by stress category.
TRENDS_STRESS_KEYWORDS = [
    "buy now pay later",
    "affirm payment plan",
    "klarna pay later",
    "afterpay shopping",
    "zip pay later",
    "sezzle payment plan",
    "pay in 4 installments",
    "no credit check financing",
    "point of sale financing",
    "shop now pay later",
    "split my payment",
    "deferred payment plan",
    "layaway online",
    "rent to own electronics",
    "progressive leasing",
    "how to use buy now pay later",
    "how does affirm work",
    "how to pay with klarna",
    "how to split payments online",
    "how to buy groceries with afterpay",
    "can I use afterpay for rent",
    "can I use klarna for bills",
    "how to pay bills in installments",
    "how to get approved for affirm",
    "can you use buy now pay later for gas",
    "how to finance furniture no credit",
    "how to buy a laptop with no money",
    "can I use afterpay at walmart",
    "how to get buy now pay later with bad credit",
    "how to pay for car repairs in installments",
    "can you use klarna for medical bills",
    "how to split rent payment",
    "how to finance tires no credit check",
    "can I use affirm for groceries",
    "how to pay for dental work with no money",
    "how to buy appliances with no credit",
    "how does progressive leasing work",
    "how to get approved for sezzle",
    "can I use afterpay for utilities",
    "how to finance emergency expenses",
    "second job near me",
    "side hustle ideas",
    "gig economy jobs",
    "part time work near me",
    "weekend job openings",
    "work from home extra income",
    "uber driver sign up",
    "doordash driver application",
    "instacart shopper sign up",
    "amazon flex driver",
    "grubhub delivery driver",
    "lyft driver requirements",
    "taskrabbit sign up",
    "fiverr freelance",
    "upwork freelance jobs",
    "sell on etsy",
    "sell on ebay",
    "how to make money online",
    "tutoring jobs near me",
    "babysitting jobs near me",
    "dog walking jobs",
    "house cleaning jobs",
    "freelance writing jobs",
    "virtual assistant jobs",
    "night shift jobs near me",
    "how to make extra money on the side",
    "how to start driving for uber",
    "how to make money from home fast",
    "how do I become a doordash driver",
    "how to get a second job without employer knowing",
    "how to make money on weekends",
    "how to sell stuff online for cash",
    "how to start a side hustle with no money",
    "how to make 1000 dollars fast",
    "how to make money while working full time",
    "how can I make money today",
    "how to get paid same day",
    "how to start freelancing with no experience",
    "how to sell my skills online",
    "how to make money with my car",
    "how to get a job that pays daily",
    "how to work two jobs at once",
    "how to make money overnight",
    "how do I start delivering for amazon",
    "how to earn money without a degree",
    "how to monetize my hobby",
    "how to get hired fast no experience",
    "how to make rent money fast",
    "what jobs pay cash same day",
    "how to pick up extra shifts",
    "payday loan near me",
    "cash advance app",
    "earnin app money",
    "dave app advance",
    "brigit cash advance",
    "moneylion advance",
    "chime spot me",
    "pawn shop near me",
    "title loan near me",
    "sell plasma near me",
    "donate plasma for cash",
    "sell my phone for cash",
    "sell my car fast",
    "quick cash today",
    "emergency loan bad credit",
    "personal loan no credit",
    "borrow money instantly",
    "need money today",
    "overdraft apps",
    "cash advance no interest",
    "check cashing near me",
    "401k hardship withdrawal",
    "borrow from 401k",
    "cash out refinance",
    "emergency fund help",
    "how to get money right now",
    "how to get a loan with no credit",
    "how do I cash out my 401k early",
    "how to get a payday loan online",
    "how to pawn something without losing it",
    "how to get cash advance from credit card",
    "how to borrow money with bad credit",
    "how much can I get for selling plasma",
    "how to sell my stuff for cash fast",
    "how to get emergency cash today",
    "how do I get overdraft protection",
    "how to get money before payday",
    "how to avoid overdraft fees",
    "how to get a title loan on my car",
    "how to get approved for a personal loan",
    "can I withdraw from my 401k for an emergency",
    "how to get a cash advance with no job",
    "how to sell my car same day",
    "how to get money from life insurance",
    "how to borrow against my house",
    "how to get cash from a credit card without interest",
    "how do I pawn my jewelry",
    "how to get an emergency loan today",
    "how to make 500 dollars fast",
    "where can I sell my electronics for cash",
    "cheap apartments near me",
    "affordable apartments",
    "studio apartment cheap",
    "low income apartments",
    "section 8 application",
    "housing assistance program",
    "roommate wanted",
    "find roommate",
    "shared apartment",
    "downsize home",
    "cheapest cities to live",
    "relocate for cheaper rent",
    "move to lower cost city",
    "sublease my apartment",
    "break apartment lease",
    "rent assistance program",
    "hud housing",
    "subsidized housing",
    "income based apartments",
    "mobile home for rent",
    "how to break my apartment lease",
    "how to get out of a lease early",
    "how to find cheap rent",
    "how to negotiate lower rent",
    "how to apply for section 8 housing",
    "how long is the section 8 waiting list",
    "how to get emergency housing assistance",
    "how to find a roommate to split rent",
    "how to move with no money",
    "how to downsize to a smaller apartment",
    "how to get help paying rent",
    "how to qualify for low income housing",
    "how to find income based apartments",
    "how to move to a cheaper city",
    "how to sublease my apartment fast",
    "how to break a lease without penalty",
    "how to get rental assistance from the government",
    "how to reduce my housing costs",
    "how to live in a mobile home",
    "how to find subsidized housing near me",
    "can my landlord raise rent this much",
    "how to fight a rent increase",
    "how to live with roommates as an adult",
    "how to get approved for an apartment with bad credit",
    "where is the cheapest place to rent right now",
    "food bank near me",
    "food stamps apply",
    "snap benefits application",
    "free meals near me",
    "food pantry near me",
    "wic benefits apply",
    "community meal program",
    "free groceries near me",
    "food assistance program",
    "emergency food help",
    "how to apply for food stamps",
    "how to get free food near me",
    "how to feed my family with no money",
    "how do I qualify for snap benefits",
    "how to get wic benefits",
    "how to apply for free school lunch",
    "how to get food assistance fast",
    "how to find a food pantry near me",
    "how to eat on 20 dollars a week",
    "how to stretch groceries",
    "how to get emergency food stamps",
    "how to get free baby formula",
    "how to get free diapers",
    "how to survive on minimum wage",
    "how to get help with utility bills",
    "how to get free medicine",
    "how to apply for medicaid",
    "how to get help paying electric bill",
    "how to get free internet for low income",
    "how to get a free phone from the government",
    "how to apply for rental assistance",
    "how to get gas money help",
    "how to get free clothes for my kids",
    "where to get free meals today",
    "how to get help when you are broke",
    "debt consolidation loan",
    "bankruptcy lawyer near me",
    "credit counseling free",
    "debt relief program",
    "negotiate credit card debt",
    "how to file for bankruptcy",
    "how to get out of credit card debt",
    "how to negotiate medical debt",
    "how to stop debt collectors from calling",
    "how to consolidate my debt",
    "how to settle debt for less",
    "how to get a debt collector to stop",
    "how to deal with debt I can't pay",
    "how to file chapter 7 bankruptcy",
    "how much does it cost to file bankruptcy",
    "can I go to jail for not paying debt",
    "how to stop wage garnishment",
    "how to get rid of student loan debt",
    "how to get a hardship discharge on student loans",
    "how to defer my student loan payments",
    "how to apply for income driven repayment",
    "how to get my car back after repossession",
    "how to stop a repossession",
    "how to deal with medical bills I can't afford",
    "how to negotiate hospital bills",
    "how to get collections removed from credit report",
    "how to rebuild credit after bankruptcy",
    "can I lose my house if I file bankruptcy",
    "how to protect my assets from creditors",
    "what happens if I just stop paying my credit cards",
]

STRESS_CATEGORY_WEIGHTS: dict[str, float] = {
    "bnpl_alternative_financing": 0.15,
    "seeking_additional_income": 0.25,
    "quick_cash_desperation": 0.25,
    "cheaper_housing": 0.20,
    "food_basic_needs": 0.10,
    "debt_bankruptcy": 0.05,
}

STRESS_CATEGORY_RANGES: dict[str, tuple[int, int]] = {
    "bnpl_alternative_financing": (0, 40),
    "seeking_additional_income": (40, 90),
    "quick_cash_desperation": (90, 140),
    "cheaper_housing": (140, 185),
    "food_basic_needs": (185, 220),
    "debt_bankruptcy": (220, 250),
}


class GoogleTrendsCollector(BaseCollector):
    """Collector for Google Trends search interest data.

    Collects both general economic keywords and financial stress keywords.
    Uses aggressive rate limiting to avoid Google blocking.
    """

    BATCH_DELAY_SECONDS = 30
    MAX_RETRIES_PER_BATCH = 3
    BACKOFF_BASE_SECONDS = 60
    BACKOFF_MAX_SECONDS = 3600

    def __init__(self):
        super().__init__(name="GoogleTrends", source_id="google_trends")
        self.rate_limiter = get_rate_limiter("google_trends")
        self._pytrends: TrendReq | None = None
        self._rate_limited = False

    @property
    def frequency(self) -> DataFrequency:
        return DataFrequency.DAILY

    def get_schedule(self) -> str:
        return "0 3 * * *"

    def get_default_series(self) -> list[str]:
        return TRENDS_DEFAULT_KEYWORDS

    async def validate_api_key(self) -> bool:
        return True

    def _get_pytrends(self) -> TrendReq:
        if self._pytrends is None:
            self._patch_urllib3_retry()
            try:
                self._pytrends = TrendReq(
                    hl="en-US",
                    tz=360,
                    timeout=(10, 25),
                    retries=2,
                    backoff_factor=0.5,
                )
            except TypeError:
                self._pytrends = TrendReq(
                    hl="en-US",
                    tz=360,
                    timeout=(10, 25),
                )
        return self._pytrends

    @staticmethod
    def _patch_urllib3_retry() -> None:
        """Shim urllib3.util.retry.Retry to accept the deprecated
        ``method_whitelist`` kwarg that pytrends 4.9.x still passes."""
        from urllib3.util.retry import Retry

        _orig_init = Retry.__init__

        def _patched_init(self: Retry, *args: Any, **kwargs: Any) -> None:
            if "method_whitelist" in kwargs:
                kwargs.setdefault("allowed_methods", kwargs.pop("method_whitelist"))
            _orig_init(self, *args, **kwargs)

        if not getattr(Retry, "_ccr_patched", False):
            Retry.__init__ = _patched_init  # type: ignore[method-assign]
            Retry._ccr_patched = True  # type: ignore[attr-defined]

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        self._rate_limited = False
        all_data_points: list[DataPoint] = []

        general_kw = series_ids or TRENDS_DEFAULT_KEYWORDS
        general_points = await self._collect_keyword_set(
            general_kw, start_date, end_date, series_prefix="TRENDS"
        )
        all_data_points.extend(general_points)

        if self._rate_limited:
            self.logger.warning(
                "Rate limited during general keywords, skipping stress keywords",
                general_collected=len(general_points),
            )
            return all_data_points

        stress_points = await self._collect_keyword_set(
            TRENDS_STRESS_KEYWORDS, start_date, end_date, series_prefix="TRENDS_STRESS"
        )
        all_data_points.extend(stress_points)

        stress_index = self._compute_stress_index(stress_points)
        if stress_index is not None:
            all_data_points.append(stress_index)

        self.logger.info(
            "Google Trends collection complete",
            general=len(general_points),
            stress=len(stress_points),
            total=len(all_data_points),
            rate_limited=self._rate_limited,
        )

        return all_data_points

    async def _collect_keyword_set(
        self,
        keywords: list[str],
        start_date: datetime | None,
        end_date: datetime | None,
        series_prefix: str,
    ) -> list[DataPoint]:
        all_points: list[DataPoint] = []
        batch_size = 5
        total_batches = (len(keywords) + batch_size - 1) // batch_size

        for batch_idx in range(total_batches):
            if self._rate_limited:
                break

            start_idx = batch_idx * batch_size
            batch = keywords[start_idx : start_idx + batch_size]

            retry_count = 0
            backoff = self.BACKOFF_BASE_SECONDS

            while retry_count < self.MAX_RETRIES_PER_BATCH:
                try:
                    data_points = await self._collect_batch(
                        batch, start_date, end_date, series_prefix
                    )
                    all_points.extend(data_points)
                    break
                except Exception as e:
                    error_str = str(e).lower()
                    if "429" in error_str or "too many" in error_str:
                        retry_count += 1
                        if retry_count >= self.MAX_RETRIES_PER_BATCH:
                            self.logger.warning(
                                "Rate limited, stopping collection",
                                batch_idx=batch_idx,
                                total_batches=total_batches,
                            )
                            self._rate_limited = True
                            break

                        self.logger.warning(
                            "Rate limited, backing off",
                            backoff_seconds=backoff,
                            retry=retry_count,
                        )
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, self.BACKOFF_MAX_SECONDS)
                        self._pytrends = None
                    else:
                        self.logger.error(
                            "Failed to collect trends batch",
                            keywords=batch,
                            error=str(e),
                        )
                        break

            if not self._rate_limited and batch_idx < total_batches - 1:
                await asyncio.sleep(self.BATCH_DELAY_SECONDS)

        return all_points

    def _compute_stress_index(
        self,
        stress_points: list[DataPoint],
    ) -> DataPoint | None:
        """Compute a weighted composite stress index from category averages."""
        if not stress_points:
            return None

        category_values: dict[str, list[float]] = {
            cat: [] for cat in STRESS_CATEGORY_WEIGHTS
        }

        for dp in stress_points:
            keyword = dp.metadata.get("keyword", "") if dp.metadata else ""
            kw_lower = keyword.lower()

            try:
                idx = [k.lower() for k in TRENDS_STRESS_KEYWORDS].index(kw_lower)
            except ValueError:
                continue

            for cat_name, (start, end) in STRESS_CATEGORY_RANGES.items():
                if start <= idx < end:
                    category_values[cat_name].append(dp.value)
                    break

        weighted_sum = 0.0
        weight_total = 0.0

        for cat_name, values in category_values.items():
            if not values:
                continue
            cat_mean = float(np.mean(values))
            weight = STRESS_CATEGORY_WEIGHTS.get(cat_name, 0.0)
            weighted_sum += cat_mean * weight
            weight_total += weight

        if weight_total == 0:
            return None

        composite = weighted_sum / weight_total

        latest_ts = max(
            (dp.timestamp for dp in stress_points),
            default=datetime.now(),
        )

        return DataPoint(
            source_id=self.source_id,
            series_id="TRENDS_STRESS_INDEX",
            timestamp=latest_ts,
            value=composite,
            unit="composite_index",
            metadata={
                "categories_with_data": [
                    k for k, v in category_values.items() if v
                ],
                "total_keywords_collected": len(stress_points),
            },
        )


    async def _collect_batch(
        self,
        keywords: list[str],
        start_date: datetime | None,
        end_date: datetime | None,
        series_prefix: str = "TRENDS",
    ) -> list[DataPoint]:
        """Collect trends data for a batch of keywords (max 5)."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._collect_batch_sync,
            keywords,
            start_date,
            end_date,
            series_prefix,
        )

    def _collect_batch_sync(
        self,
        keywords: list[str],
        start_date: datetime | None,
        end_date: datetime | None,
        series_prefix: str = "TRENDS",
    ) -> list[DataPoint]:
        """Synchronous collection for pytrends."""
        pytrends = self._get_pytrends()
        data_points: list[DataPoint] = []

        if start_date and end_date:
            timeframe = f"{start_date.strftime('%Y-%m-%d')} {end_date.strftime('%Y-%m-%d')}"
        elif start_date:
            timeframe = f"{start_date.strftime('%Y-%m-%d')} {datetime.now().strftime('%Y-%m-%d')}"
        else:
            end = datetime.now()
            start = end - timedelta(days=90)
            timeframe = f"{start.strftime('%Y-%m-%d')} {end.strftime('%Y-%m-%d')}"

        try:
            pytrends.build_payload(
                kw_list=keywords,
                cat=0,
                timeframe=timeframe,
                geo="US",
            )

            interest_df = pytrends.interest_over_time()

            if interest_df.empty:
                return []

            if "isPartial" in interest_df.columns:
                interest_df = interest_df.drop(columns=["isPartial"])

            for timestamp, row in interest_df.iterrows():
                for keyword in keywords:
                    if keyword in row:
                        value = float(row[keyword])
                        series_id = f"{series_prefix}_{keyword.replace(' ', '_').upper()}"

                        data_points.append(
                            DataPoint(
                                source_id=self.source_id,
                                series_id=series_id,
                                timestamp=timestamp.to_pydatetime(),
                                value=value,
                                unit="search_interest",
                                metadata={
                                    "keyword": keyword,
                                    "geo": "US",
                                    "prefix": series_prefix,
                                },
                            )
                        )

        except Exception as e:
            self.logger.error("pytrends collection failed", error=str(e))

        self.logger.debug(
            "Collected Google Trends",
            keywords=keywords,
            observations=len(data_points),
        )

        return data_points

    async def get_related_queries(self, keyword: str) -> dict[str, Any]:
        """Get related queries for a keyword.

        Args:
            keyword: Keyword to analyze

        Returns:
            Dictionary of related queries
        """
        import asyncio

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._get_related_sync,
            keyword,
        )

    def _get_related_sync(self, keyword: str) -> dict[str, Any]:
        """Get related queries synchronously."""
        pytrends = self._get_pytrends()

        try:
            pytrends.build_payload(
                kw_list=[keyword],
                timeframe="today 3-m",
                geo="US",
            )

            related = pytrends.related_queries()
            result = related.get(keyword, {})

            return {
                "top": result.get("top", {}).to_dict() if result.get("top") is not None else {},
                "rising": result.get("rising", {}).to_dict() if result.get("rising") is not None else {},
            }

        except Exception as e:
            self.logger.error("Failed to get related queries", keyword=keyword, error=str(e))
            return {}
