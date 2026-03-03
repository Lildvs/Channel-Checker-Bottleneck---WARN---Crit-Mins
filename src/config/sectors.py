"""Sector definitions and mappings for bottleneck detection."""

from dataclasses import dataclass
from enum import Enum


class SectorCategory(str, Enum):
    """High-level sector categories for bottleneck analysis."""

    ENERGY = "ENERGY"
    MANUFACTURING = "MANUFACTURING"
    AGRICULTURE = "AGRICULTURE"
    TRANSPORTATION = "TRANSPORTATION"
    TECHNOLOGY = "TECHNOLOGY"
    HOUSING = "HOUSING"
    CONSUMER = "CONSUMER"
    HEALTHCARE = "HEALTHCARE"


@dataclass(frozen=True)
class SectorDefinition:
    """Definition of a sector for analysis."""

    code: str
    name: str
    category: SectorCategory
    naics_codes: tuple[str, ...]
    description: str
    key_indicators: tuple[str, ...]  # FRED series IDs


# Sector definitions with key economic indicators
SECTOR_DEFINITIONS: dict[SectorCategory, SectorDefinition] = {
    SectorCategory.ENERGY: SectorDefinition(
        code="ENERGY",
        name="Energy",
        category=SectorCategory.ENERGY,
        naics_codes=("211", "212", "213", "22", "324"),
        description="Oil, gas, electricity, coal, and renewable energy",
        key_indicators=(
            "DCOILWTICO",  # WTI Crude Oil
            "DHHNGSP",  # Henry Hub Natural Gas
            "WCRSTUS1",  # Crude Oil Stocks
            "TOTALSA",  # Total Vehicle Sales (demand proxy)
            "IPG211111CS",  # Crude Oil Production Index
        ),
    ),
    SectorCategory.MANUFACTURING: SectorDefinition(
        code="MANUFACTURING",
        name="Manufacturing",
        category=SectorCategory.MANUFACTURING,
        naics_codes=("31", "32", "33"),
        description="All manufacturing and industrial production",
        key_indicators=(
            "INDPRO",  # Industrial Production Index
            "DGORDER",  # Durable Goods Orders
            "NEWORDER",  # Manufacturers New Orders
            "UMTMNO",  # Unfilled Orders
            "BUSINV",  # Business Inventories
        ),
    ),
    SectorCategory.AGRICULTURE: SectorDefinition(
        code="AGRICULTURE",
        name="Agriculture",
        category=SectorCategory.AGRICULTURE,
        naics_codes=("11", "311", "312"),
        description="Agriculture, food production, and processing",
        key_indicators=(
            "WPU01",  # PPI Farm Products
            "CPIUFDNS",  # CPI Food
            "PCU311311",  # PPI Food Manufacturing
            "PCUOMFGOMFG",  # PPI Total Manufacturing
        ),
    ),
    SectorCategory.TRANSPORTATION: SectorDefinition(
        code="TRANSPORTATION",
        name="Transportation",
        category=SectorCategory.TRANSPORTATION,
        naics_codes=("48", "49", "4841", "4831"),
        description="Transportation, shipping, and logistics",
        key_indicators=(
            "TSIFRGHT",  # Transportation Services Index - Freight
            "RAILFRTCARLOADSD11",  # Rail Freight Carloads
            "BDIY",  # Baltic Dry Index (via FRED)
            "GASREGW",  # Regular Gas Price
            "TRUCKD11",  # Truck Transportation Index
        ),
    ),
    SectorCategory.TECHNOLOGY: SectorDefinition(
        code="TECHNOLOGY",
        name="Technology",
        category=SectorCategory.TECHNOLOGY,
        naics_codes=("334", "5112", "5415", "518", "519"),
        description="Technology, semiconductors, and software",
        key_indicators=(
            "IPG334S",  # Computer and Electronic Products Index
            "DGORDER",  # Durable Goods (includes tech)
            "B020RE1Q156NBEA",  # Business Investment in Equipment
            "CE16OV",  # Employment (tech jobs)
        ),
    ),
    SectorCategory.HOUSING: SectorDefinition(
        code="HOUSING",
        name="Housing",
        category=SectorCategory.HOUSING,
        naics_codes=("23", "236", "531"),
        description="Housing construction and real estate",
        key_indicators=(
            "HOUST",  # Housing Starts
            "PERMIT",  # Building Permits
            "HSN1F",  # New Home Sales
            "CSUSHPINSA",  # Case-Shiller Home Price Index
            "MORTGAGE30US",  # 30-Year Mortgage Rate
        ),
    ),
    SectorCategory.CONSUMER: SectorDefinition(
        code="CONSUMER",
        name="Consumer",
        category=SectorCategory.CONSUMER,
        naics_codes=("44", "45", "722"),
        description="Consumer spending and retail",
        key_indicators=(
            "RSXFS",  # Retail Sales
            "PCEC",  # Personal Consumption Expenditures
            "UMCSENT",  # Consumer Sentiment
            "CSCICP03USM665S",  # Consumer Confidence
            "PCE",  # Personal Consumption Expenditures
        ),
    ),
    SectorCategory.HEALTHCARE: SectorDefinition(
        code="HEALTHCARE",
        name="Healthcare",
        category=SectorCategory.HEALTHCARE,
        naics_codes=("62", "3254", "339"),
        description="Healthcare, pharmaceuticals, and medical devices",
        key_indicators=(
            "CPIMEDSL",  # CPI Medical Care
            "HLTHCCTIND",  # Healthcare Cost Index
            "B933RC1Q027SBEA",  # Healthcare Spending
        ),
    ),
}


# Mapping from NAICS codes to sector categories
def get_sector_for_naics(naics_code: str) -> SectorCategory | None:
    """Get the sector category for a NAICS code."""
    for sector_def in SECTOR_DEFINITIONS.values():
        for naics in sector_def.naics_codes:
            if naics_code.startswith(naics):
                return sector_def.category
    return None


# Key inter-sector dependencies (simplified, will be loaded from BEA I-O tables)
SECTOR_DEPENDENCIES: dict[SectorCategory, list[tuple[SectorCategory, float]]] = {
    SectorCategory.ENERGY: [
        (SectorCategory.TRANSPORTATION, 0.9),
        (SectorCategory.MANUFACTURING, 0.8),
        (SectorCategory.AGRICULTURE, 0.7),
    ],
    SectorCategory.MANUFACTURING: [
        (SectorCategory.CONSUMER, 0.8),
        (SectorCategory.TECHNOLOGY, 0.7),
        (SectorCategory.HOUSING, 0.6),
    ],
    SectorCategory.AGRICULTURE: [
        (SectorCategory.CONSUMER, 0.9),
        (SectorCategory.MANUFACTURING, 0.5),
    ],
    SectorCategory.TRANSPORTATION: [
        (SectorCategory.CONSUMER, 0.7),
        (SectorCategory.MANUFACTURING, 0.8),
        (SectorCategory.AGRICULTURE, 0.6),
    ],
    SectorCategory.TECHNOLOGY: [
        (SectorCategory.MANUFACTURING, 0.8),
        (SectorCategory.CONSUMER, 0.6),
        (SectorCategory.HEALTHCARE, 0.5),
    ],
    SectorCategory.HOUSING: [
        (SectorCategory.CONSUMER, 0.7),
        (SectorCategory.MANUFACTURING, 0.5),
    ],
    SectorCategory.CONSUMER: [
        (SectorCategory.MANUFACTURING, 0.4),
    ],
    SectorCategory.HEALTHCARE: [
        (SectorCategory.CONSUMER, 0.3),
    ],
}
