"""BEA Input-Output industry code mappings.

This module provides mappings between BEA's industry classification system
(used in Input-Output tables) and our internal sector categories.

BEA I-O Tables use a hierarchical industry classification:
- Sector level: 15 industries (aggregated)
- Summary level: 71 industries
- Detail level: 402 industries (benchmark years only)

The codes follow NAICS conventions but with BEA-specific groupings.
"""

from dataclasses import dataclass
from typing import Any

from src.config.sectors import SectorCategory


@dataclass(frozen=True)
class BEAIndustry:
    """BEA industry definition."""

    code: str
    name: str
    level: str  # 'sector', 'summary', or 'detail'
    sector_category: SectorCategory | None
    naics_equivalent: str | None = None
    notes: str | None = None


# =============================================================================
# BEA SECTOR-LEVEL INDUSTRIES (15 industries)
# Used in sector-level I-O tables
# =============================================================================

BEA_SECTOR_INDUSTRIES: dict[str, BEAIndustry] = {
    "11": BEAIndustry(
        code="11",
        name="Agriculture, forestry, fishing, and hunting",
        level="sector",
        sector_category=SectorCategory.AGRICULTURE,
        naics_equivalent="11",
    ),
    "21": BEAIndustry(
        code="21",
        name="Mining",
        level="sector",
        sector_category=SectorCategory.ENERGY,
        naics_equivalent="21",
        notes="Includes oil/gas extraction",
    ),
    "22": BEAIndustry(
        code="22",
        name="Utilities",
        level="sector",
        sector_category=SectorCategory.ENERGY,
        naics_equivalent="22",
    ),
    "23": BEAIndustry(
        code="23",
        name="Construction",
        level="sector",
        sector_category=SectorCategory.HOUSING,
        naics_equivalent="23",
    ),
    "31G": BEAIndustry(
        code="31G",
        name="Manufacturing",
        level="sector",
        sector_category=SectorCategory.MANUFACTURING,
        naics_equivalent="31-33",
        notes="Aggregated manufacturing (NAICS 31-33)",
    ),
    "42": BEAIndustry(
        code="42",
        name="Wholesale trade",
        level="sector",
        sector_category=SectorCategory.CONSUMER,
        naics_equivalent="42",
    ),
    "44RT": BEAIndustry(
        code="44RT",
        name="Retail trade",
        level="sector",
        sector_category=SectorCategory.CONSUMER,
        naics_equivalent="44-45",
    ),
    "48TW": BEAIndustry(
        code="48TW",
        name="Transportation and warehousing",
        level="sector",
        sector_category=SectorCategory.TRANSPORTATION,
        naics_equivalent="48-49",
    ),
    "51": BEAIndustry(
        code="51",
        name="Information",
        level="sector",
        sector_category=SectorCategory.TECHNOLOGY,
        naics_equivalent="51",
    ),
    "FIRE": BEAIndustry(
        code="FIRE",
        name="Finance, insurance, real estate, rental, and leasing",
        level="sector",
        sector_category=None,  # Mixed - spans multiple categories
        naics_equivalent="52-53",
    ),
    "PROF": BEAIndustry(
        code="PROF",
        name="Professional and business services",
        level="sector",
        sector_category=None,  # Mixed
        naics_equivalent="54-56",
    ),
    "6": BEAIndustry(
        code="6",
        name="Educational services, health care, and social assistance",
        level="sector",
        sector_category=SectorCategory.HEALTHCARE,
        naics_equivalent="61-62",
    ),
    "7": BEAIndustry(
        code="7",
        name="Arts, entertainment, recreation, accommodation, and food services",
        level="sector",
        sector_category=SectorCategory.CONSUMER,
        naics_equivalent="71-72",
    ),
    "81": BEAIndustry(
        code="81",
        name="Other services, except government",
        level="sector",
        sector_category=SectorCategory.CONSUMER,
        naics_equivalent="81",
    ),
    "G": BEAIndustry(
        code="G",
        name="Government",
        level="sector",
        sector_category=None,  # Government - excluded from private sector analysis
        naics_equivalent="92",
    ),
}


# =============================================================================
# BEA SUMMARY-LEVEL INDUSTRIES (71 industries)
# Most commonly used for analysis - good balance of detail vs. data quality
# =============================================================================

BEA_SUMMARY_INDUSTRIES: dict[str, BEAIndustry] = {
    # Agriculture
    "111CA": BEAIndustry(
        code="111CA",
        name="Farms",
        level="summary",
        sector_category=SectorCategory.AGRICULTURE,
        naics_equivalent="111-112",
    ),
    "113FF": BEAIndustry(
        code="113FF",
        name="Forestry, fishing, and related activities",
        level="summary",
        sector_category=SectorCategory.AGRICULTURE,
        naics_equivalent="113-115",
    ),
    # Mining (Energy)
    "211": BEAIndustry(
        code="211",
        name="Oil and gas extraction",
        level="summary",
        sector_category=SectorCategory.ENERGY,
        naics_equivalent="211",
    ),
    "212": BEAIndustry(
        code="212",
        name="Mining, except oil and gas",
        level="summary",
        sector_category=SectorCategory.ENERGY,
        naics_equivalent="212",
    ),
    "213": BEAIndustry(
        code="213",
        name="Support activities for mining",
        level="summary",
        sector_category=SectorCategory.ENERGY,
        naics_equivalent="213",
    ),
    # Utilities
    "22": BEAIndustry(
        code="22",
        name="Utilities",
        level="summary",
        sector_category=SectorCategory.ENERGY,
        naics_equivalent="22",
    ),
    # Construction
    "23": BEAIndustry(
        code="23",
        name="Construction",
        level="summary",
        sector_category=SectorCategory.HOUSING,
        naics_equivalent="23",
    ),
    # Manufacturing - Food & Beverages
    "311FT": BEAIndustry(
        code="311FT",
        name="Food and beverage and tobacco products",
        level="summary",
        sector_category=SectorCategory.AGRICULTURE,
        naics_equivalent="311-312",
    ),
    # Manufacturing - Textiles & Apparel
    "313TT": BEAIndustry(
        code="313TT",
        name="Textile mills and textile product mills",
        level="summary",
        sector_category=SectorCategory.MANUFACTURING,
        naics_equivalent="313-314",
    ),
    "315AL": BEAIndustry(
        code="315AL",
        name="Apparel and leather and allied products",
        level="summary",
        sector_category=SectorCategory.CONSUMER,
        naics_equivalent="315-316",
    ),
    # Manufacturing - Wood & Paper
    "321": BEAIndustry(
        code="321",
        name="Wood products",
        level="summary",
        sector_category=SectorCategory.HOUSING,
        naics_equivalent="321",
    ),
    "322": BEAIndustry(
        code="322",
        name="Paper products",
        level="summary",
        sector_category=SectorCategory.MANUFACTURING,
        naics_equivalent="322",
    ),
    "323": BEAIndustry(
        code="323",
        name="Printing and related support activities",
        level="summary",
        sector_category=SectorCategory.MANUFACTURING,
        naics_equivalent="323",
    ),
    # Manufacturing - Petroleum & Chemicals
    "324": BEAIndustry(
        code="324",
        name="Petroleum and coal products",
        level="summary",
        sector_category=SectorCategory.ENERGY,
        naics_equivalent="324",
    ),
    "325": BEAIndustry(
        code="325",
        name="Chemical products",
        level="summary",
        sector_category=SectorCategory.MANUFACTURING,
        naics_equivalent="325",
        notes="Includes pharmaceuticals",
    ),
    "326": BEAIndustry(
        code="326",
        name="Plastics and rubber products",
        level="summary",
        sector_category=SectorCategory.MANUFACTURING,
        naics_equivalent="326",
    ),
    # Manufacturing - Minerals & Metals
    "327": BEAIndustry(
        code="327",
        name="Nonmetallic mineral products",
        level="summary",
        sector_category=SectorCategory.MANUFACTURING,
        naics_equivalent="327",
    ),
    "331": BEAIndustry(
        code="331",
        name="Primary metals",
        level="summary",
        sector_category=SectorCategory.MANUFACTURING,
        naics_equivalent="331",
        notes="Steel, aluminum, copper production",
    ),
    "332": BEAIndustry(
        code="332",
        name="Fabricated metal products",
        level="summary",
        sector_category=SectorCategory.MANUFACTURING,
        naics_equivalent="332",
    ),
    # Manufacturing - Machinery & Equipment
    "333": BEAIndustry(
        code="333",
        name="Machinery",
        level="summary",
        sector_category=SectorCategory.MANUFACTURING,
        naics_equivalent="333",
    ),
    "334": BEAIndustry(
        code="334",
        name="Computer and electronic products",
        level="summary",
        sector_category=SectorCategory.TECHNOLOGY,
        naics_equivalent="334",
        notes="Includes semiconductors",
    ),
    "335": BEAIndustry(
        code="335",
        name="Electrical equipment, appliances, and components",
        level="summary",
        sector_category=SectorCategory.MANUFACTURING,
        naics_equivalent="335",
    ),
    # Manufacturing - Vehicles
    "3361MV": BEAIndustry(
        code="3361MV",
        name="Motor vehicles, bodies and trailers, and parts",
        level="summary",
        sector_category=SectorCategory.MANUFACTURING,
        naics_equivalent="3361-3363",
        notes="Auto manufacturing - critical for vehicle production tracking",
    ),
    "3364OT": BEAIndustry(
        code="3364OT",
        name="Other transportation equipment",
        level="summary",
        sector_category=SectorCategory.MANUFACTURING,
        naics_equivalent="3364-3369",
        notes="Includes aerospace, ships, railroad equipment",
    ),
    # Manufacturing - Other
    "337": BEAIndustry(
        code="337",
        name="Furniture and related products",
        level="summary",
        sector_category=SectorCategory.CONSUMER,
        naics_equivalent="337",
    ),
    "339": BEAIndustry(
        code="339",
        name="Miscellaneous manufacturing",
        level="summary",
        sector_category=SectorCategory.MANUFACTURING,
        naics_equivalent="339",
        notes="Includes medical devices",
    ),
    # Trade
    "42": BEAIndustry(
        code="42",
        name="Wholesale trade",
        level="summary",
        sector_category=SectorCategory.CONSUMER,
        naics_equivalent="42",
    ),
    "441": BEAIndustry(
        code="441",
        name="Motor vehicle and parts dealers",
        level="summary",
        sector_category=SectorCategory.CONSUMER,
        naics_equivalent="441",
    ),
    "445": BEAIndustry(
        code="445",
        name="Food and beverage stores",
        level="summary",
        sector_category=SectorCategory.CONSUMER,
        naics_equivalent="445",
    ),
    "452": BEAIndustry(
        code="452",
        name="General merchandise stores",
        level="summary",
        sector_category=SectorCategory.CONSUMER,
        naics_equivalent="452",
    ),
    "4A0": BEAIndustry(
        code="4A0",
        name="Other retail",
        level="summary",
        sector_category=SectorCategory.CONSUMER,
        naics_equivalent="442-454 (excl. 441,445,452)",
    ),
    # Transportation
    "481": BEAIndustry(
        code="481",
        name="Air transportation",
        level="summary",
        sector_category=SectorCategory.TRANSPORTATION,
        naics_equivalent="481",
    ),
    "482": BEAIndustry(
        code="482",
        name="Rail transportation",
        level="summary",
        sector_category=SectorCategory.TRANSPORTATION,
        naics_equivalent="482",
    ),
    "483": BEAIndustry(
        code="483",
        name="Water transportation",
        level="summary",
        sector_category=SectorCategory.TRANSPORTATION,
        naics_equivalent="483",
        notes="Shipping - critical for trade flow analysis",
    ),
    "484": BEAIndustry(
        code="484",
        name="Truck transportation",
        level="summary",
        sector_category=SectorCategory.TRANSPORTATION,
        naics_equivalent="484",
    ),
    "485": BEAIndustry(
        code="485",
        name="Transit and ground passenger transportation",
        level="summary",
        sector_category=SectorCategory.TRANSPORTATION,
        naics_equivalent="485",
    ),
    "486": BEAIndustry(
        code="486",
        name="Pipeline transportation",
        level="summary",
        sector_category=SectorCategory.ENERGY,
        naics_equivalent="486",
        notes="Oil/gas pipelines",
    ),
    "487OS": BEAIndustry(
        code="487OS",
        name="Other transportation and support activities",
        level="summary",
        sector_category=SectorCategory.TRANSPORTATION,
        naics_equivalent="487-488",
    ),
    "493": BEAIndustry(
        code="493",
        name="Warehousing and storage",
        level="summary",
        sector_category=SectorCategory.TRANSPORTATION,
        naics_equivalent="493",
    ),
    # Information & Technology
    "511": BEAIndustry(
        code="511",
        name="Publishing industries, except internet",
        level="summary",
        sector_category=SectorCategory.TECHNOLOGY,
        naics_equivalent="511",
    ),
    "512": BEAIndustry(
        code="512",
        name="Motion picture and sound recording industries",
        level="summary",
        sector_category=SectorCategory.CONSUMER,
        naics_equivalent="512",
    ),
    "513": BEAIndustry(
        code="513",
        name="Broadcasting and telecommunications",
        level="summary",
        sector_category=SectorCategory.TECHNOLOGY,
        naics_equivalent="513",
    ),
    "514": BEAIndustry(
        code="514",
        name="Data processing, internet publishing, and other information services",
        level="summary",
        sector_category=SectorCategory.TECHNOLOGY,
        naics_equivalent="514",
    ),
    # Finance & Insurance
    "521CI": BEAIndustry(
        code="521CI",
        name="Federal Reserve banks, credit intermediation, and related activities",
        level="summary",
        sector_category=None,  # Finance - not mapped to our sectors
        naics_equivalent="521-522",
    ),
    "523": BEAIndustry(
        code="523",
        name="Securities, commodity contracts, and investments",
        level="summary",
        sector_category=None,
        naics_equivalent="523",
    ),
    "524": BEAIndustry(
        code="524",
        name="Insurance carriers and related activities",
        level="summary",
        sector_category=None,
        naics_equivalent="524",
    ),
    "525": BEAIndustry(
        code="525",
        name="Funds, trusts, and other financial vehicles",
        level="summary",
        sector_category=None,
        naics_equivalent="525",
    ),
    # Real Estate
    "HS": BEAIndustry(
        code="HS",
        name="Housing",
        level="summary",
        sector_category=SectorCategory.HOUSING,
        naics_equivalent="N/A",
        notes="Owner-occupied housing (imputed)",
    ),
    "ORE": BEAIndustry(
        code="ORE",
        name="Other real estate",
        level="summary",
        sector_category=SectorCategory.HOUSING,
        naics_equivalent="531 (partial)",
    ),
    "532RL": BEAIndustry(
        code="532RL",
        name="Rental and leasing services and lessors of intangible assets",
        level="summary",
        sector_category=None,
        naics_equivalent="532-533",
    ),
    # Professional Services
    "5411": BEAIndustry(
        code="5411",
        name="Legal services",
        level="summary",
        sector_category=None,
        naics_equivalent="5411",
    ),
    "5415": BEAIndustry(
        code="5415",
        name="Computer systems design and related services",
        level="summary",
        sector_category=SectorCategory.TECHNOLOGY,
        naics_equivalent="5415",
    ),
    "5412OP": BEAIndustry(
        code="5412OP",
        name="Miscellaneous professional, scientific, and technical services",
        level="summary",
        sector_category=None,
        naics_equivalent="5412-5414, 5416-5419",
    ),
    "55": BEAIndustry(
        code="55",
        name="Management of companies and enterprises",
        level="summary",
        sector_category=None,
        naics_equivalent="55",
    ),
    # Administrative & Support
    "561": BEAIndustry(
        code="561",
        name="Administrative and support services",
        level="summary",
        sector_category=None,
        naics_equivalent="561",
    ),
    "562": BEAIndustry(
        code="562",
        name="Waste management and remediation services",
        level="summary",
        sector_category=None,
        naics_equivalent="562",
    ),
    # Education
    "61": BEAIndustry(
        code="61",
        name="Educational services",
        level="summary",
        sector_category=None,
        naics_equivalent="61",
    ),
    # Healthcare
    "621": BEAIndustry(
        code="621",
        name="Ambulatory health care services",
        level="summary",
        sector_category=SectorCategory.HEALTHCARE,
        naics_equivalent="621",
    ),
    "622": BEAIndustry(
        code="622",
        name="Hospitals",
        level="summary",
        sector_category=SectorCategory.HEALTHCARE,
        naics_equivalent="622",
    ),
    "623": BEAIndustry(
        code="623",
        name="Nursing and residential care facilities",
        level="summary",
        sector_category=SectorCategory.HEALTHCARE,
        naics_equivalent="623",
    ),
    "624": BEAIndustry(
        code="624",
        name="Social assistance",
        level="summary",
        sector_category=SectorCategory.HEALTHCARE,
        naics_equivalent="624",
    ),
    # Arts & Recreation
    "711AS": BEAIndustry(
        code="711AS",
        name="Performing arts, spectator sports, museums, and related activities",
        level="summary",
        sector_category=SectorCategory.CONSUMER,
        naics_equivalent="711-712",
    ),
    "713": BEAIndustry(
        code="713",
        name="Amusements, gambling, and recreation industries",
        level="summary",
        sector_category=SectorCategory.CONSUMER,
        naics_equivalent="713",
    ),
    # Accommodation & Food
    "721": BEAIndustry(
        code="721",
        name="Accommodation",
        level="summary",
        sector_category=SectorCategory.CONSUMER,
        naics_equivalent="721",
    ),
    "722": BEAIndustry(
        code="722",
        name="Food services and drinking places",
        level="summary",
        sector_category=SectorCategory.CONSUMER,
        naics_equivalent="722",
    ),
    # Other Services
    "81": BEAIndustry(
        code="81",
        name="Other services, except government",
        level="summary",
        sector_category=SectorCategory.CONSUMER,
        naics_equivalent="81",
    ),
    # Government
    "GFG": BEAIndustry(
        code="GFG",
        name="Federal general government",
        level="summary",
        sector_category=None,
        naics_equivalent="N/A",
    ),
    "GFGD": BEAIndustry(
        code="GFGD",
        name="Federal government defense",
        level="summary",
        sector_category=None,
        naics_equivalent="N/A",
    ),
    "GFGN": BEAIndustry(
        code="GFGN",
        name="Federal government nondefense",
        level="summary",
        sector_category=None,
        naics_equivalent="N/A",
    ),
    "GFE": BEAIndustry(
        code="GFE",
        name="Federal government enterprises",
        level="summary",
        sector_category=None,
        naics_equivalent="N/A",
    ),
    "GSLG": BEAIndustry(
        code="GSLG",
        name="State and local general government",
        level="summary",
        sector_category=None,
        naics_equivalent="N/A",
    ),
    "GSLE": BEAIndustry(
        code="GSLE",
        name="State and local government enterprises",
        level="summary",
        sector_category=None,
        naics_equivalent="N/A",
    ),
}


# =============================================================================
# MAPPING FUNCTIONS
# =============================================================================


def get_bea_industry(code: str, level: str = "summary") -> BEAIndustry | None:
    """Get BEA industry definition by code.

    Args:
        code: BEA industry code
        level: Detail level ('sector' or 'summary')

    Returns:
        BEAIndustry if found, None otherwise
    """
    if level == "sector":
        return BEA_SECTOR_INDUSTRIES.get(code)
    return BEA_SUMMARY_INDUSTRIES.get(code)


def map_bea_to_sector(bea_code: str) -> SectorCategory | None:
    """Map a BEA industry code to an internal sector category.

    Args:
        bea_code: BEA industry code (sector or summary level)

    Returns:
        SectorCategory if mappable, None otherwise
    """
    # Try summary level first (more specific)
    industry = BEA_SUMMARY_INDUSTRIES.get(bea_code)
    if industry:
        return industry.sector_category

    # Fall back to sector level
    industry = BEA_SECTOR_INDUSTRIES.get(bea_code)
    if industry:
        return industry.sector_category

    return None


def get_industries_by_sector(
    sector: SectorCategory,
    level: str = "summary"
) -> list[BEAIndustry]:
    """Get all BEA industries that map to a sector category.

    Args:
        sector: Internal sector category
        level: Detail level ('sector' or 'summary')

    Returns:
        List of BEAIndustry objects for that sector
    """
    source = BEA_SECTOR_INDUSTRIES if level == "sector" else BEA_SUMMARY_INDUSTRIES
    return [
        industry
        for industry in source.values()
        if industry.sector_category == sector
    ]


def get_all_industries(level: str = "summary") -> dict[str, BEAIndustry]:
    """Get all BEA industries at a given detail level.

    Args:
        level: Detail level ('sector' or 'summary')

    Returns:
        Dictionary of code -> BEAIndustry
    """
    if level == "sector":
        return BEA_SECTOR_INDUSTRIES.copy()
    return BEA_SUMMARY_INDUSTRIES.copy()


# =============================================================================
# CRITICAL INDUSTRIES FOR BOTTLENECK ANALYSIS
# Industries to watch closely for supply chain disruptions
# =============================================================================

CRITICAL_INDUSTRIES: list[str] = [
    "211",      # Oil and gas extraction
    "22",       # Utilities
    "324",      # Petroleum and coal products
    "325",      # Chemical products (includes pharma)
    "331",      # Primary metals (steel, aluminum)
    "334",      # Computer and electronic products (semiconductors)
    "3361MV",   # Motor vehicles
    "3364OT",   # Other transportation equipment (aerospace)
    "483",      # Water transportation (shipping)
    "484",      # Truck transportation
    "486",      # Pipeline transportation
]


def is_critical_industry(bea_code: str) -> bool:
    """Check if an industry is considered critical for bottleneck analysis."""
    return bea_code in CRITICAL_INDUSTRIES
