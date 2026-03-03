"""Prompt templates for LLM-powered bottleneck forecasting.

This module contains structured prompts for different forecasting tasks.
"""

DURATION_FORECAST_PROMPT = """You are an expert economic analyst forecasting how long an economic bottleneck will persist.

## Bottleneck Details
- **Category**: {category}
- **Severity**: {severity:.0%}
- **Confidence**: {confidence:.0%}
- **Affected Sectors**: {sectors}
- **Description**: {description}
- **Detected At**: {detected_at}
- **Source Data**: {source_series}

## Research Context
{research_context}

## Historical Precedents
{precedents}

## Task
Analyze this bottleneck and forecast how many days it will persist before resolution (severity drops below 20%).

Consider:
1. Historical patterns for {category} bottlenecks
2. Current severity level and trajectory
3. Affected sectors and their typical recovery patterns
4. Research context and current market conditions
5. Potential resolution mechanisms and policy responses

## Required Output Format
Provide your forecast in EXACTLY this format:

EXPECTED_DURATION_DAYS: [number]
PERCENTILE_25: [number]
PERCENTILE_75: [number]
PROBABILITY_PERSISTS_30_DAYS: [decimal 0-1]
PROBABILITY_PERSISTS_60_DAYS: [decimal 0-1]
PROBABILITY_PERSISTS_90_DAYS: [decimal 0-1]

REASONING:
[Your detailed analysis explaining the forecast, 2-4 paragraphs]
"""

SEVERITY_TRAJECTORY_PROMPT = """You are an expert economic analyst forecasting how the severity of an economic bottleneck will evolve over time.

## Bottleneck Details
- **Category**: {category}
- **Current Severity**: {severity:.0%}
- **Confidence**: {confidence:.0%}
- **Affected Sectors**: {sectors}
- **Description**: {description}
- **Age**: {age_days} days since detection

## Research Context
{research_context}

## Task
Forecast how the bottleneck severity will evolve over the next {horizon_days} days.

Consider:
1. Natural decay patterns for this type of bottleneck
2. Potential for severity to worsen before improving
3. External factors that could accelerate or delay resolution
4. Seasonal patterns and economic cycles

## Required Output Format
Provide severity forecasts for each week in this format:

DAY_7_SEVERITY: [decimal 0-1]
DAY_14_SEVERITY: [decimal 0-1]
DAY_21_SEVERITY: [decimal 0-1]
DAY_30_SEVERITY: [decimal 0-1]
EXPECTED_RESOLUTION_DAY: [number or "beyond_horizon"]
TRAJECTORY_TYPE: [improving | stable | worsening | volatile]

REASONING:
[Your analysis of the expected trajectory, 1-2 paragraphs]
"""

RESEARCH_SUMMARY_PROMPT = """You are an expert economic researcher analyzing a bottleneck.

## Bottleneck Details
- **Category**: {category}
- **Severity**: {severity:.0%}
- **Affected Sectors**: {sectors}
- **Description**: {description}

## Research Papers and Sources
{research_papers}

## Task
Synthesize the research into actionable insights for forecasting this bottleneck.

Provide:
1. **Key Findings**: 3-5 most relevant insights from the research
2. **Market Implications**: How this affects the affected sectors
3. **Resolution Indicators**: What signals would indicate resolution
4. **Risk Factors**: What could make this bottleneck worse

## Required Output Format

KEY_FINDINGS:
- [Finding 1]
- [Finding 2]
- [Finding 3]

MARKET_IMPLICATIONS:
[1-2 paragraphs on sector impacts]

RESOLUTION_INDICATORS:
- [Indicator 1]
- [Indicator 2]
- [Indicator 3]

RISK_FACTORS:
- [Risk 1]
- [Risk 2]
"""

BINARY_PERSISTENCE_PROMPT = """You are an expert forecaster predicting whether an economic bottleneck will persist beyond a threshold.

## Question
Will this {category} bottleneck persist beyond {threshold_days} days?

## Bottleneck Details
- **Current Severity**: {severity:.0%}
- **Affected Sectors**: {sectors}
- **Description**: {description}
- **Current Age**: {age_days} days

## Research Context
{research_context}

## Historical Base Rates
{base_rates}

## Task
Estimate the probability that this bottleneck will still be active (severity > 20%) after {threshold_days} days.

## Required Output Format

PROBABILITY: [decimal 0-1]

REASONING:
[Your analysis, considering historical patterns, current conditions, and research context. 1-2 paragraphs]

CONFIDENCE: [low | medium | high]
"""

KEY_FACTORS_PROMPT = """Identify the key factors driving this economic bottleneck and their likely trajectory.

## Bottleneck Details
- **Category**: {category}
- **Severity**: {severity:.0%}
- **Affected Sectors**: {sectors}
- **Description**: {description}

## Research Context
{research_context}

## Task
Identify 3-5 key factors that will determine how this bottleneck evolves.

For each factor, provide:
1. Description of the factor
2. Current state (improving, stable, worsening)
3. Impact on bottleneck duration (high, medium, low)
4. Key metrics to monitor

## Required Output Format

FACTOR_1:
- Description: [text]
- Current State: [improving | stable | worsening]
- Impact: [high | medium | low]
- Key Metrics: [list of metrics]

FACTOR_2:
...

OVERALL_OUTLOOK: [positive | neutral | negative]
"""


def format_duration_prompt(
    category: str,
    severity: float,
    confidence: float,
    sectors: list[str],
    description: str,
    detected_at: str,
    source_series: list[str],
    research_context: str,
    precedents: str,
) -> str:
    """Format the duration forecast prompt with bottleneck data.

    Args:
        category: Bottleneck category
        severity: Severity score (0-1)
        confidence: Confidence score (0-1)
        sectors: List of affected sector names
        description: Bottleneck description
        detected_at: Detection timestamp
        source_series: List of source data series IDs
        research_context: Formatted research context
        precedents: Formatted historical precedents

    Returns:
        Formatted prompt string
    """
    return DURATION_FORECAST_PROMPT.format(
        category=category.replace("_", " ").title(),
        severity=severity,
        confidence=confidence,
        sectors=", ".join(sectors) if sectors else "Multiple sectors",
        description=description,
        detected_at=detected_at,
        source_series=", ".join(source_series) if source_series else "N/A",
        research_context=research_context or "No research context available.",
        precedents=precedents or "No historical precedents available.",
    )


def format_trajectory_prompt(
    category: str,
    severity: float,
    confidence: float,
    sectors: list[str],
    description: str,
    age_days: int,
    horizon_days: int,
    research_context: str,
) -> str:
    """Format the severity trajectory prompt.

    Args:
        category: Bottleneck category
        severity: Current severity (0-1)
        confidence: Confidence score (0-1)
        sectors: Affected sectors
        description: Bottleneck description
        age_days: Days since detection
        horizon_days: Forecast horizon
        research_context: Research context

    Returns:
        Formatted prompt string
    """
    return SEVERITY_TRAJECTORY_PROMPT.format(
        category=category.replace("_", " ").title(),
        severity=severity,
        confidence=confidence,
        sectors=", ".join(sectors) if sectors else "Multiple sectors",
        description=description,
        age_days=age_days,
        horizon_days=horizon_days,
        research_context=research_context or "No research context available.",
    )


def format_binary_prompt(
    category: str,
    severity: float,
    sectors: list[str],
    description: str,
    age_days: int,
    threshold_days: int,
    research_context: str,
    base_rates: str,
) -> str:
    """Format the binary persistence prompt.

    Args:
        category: Bottleneck category
        severity: Current severity
        sectors: Affected sectors
        description: Description
        age_days: Current age
        threshold_days: Days to forecast
        research_context: Research context
        base_rates: Historical base rates

    Returns:
        Formatted prompt
    """
    return BINARY_PERSISTENCE_PROMPT.format(
        category=category.replace("_", " ").title(),
        severity=severity,
        sectors=", ".join(sectors) if sectors else "Multiple sectors",
        description=description,
        age_days=age_days,
        threshold_days=threshold_days,
        research_context=research_context or "No research context available.",
        base_rates=base_rates or "No historical base rates available.",
    )
