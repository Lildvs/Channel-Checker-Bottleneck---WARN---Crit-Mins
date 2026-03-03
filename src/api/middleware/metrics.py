"""Prometheus metrics integration for FastAPI.

Provides request instrumentation and custom metrics for monitoring
collector health, bottleneck detection, and API performance.
"""

from typing import Callable

import structlog
from fastapi import FastAPI, Request
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
from starlette.routing import Match

logger = structlog.get_logger()

COLLECTOR_RUNS_TOTAL = Counter(
    "channelcheck_collector_runs_total",
    "Total number of collector runs",
    ["collector_name", "status"],  # status: success, error, partial
)

COLLECTOR_RECORDS_COLLECTED = Counter(
    "channelcheck_collector_records_total",
    "Total records collected",
    ["collector_name"],
)

COLLECTOR_LAST_RUN_TIMESTAMP = Gauge(
    "channelcheck_collector_last_run_timestamp_seconds",
    "Unix timestamp of last collector run",
    ["collector_name"],
)

COLLECTOR_DURATION_SECONDS = Histogram(
    "channelcheck_collector_duration_seconds",
    "Collector run duration in seconds",
    ["collector_name"],
    buckets=(1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0),
)

ACTIVE_BOTTLENECKS = Gauge(
    "channelcheck_active_bottlenecks",
    "Number of active bottlenecks",
    ["category"],
)

BOTTLENECK_SEVERITY = Gauge(
    "channelcheck_bottleneck_severity",
    "Current severity of active bottlenecks (0-1)",
    ["category", "series_id"],
)

BOTTLENECK_DETECTIONS_TOTAL = Counter(
    "channelcheck_bottleneck_detections_total",
    "Total bottleneck detections",
    ["category"],
)

API_REQUESTS_TOTAL = Counter(
    "channelcheck_api_requests_total",
    "Total API requests",
    ["method", "endpoint", "status_code"],
)

API_REQUEST_DURATION_SECONDS = Histogram(
    "channelcheck_api_request_duration_seconds",
    "API request duration in seconds",
    ["method", "endpoint"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

API_ERRORS_TOTAL = Counter(
    "channelcheck_api_errors_total",
    "Total API errors",
    ["method", "endpoint", "error_type"],
)

RATE_LIMIT_HITS_TOTAL = Counter(
    "channelcheck_rate_limit_hits_total",
    "Total rate limit hits",
    ["client_type"],  # ip, api_key
)

DATA_LAST_UPDATE_TIMESTAMP = Gauge(
    "channelcheck_data_last_update_timestamp_seconds",
    "Unix timestamp of last data update",
    ["series_id"],
)


async def metrics_endpoint(_request: Request) -> Response:
    """Prometheus metrics endpoint handler.
    
    Returns metrics in Prometheus text format.
    """
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


def get_path_template(request: Request) -> str:
    """Extract path template from request for metrics labeling.
    
    Converts /api/bottlenecks/123 to /api/bottlenecks/{id}
    to avoid high-cardinality metrics.
    """
    app = request.app
    for route in app.routes:
        match, _ = route.matches(request.scope)
        if match == Match.FULL:
            return route.path
    
    return request.url.path


async def request_metrics_middleware(request: Request, call_next: Callable) -> Response:
    """Middleware to track request metrics.
    
    Measures request duration and counts by status code.
    """
    import time
    
    # Skip metrics endpoint to avoid recursion
    if request.url.path == "/metrics":
        return await call_next(request)
    
    method = request.method
    path_template = get_path_template(request)
    
    start_time = time.perf_counter()
    
    try:
        response = await call_next(request)
        status_code = response.status_code
    except Exception as e:
        API_ERRORS_TOTAL.labels(
            method=method,
            endpoint=path_template,
            error_type=type(e).__name__,
        ).inc()
        raise
    finally:
        duration = time.perf_counter() - start_time
        API_REQUEST_DURATION_SECONDS.labels(
            method=method,
            endpoint=path_template,
        ).observe(duration)
    
    API_REQUESTS_TOTAL.labels(
        method=method,
        endpoint=path_template,
        status_code=str(status_code),
    ).inc()
    
    return response


def setup_metrics(app: FastAPI) -> None:
    """Configure Prometheus metrics for the FastAPI application.
    
    Args:
        app: FastAPI application instance
    """
    app.add_api_route(
        "/metrics",
        metrics_endpoint,
        methods=["GET"],
        tags=["Monitoring"],
        include_in_schema=False,  # Hide from OpenAPI docs
    )
    
    app.middleware("http")(request_metrics_middleware)
    
    logger.info("Prometheus metrics configured", endpoint="/metrics")


def record_collector_run(
    collector_name: str,
    status: str,
    records_collected: int,
    duration_seconds: float,
) -> None:
    """Record metrics for a collector run.
    
    Args:
        collector_name: Name of the collector
        status: Run status (success, error, partial)
        records_collected: Number of records collected
        duration_seconds: Run duration in seconds
    """
    import time
    
    COLLECTOR_RUNS_TOTAL.labels(collector_name=collector_name, status=status).inc()
    COLLECTOR_RECORDS_COLLECTED.labels(collector_name=collector_name).inc(records_collected)
    COLLECTOR_LAST_RUN_TIMESTAMP.labels(collector_name=collector_name).set(time.time())
    COLLECTOR_DURATION_SECONDS.labels(collector_name=collector_name).observe(duration_seconds)


def update_active_bottlenecks(category: str, count: int) -> None:
    """Update the active bottleneck count for a category.
    
    Args:
        category: Bottleneck category
        count: Current count of active bottlenecks
    """
    ACTIVE_BOTTLENECKS.labels(category=category).set(count)


def record_bottleneck_detection(category: str) -> None:
    """Record a bottleneck detection event.
    
    Args:
        category: Bottleneck category
    """
    BOTTLENECK_DETECTIONS_TOTAL.labels(category=category).inc()


def update_bottleneck_severity(category: str, series_id: str, severity: float) -> None:
    """Update bottleneck severity gauge.
    
    Args:
        category: Bottleneck category
        series_id: Series that triggered the bottleneck
        severity: Severity score (0-1)
    """
    BOTTLENECK_SEVERITY.labels(category=category, series_id=series_id).set(severity)


def record_rate_limit_hit(client_type: str = "ip") -> None:
    """Record a rate limit hit.
    
    Args:
        client_type: Type of client identifier used
    """
    RATE_LIMIT_HITS_TOTAL.labels(client_type=client_type).inc()


def update_data_freshness(series_id: str, timestamp_seconds: float) -> None:
    """Update data freshness timestamp for a series.
    
    Args:
        series_id: Data series identifier
        timestamp_seconds: Unix timestamp of last update
    """
    DATA_LAST_UPDATE_TIMESTAMP.labels(series_id=series_id).set(timestamp_seconds)
