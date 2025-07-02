"""Logging utilities with correlation IDs and success/failure counters."""

import logging
import os
import uuid
from contextvars import ContextVar
from typing import Optional

try:
    from prometheus_client import Counter, start_http_server
    _PROM_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    _PROM_AVAILABLE = False
    class _DummyCounter:
        def inc(self) -> None:
            pass
    def Counter(*a, **k):  # type: ignore
        return _DummyCounter()
    def start_http_server(*a, **k):  # type: ignore
        pass

correlation_id_var: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)

class CorrelationIdFilter(logging.Filter):
    """Inject the correlation ID into all log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, 'correlation_id'):
            record.correlation_id = '-'  # Default value when not set
        return True

operation_counts = {"success": 0, "failure": 0}

# Prometheus counters mirroring ``operation_counts``
success_counter = Counter(
    "etl_success_total",
    "Total successful ETL operations",
)
failure_counter = Counter(
    "etl_failure_total",
    "Total failed ETL operations",
)


def record_success() -> None:
    operation_counts["success"] += 1
    success_counter.inc()


def record_failure() -> None:
    operation_counts["failure"] += 1
    failure_counter.inc()


def setup_logging(level: int = logging.INFO) -> str:
    """Configure root logging and generate a correlation ID.

    Returns the generated correlation ID so callers can include it elsewhere if
    needed.
    """
    cid = uuid.uuid4().hex
    correlation_id_var.set(cid)

    root = logging.getLogger()
    root.setLevel(level)

    if not root.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s [%(correlation_id)s] %(levelname)s %(name)s: %(message)s"
        )
        handler.setFormatter(formatter)
        root.addHandler(handler)

    root.addFilter(CorrelationIdFilter())

    # Add the filter to each handler
    for handler in logging.root.handlers:
        handler.addFilter(CorrelationIdFilter())

    port = os.getenv("PROMETHEUS_PORT")
    if port:
        try:
            start_http_server(int(port))
            logging.getLogger(__name__).info("Prometheus metrics server running on port %s", port)
        except Exception as exc:  # pragma: no cover - environment may block
            logging.getLogger(__name__).error("Failed to start metrics server: %s", exc)

    return cid

