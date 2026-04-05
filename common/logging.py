from __future__ import annotations

import os

import structlog


def setup_logging() -> None:
    """Configure structlog for structured JSON output.

    Set LOG_FORMAT=console for human-readable dev output.
    Defaults to JSON for production / observability.
    """
    log_format = os.getenv("LOG_FORMAT", "json")

    if log_format == "console":
        renderer = structlog.dev.ConsoleRenderer()
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso"),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(0),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
