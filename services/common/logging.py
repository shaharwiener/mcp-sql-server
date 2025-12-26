import logging
import sys
import structlog
from typing import Optional

def configure_logging(log_level: str = "INFO", json_format: bool = True):
    """
    Configure global logging for the application.
    
    Args:
        log_level: "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"
        json_format: If True, outputs JSON (best for Prod/K8s). If False, outputs pretty text (Local).
    """
    
    # 1. Standard Lib Configuration
    # We want standard logging (e.g. from libraries) to go through our handler
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper(), logging.INFO),
    )

    # 2. Processor Chain
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
    ]

    # 3. Formatter Selection
    if json_format:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    # 4. Apply Configuration
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, log_level.upper())),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
