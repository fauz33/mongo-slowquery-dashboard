"""Logging helpers for the revamped analyzer."""

from __future__ import annotations

import logging


def get_logger(name: str) -> logging.Logger:
    """Return a namespaced logger with sensible defaults."""

    logger = logging.getLogger(f"log_analyzer_v2.{name}")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger
