"""Timing utilities used across the v2 pipeline."""

from __future__ import annotations

import contextlib
import time
from typing import Iterator, Callable


@contextlib.contextmanager
def timed(section: str, sink: Callable[[str, float], None]) -> Iterator[None]:
    """Measure execution time of a code block and report to *sink*."""

    start = time.perf_counter()
    try:
        yield
    finally:
        duration = time.perf_counter() - start
        sink(section, duration)
