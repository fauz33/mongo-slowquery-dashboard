"""Concurrency helpers for the v2 analyzer."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Optional


def create_thread_pool(max_workers: Optional[int] = None) -> ThreadPoolExecutor:
    """Build a thread pool with a project-specific default name prefix."""

    return ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="slowq-v2")
