"""Configuration primitives for the revamped analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


def _env_flag(name: str, *, default: bool) -> bool:
    """Interpret common truthy/falsey environment values."""

    value = os.environ.get(name)
    if value is None:
        return default

    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


@dataclass(frozen=True)
class Settings:
    """Runtime configuration defaults for the v2 pipeline."""

    output_root: Path = Path(os.environ.get("MONGO_SLOWQ_OUT_ROOT", "out"))
    parquet_compression: str = os.environ.get("MONGO_SLOWQ_PARQUET_COMPRESSION", "snappy")
    chunk_rows: int = int(os.environ.get("MONGO_SLOWQ_CHUNK_ROWS", "50000"))
    keep_source_copy: bool = _env_flag("MONGO_SLOWQ_KEEP_SOURCE_COPY", default=True)
    enable_duckdb: bool = not _env_flag("MONGO_SLOWQ_DISABLE_DUCKDB", default=False)
    dataset_version: int = 1
    wipe_dataset_on_upload: bool = _env_flag("MONGO_SLOWQ_WIPE_ON_UPLOAD", default=True)


settings = Settings()
