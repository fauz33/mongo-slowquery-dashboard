"""Configuration primitives for the revamped analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


@dataclass(frozen=True)
class Settings:
    """Runtime configuration defaults for the v2 pipeline."""

    output_root: Path = Path(os.environ.get("MONGO_SLOWQ_OUT_ROOT", "out"))
    parquet_compression: str = os.environ.get("MONGO_SLOWQ_PARQUET_COMPRESSION", "snappy")
    chunk_rows: int = int(os.environ.get("MONGO_SLOWQ_CHUNK_ROWS", "50000"))
    keep_source_copy: bool = os.environ.get("MONGO_SLOWQ_KEEP_SOURCE_COPY", "0") == "1"
    enable_duckdb: bool = os.environ.get("MONGO_SLOWQ_DISABLE_DUCKDB", "0") != "1"
    dataset_version: int = 1
    wipe_dataset_on_upload: bool = os.environ.get("MONGO_SLOWQ_WIPE_ON_UPLOAD", "0") == "1"


settings = Settings()
