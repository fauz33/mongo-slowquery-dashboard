"""Manifest helpers for the v2 dataset."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from ..utils.logging_utils import get_logger

LOGGER = get_logger("storage.manifest")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_manifest(path: Path) -> Dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except json.JSONDecodeError:
        LOGGER.warning("Manifest at %s is corrupt; starting fresh", path)
        return None


def append_manifest_entry(
    path: Path,
    *,
    dataset_version: int,
    source_file: Path,
    file_id: int,
    row_counts: Dict[str, int],
    artifacts: Dict[str, str],
) -> Dict[str, Any]:
    manifest = load_manifest(path)
    now = _now()

    if manifest is None:
        manifest = {
            "dataset_version": dataset_version,
            "created_at": now,
            "ingests": [],
        }

    manifest["dataset_version"] = dataset_version
    manifest["updated_at"] = now

    ingest_entry = {
        "ingest_id": len(manifest["ingests"])
        + 1,
        "created_at": now,
        "source_file": str(source_file),
        "file_id": file_id,
        "row_counts": row_counts,
        "artifacts": artifacts,
    }
    manifest["ingests"].append(ingest_entry)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)

    LOGGER.info(
        "Updated manifest at %s with ingest #%d", path, ingest_entry["ingest_id"]
    )
    return {"path": str(path), "ingest_id": ingest_entry["ingest_id"]}
