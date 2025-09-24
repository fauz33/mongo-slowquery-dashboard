"""Helper to persist file_id to source path mapping."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

from ..utils.logging_utils import get_logger

LOGGER = get_logger("storage.file_map")


def load_file_map(path: Path) -> Dict[int, str]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except json.JSONDecodeError:
        LOGGER.warning("File map at %s is corrupt; starting fresh", path)
        return {}

    mapping: Dict[int, str] = {}
    for key, value in raw.items():
        try:
            mapping[int(key)] = value
        except ValueError:
            LOGGER.debug("Ignoring non-integer file_id %s in file map", key)
    return mapping


def next_file_id(path: Path) -> int:
    mapping = load_file_map(path)
    return max(mapping.keys(), default=0) + 1


def write_file_map(path: Path, mapping: Dict[int, str]) -> Dict[str, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump({str(k): v for k, v in mapping.items()}, handle, indent=2)
    LOGGER.info("Wrote file map to %s", path)
    return {"path": str(path), "entries": len(mapping)}


def update_file_map(path: Path, file_id: int, source_path: str) -> Dict[str, str]:
    mapping = load_file_map(path)
    mapping[file_id] = source_path
    return write_file_map(path, mapping)
