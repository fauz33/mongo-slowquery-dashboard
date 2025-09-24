"""Utilities to search raw MongoDB log files stored in the v2 dataset."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def search_logs(
    *,
    dataset_root: Path,
    text: str,
    limit: int = 100,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
    regex: Optional[str] = None,
    case_sensitive: bool = False,
) -> List[Dict[str, Any]]:
    """Perform a simple substring (or regex) search across stored log files.

    This is a best-effort implementation intended for small to medium datasets.
    """

    if not text and not regex:
        return []

    file_map_path = dataset_root / "index" / "file_map.json"
    if not file_map_path.exists():
        return []

    try:
        file_map = json.loads(file_map_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []

    patterns = []
    if regex:
        import re

        flags = 0 if case_sensitive else re.IGNORECASE
        try:
            compiled = re.compile(regex, flags)
        except re.error:
            compiled = None
        if compiled:
            patterns.append(("regex", compiled))
    else:
        needle = text if case_sensitive else text.lower()
        patterns.append(("substr", needle))

    results: List[Dict[str, Any]] = []

    for path_str in file_map.values():
        path = Path(path_str)
        if not path.exists():
            continue

        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                line_stripped = line.strip()
                if not line_stripped or not line_stripped.startswith("{"):
                    continue

                if not _matches(line_stripped, patterns, case_sensitive):
                    continue

                try:
                    entry = json.loads(line_stripped)
                except json.JSONDecodeError:
                    continue

                ts_str = _extract_timestamp(entry)
                ts_epoch = _parse_epoch(ts_str) if ts_str else None

                if start_ts is not None and ts_epoch is not None and ts_epoch < start_ts:
                    continue
                if end_ts is not None and ts_epoch is not None and ts_epoch > end_ts:
                    continue

                results.append(
                    {
                        "timestamp": ts_str,
                        "ts_epoch": ts_epoch,
                        "msg": entry.get("msg"),
                        "attr": entry.get("attr"),
                        "context": entry.get("ctx"),
                        "level": entry.get("s"),
                        "component": entry.get("c"),
                        "raw": line_stripped,
                    }
                )

                if len(results) >= limit:
                    return results

    return results


def _matches(line: str, patterns: Iterable[tuple[str, Any]], case_sensitive: bool) -> bool:
    for kind, value in patterns:
        if kind == "regex":
            if value.search(line):  # type: ignore[arg-type]
                return True
        elif kind == "substr":
            haystack = line if case_sensitive else line.lower()
            if value in haystack:  # type: ignore[operator]
                return True
    return False


def _extract_timestamp(entry: Dict[str, Any]) -> Optional[str]:
    t = entry.get("t")
    if isinstance(t, dict):
        date = t.get("$date")
        if isinstance(date, str):
            return date
    return None


def _parse_epoch(timestamp: Optional[str]) -> Optional[int]:
    if not timestamp:
        return None
    try:
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except ValueError:
        return None
