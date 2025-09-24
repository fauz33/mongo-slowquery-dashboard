"""Runtime status helpers for the v2 analyzer."""

from __future__ import annotations

import copy
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, Optional


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


_LOCK = Lock()
_STATE: Dict[str, Any] = {
    "heavy_indexes_ready": True,
    "fts_ready": True,
    "fts_rebuild_in_progress": False,
    "cache_warmup_ready": True,
    "cache_warmup_in_progress": False,
    "cache_warmup_total": 0,
    "cache_warmup_completed": 0,
    "cache_warmup_error": None,
    "current_ingest": None,
    "last_ingest": None,
    "last_upload": None,
    "recent_ingests": [],
}


def ingest_upload_prepared(path: Any, metrics: Dict[str, Any]) -> None:
    """Record metrics gathered before ingest kicks off (upload/save stage)."""

    with _LOCK:
        _STATE["last_upload"] = {
            "file": str(path),
            "prepared_at": _now_iso(),
            "metrics": dict(metrics),
        }


def ingest_started(path: Any, *, upload_metrics: Optional[Dict[str, Any]] = None) -> None:
    """Mark the beginning of an ingest run for *path*."""

    now = _now_iso()
    with _LOCK:
        _STATE["heavy_indexes_ready"] = False
        _STATE["current_ingest"] = {
            "file": str(path),
            "phase": "starting",
            "started_at": now,
            "updated_at": now,
            "metrics": {},
        }
        if upload_metrics:
            _STATE["current_ingest"]["upload"] = dict(upload_metrics)


def ingest_phase(
    path: Any,
    phase: str,
    *,
    detail: Optional[str] = None,
    metrics: Optional[Dict[str, Any]] = None,
) -> None:
    """Update the active ingest with a new *phase* and optional metrics."""

    now = _now_iso()
    with _LOCK:
        current = _STATE.get("current_ingest")
        if current is None or current.get("file") != str(path):
            current = {
                "file": str(path),
                "started_at": now,
                "metrics": {},
            }
            _STATE["current_ingest"] = current
        current["phase"] = phase
        current["updated_at"] = now
        if detail is not None:
            current["detail"] = detail
        if metrics:
            stored = current.setdefault("metrics", {})
            stored.update(metrics)


def ingest_finished(
    path: Any,
    *,
    success: bool,
    duration_seconds: float,
    row_counts: Dict[str, int],
    timings: Dict[str, float],
    telemetry: Optional[Dict[str, Any]] = None,
) -> None:
    """Record completion of the current ingest."""

    now = _now_iso()
    summary = {
        "file": str(path),
        "completed_at": now,
        "success": success,
        "duration_seconds": duration_seconds,
        "row_counts": dict(row_counts),
        "timings": dict(timings),
    }
    if telemetry is not None:
        summary["artifacts"] = {
            key: value.get("path") if isinstance(value, dict) else value
            for key, value in telemetry.items()
            if isinstance(value, dict) and "path" in value
        }
    with _LOCK:
        _STATE["last_ingest"] = summary
        _STATE["current_ingest"] = None
        _STATE["heavy_indexes_ready"] = True
        _STATE["fts_ready"] = True
        _STATE["fts_rebuild_in_progress"] = False
        _STATE["cache_warmup_ready"] = True
        _STATE["cache_warmup_in_progress"] = False
        recent = _STATE.setdefault("recent_ingests", [])
        recent.append(summary)
        if len(recent) > 10:
            del recent[:-10]


def ingest_failed(path: Any, error: str, *, duration_seconds: Optional[float] = None) -> None:
    """Capture failure details and reset status flags."""

    now = _now_iso()
    summary = {
        "file": str(path),
        "completed_at": now,
        "success": False,
        "error": error,
    }
    if duration_seconds is not None:
        summary["duration_seconds"] = duration_seconds
    with _LOCK:
        _STATE["last_ingest"] = summary
        _STATE["current_ingest"] = None
        _STATE["heavy_indexes_ready"] = True
        _STATE["fts_ready"] = True
        _STATE["fts_rebuild_in_progress"] = False
        _STATE["cache_warmup_ready"] = True
        _STATE["cache_warmup_in_progress"] = False


def get_status() -> Dict[str, Any]:
    """Return a snapshot of the current processing status."""

    with _LOCK:
        return copy.deepcopy(_STATE)
