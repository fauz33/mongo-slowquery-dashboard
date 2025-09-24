"""High-level ingest orchestration for the v2 analyzer."""

from __future__ import annotations

import time
from pathlib import Path
import shutil
from typing import Any, Dict, Optional

from ..config import settings
from ..utils.logging_utils import get_logger
from .parser import ParsedBatch, parse_log_file
from .parquet_writer import (
    AuthenticationBatchWriter,
    ConnectionBatchWriter,
    SlowQueryBatchWriter,
)
from ..storage.offsets_index import QueryOffsetBatchWriter
from ..storage.manifest import append_manifest_entry
from ..storage.file_map import next_file_id, load_file_map, update_file_map
from ..runtime import status as status_tracker

LOGGER = get_logger("ingest.pipeline")


def ingest_log_file(
    input_path: Path,
    *,
    output_root: Path | None = None,
    file_id: int | None = None,
    compression: str | None = None,
    upload_metrics: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Parse a MongoDB log file and persist available events to Parquet."""

    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(path)

    root = Path(output_root) if output_root is not None else settings.output_root
    codec = compression or settings.parquet_compression

    LOGGER.info("Starting ingest for %s (codec=%s)", path, codec)
    status_tracker.ingest_started(path, upload_metrics=upload_metrics)

    overall_start = time.perf_counter()
    parse_seconds = 0.0
    write_seconds = 0.0
    finalize_seconds = 0.0
    batches = 0
    totals = {"slow_queries": 0, "authentications": 0, "connections": 0}

    file_map_path = root / "index" / "file_map.json"
    existing_map = load_file_map(file_map_path)

    resolved_input = path.resolve()
    source_abs = str(resolved_input)

    if file_id is None:
        for existing_id, existing_path in existing_map.items():
            if existing_path == source_abs:
                file_id = existing_id
                LOGGER.info(
                    "Reusing existing file_id %s for %s", file_id, source_abs
                )
                break
        else:
            file_id = next_file_id(file_map_path)
            LOGGER.info("Allocated file_id %s for %s", file_id, source_abs)

    file_prefix = f"{file_id:04d}_{path.stem}"

    slow_target = root / "slow_queries" / f"{file_prefix}.parquet"
    auth_target = root / "authentications" / f"{file_prefix}.parquet"
    conn_target = root / "connections" / f"{file_prefix}.parquet"

    slow_writer = SlowQueryBatchWriter(
        slow_target, file_id=file_id, compression=codec
    )
    auth_writer = AuthenticationBatchWriter(
        auth_target, file_id=file_id, compression=codec
    )
    conn_writer = ConnectionBatchWriter(
        conn_target, file_id=file_id, compression=codec
    )
    offset_target = root / "index" / f"{file_prefix}_query_offsets.parquet"
    offset_writer = QueryOffsetBatchWriter(
        offset_target, file_id=file_id, compression=codec
    )

    iterator = parse_log_file(path)
    try:
        status_tracker.ingest_phase(
            path,
            "streaming",
            detail="parsing and writing batches",
            metrics={"batches": 0},
        )
        while True:
            parse_start = time.perf_counter()
            try:
                batch = next(iterator)
            except StopIteration:
                parse_seconds += time.perf_counter() - parse_start
                break
            parse_seconds += time.perf_counter() - parse_start

            batches += 1
            slow_count = len(batch.slow_queries)
            auth_count = len(batch.authentications)
            conn_count = len(batch.connections)
            totals["slow_queries"] += slow_count
            totals["authentications"] += auth_count
            totals["connections"] += conn_count

            status_tracker.ingest_phase(
                path,
                "streaming",
                detail=f"batch {batches}",
                metrics={
                    "batches": batches,
                    "slow_queries": totals["slow_queries"],
                    "authentications": totals["authentications"],
                    "connections": totals["connections"],
                },
            )

            write_start = time.perf_counter()
            _ingest_batch(batch, slow_writer, auth_writer, conn_writer, offset_writer)
            write_seconds += time.perf_counter() - write_start

        status_tracker.ingest_phase(path, "finalizing", detail="writing Parquet artifacts")

        finalize_start = time.perf_counter()
        slow_info = slow_writer.finalize()
        auth_info = auth_writer.finalize()
        conn_info = conn_writer.finalize()
        offset_info = offset_writer.finalize()

        if settings.keep_source_copy:
            source_dir = root / "source"
            source_dir.mkdir(parents=True, exist_ok=True)
            copy_name = f"{file_prefix}{path.suffix or '.log'}"
            copy_target = source_dir / copy_name
            if not copy_target.exists():
                try:
                    shutil.copy2(path, copy_target)
                except Exception:
                    LOGGER.warning("Failed to copy %s to %s", path, copy_target, exc_info=True)
                else:
                    LOGGER.info("Copied source log to %s", copy_target)
            source_record = str(copy_target.resolve()) if copy_target.exists() else source_abs
        else:
            source_record = source_abs

        file_map_info = update_file_map(file_map_path, file_id, source_record)

        manifest_path = root / "manifest.json"
        manifest_info = append_manifest_entry(
            manifest_path,
            dataset_version=settings.dataset_version,
            source_file=path,
            file_id=file_id,
            row_counts={
                "slow_queries": slow_info.get("rows_written", 0),
                "authentications": auth_info.get("rows_written", 0),
                "connections": conn_info.get("rows_written", 0),
                "query_offsets": offset_info.get("rows_written", 0),
            },
            artifacts={
                "slow_queries": slow_info.get("path", ""),
                "authentications": auth_info.get("path", ""),
                "connections": conn_info.get("path", ""),
                "query_offsets": offset_info.get("path", ""),
                "file_map": file_map_info.get("path", ""),
            },
        )
        finalize_seconds += time.perf_counter() - finalize_start

        telemetry: Dict[str, Any] = {
            "input_path": str(path),
            "dataset_version": settings.dataset_version,
            "slow_queries": slow_info,
            "authentications": auth_info,
            "connections": conn_info,
            "query_offsets": offset_info,
            "file_map": file_map_info,
            "manifest": manifest_info,
            "file_id": file_id,
            "timings": {
                "parse_seconds": parse_seconds,
                "write_seconds": write_seconds,
                "finalize_seconds": finalize_seconds,
                "batches": batches,
            },
        }

        duration = time.perf_counter() - overall_start
        status_tracker.ingest_finished(
            path,
            success=True,
            duration_seconds=duration,
            row_counts={
                "slow_queries": slow_info.get("rows_written", 0),
                "authentications": auth_info.get("rows_written", 0),
                "connections": conn_info.get("rows_written", 0),
                "query_offsets": offset_info.get("rows_written", 0),
            },
            timings={
                "parse_seconds": parse_seconds,
                "write_seconds": write_seconds,
                "finalize_seconds": finalize_seconds,
            },
            telemetry=telemetry,
        )

        LOGGER.info(
            "Ingest complete for %s: slow=%d auth=%d connections=%d batches=%d in %.2fs (parse %.2fs, write %.2fs, finalize %.2fs)",
            path,
            slow_info.get("rows_written", 0),
            auth_info.get("rows_written", 0),
            conn_info.get("rows_written", 0),
            batches,
            duration,
            parse_seconds,
            write_seconds,
            finalize_seconds,
        )
        return telemetry
    except Exception as exc:
        duration = time.perf_counter() - overall_start
        status_tracker.ingest_failed(path, str(exc), duration_seconds=duration)
        LOGGER.exception("Ingest failed for %s", path)
        raise


def ingest_slow_query_file(*args, **kwargs) -> Dict[str, Any]:
    """Backward-compatible wrapper that ingests and returns slow-query telemetry."""

    telemetry = ingest_log_file(*args, **kwargs)
    return telemetry["slow_queries"]


def _ingest_batch(
    batch: ParsedBatch,
    slow_writer: SlowQueryBatchWriter,
    auth_writer: AuthenticationBatchWriter,
    conn_writer: ConnectionBatchWriter,
    offset_writer: QueryOffsetBatchWriter,
) -> None:
    if batch.slow_queries:
        slow_writer.write_records(batch.slow_queries)
        offset_writer.write_records(batch.slow_queries)
    if batch.authentications:
        auth_writer.write_records(batch.authentications)
    if batch.connections:
        conn_writer.write_records(batch.connections)
