"""Handle file uploads for the v2 pipeline."""

from __future__ import annotations

import gzip
import shutil
import tarfile
import tempfile
import time
import re
from pathlib import Path
from typing import Iterable, List
from zipfile import ZipFile

from werkzeug.datastructures import FileStorage

from .pipeline import ingest_log_file
from ..runtime import status as status_tracker
from ..config import settings
from ..utils.logging_utils import get_logger

LOG_EXTENSIONS = {".log", ".logs", ".json", ".txt"}
LOG_PATTERN = re.compile(r"\.(log|logs|json|txt)(?:$|[-_.])", re.IGNORECASE)
ARCHIVE_EXTENSIONS = {".zip", ".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2"}

LOGGER = get_logger("ingest.uploader")


def process_uploads(
    files: Iterable[FileStorage],
    *,
    dataset_root: Path,
    temp_dir: Path,
) -> List[dict]:
    dataset_root = Path(dataset_root)
    dataset_root.mkdir(parents=True, exist_ok=True)
    ingested: List[dict] = []

    if settings.wipe_dataset_on_upload:
        _wipe_dataset(dataset_root)

    for storage in files:
        if not storage or not storage.filename:
            continue

        filename = Path(storage.filename).name
        target = temp_dir / filename

        save_start = time.perf_counter()
        storage.save(target)
        save_seconds = time.perf_counter() - save_start
        size_bytes = target.stat().st_size if target.exists() else 0
        LOGGER.info(
            "Stored upload %s at %s (%.2f MiB) in %.2fs",
            filename,
            target,
            size_bytes / (1024 * 1024) if size_bytes else 0.0,
            save_seconds,
        )

        expand_start = time.perf_counter()
        log_targets = _expand_to_logs(target, temp_dir)
        expand_seconds = time.perf_counter() - expand_start
        LOGGER.info(
            "Expanded %s into %d log file(s) in %.2fs",
            filename,
            len(log_targets),
            expand_seconds,
        )

        base_metrics = {
            "original_filename": filename,
            "saved_path": str(target),
            "saved_bytes": size_bytes,
            "save_seconds": save_seconds,
            "expand_seconds": expand_seconds,
            "expanded_count": len(log_targets),
        }

        if not log_targets:
            LOGGER.warning("No valid log files discovered in upload %s", filename)
            continue

        for index, log_path in enumerate(log_targets, start=1):
            per_log_metrics = dict(base_metrics)
            per_log_metrics.update(
                {
                    "expanded_path": str(log_path),
                    "expanded_index": index,
                }
            )
            status_tracker.ingest_upload_prepared(log_path, per_log_metrics)
            LOGGER.info(
                "Dispatching ingest for %s (%d/%d)",
                log_path,
                index,
                len(log_targets),
            )
            telemetry = ingest_log_file(
                log_path,
                output_root=dataset_root,
                upload_metrics=per_log_metrics,
            )
            ingested.append(telemetry)
    return ingested


def _wipe_dataset(root: Path) -> None:
    """Remove existing Parquet outputs to mimic legacy single-upload behavior."""

    targets = [
        root / "slow_queries",
        root / "authentications",
        root / "connections",
        root / "query_offsets",
        root / "manifest.json",
        root / "file_map.json",
    ]
    for target in targets:
        try:
            if target.is_dir():
                shutil.rmtree(target, ignore_errors=True)
            elif target.exists():
                target.unlink(missing_ok=True)
        except Exception:
            LOGGER.warning("Failed to remove %s while wiping dataset", target)


def _expand_to_logs(path: Path, temp_dir: Path) -> List[Path]:
    logs: List[Path] = []
    name = path.name.lower()

    if any(
        name.endswith(ext)
        for ext in (".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".zip", ".tar")
    ):
        extract_dir = Path(tempfile.mkdtemp(dir=temp_dir, prefix="extract_"))
        LOGGER.debug("Extracting archive %s into %s", path, extract_dir)
        if name.endswith(".zip"):
            with ZipFile(path) as zipf:
                _safe_extract_zip(zipf, extract_dir)
        else:
            with tarfile.open(path) as tar:
                _safe_extract_tar(tar, extract_dir)
        for candidate in extract_dir.rglob("*"):
            if candidate.is_file() and LOG_PATTERN.search(candidate.name):
                logs.append(candidate)
        return logs

    if name.endswith(".gz") and not any(
        name.endswith(ext) for ext in (".tar.gz", ".tgz")
    ):
        dest = temp_dir / path.stem
        LOGGER.debug("Decompressing gzip %s into %s", path, dest)
        with gzip.open(path, "rb") as src, open(dest, "wb") as dst:
            shutil.copyfileobj(src, dst)
        if LOG_PATTERN.search(dest.name):
            logs.append(dest)
        return logs

    if LOG_PATTERN.search(name):
        logs.append(path)
        return logs

    return logs


def _safe_extract_zip(zipf: ZipFile, destination: Path) -> None:
    dest_root = destination.resolve()
    for member in zipf.infolist():
        member_path = (destination / member.filename).resolve()
        if not member_path.is_relative_to(dest_root):
            raise ValueError(f"Unsafe path in zip archive: {member.filename}")
        if member.is_dir() or member.filename.endswith("/"):
            member_path.mkdir(parents=True, exist_ok=True)
            continue
        member_path.parent.mkdir(parents=True, exist_ok=True)
        with zipf.open(member) as src, open(member_path, "wb") as dst:
            shutil.copyfileobj(src, dst)


def _safe_extract_tar(tar: tarfile.TarFile, destination: Path) -> None:
    dest_root = destination.resolve()
    safe_members = []
    for member in tar.getmembers():
        member_path = (destination / member.name).resolve()
        if not member_path.is_relative_to(dest_root):
            raise ValueError(f"Unsafe path in tar archive: {member.name}")
        safe_members.append(member)
    tar.extractall(destination, members=safe_members)
