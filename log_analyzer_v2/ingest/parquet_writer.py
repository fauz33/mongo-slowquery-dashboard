"""Parquet serialization helpers for ingesting log records."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from .parser import (
    AuthenticationRecord,
    ConnectionRecord,
    SlowQueryRecord,
)
from ..utils.logging_utils import get_logger

LOGGER = get_logger("ingest.parquet_writer")

SLOW_QUERY_SCHEMA = pa.schema(
    [
        ("timestamp", pa.string()),
        ("ts_epoch", pa.int64()),
        ("duration_ms", pa.int32()),
        ("docs_examined", pa.int32()),
        ("docs_returned", pa.int32()),
        ("keys_examined", pa.int32()),
        ("query_hash", pa.string()),
        ("database", pa.string()),
        ("collection", pa.string()),
        ("namespace", pa.string()),
        ("plan_summary", pa.string()),
        ("query_text", pa.string()),
        ("operation", pa.string()),
        ("connection_id", pa.string()),
        ("username", pa.string()),
        ("file_path", pa.string()),
        ("file_offset", pa.int64()),
        ("line_number", pa.int32()),
        ("line_length", pa.int32()),
        ("file_id", pa.int32()),
    ]
)

AUTHENTICATION_SCHEMA = pa.schema(
    [
        ("timestamp", pa.string()),
        ("ts_epoch", pa.int64()),
        ("user", pa.string()),
        ("database", pa.string()),
        ("mechanism", pa.string()),
        ("result", pa.string()),
        ("connection_id", pa.string()),
        ("remote_address", pa.string()),
        ("app_name", pa.string()),
        ("error", pa.string()),
        ("file_path", pa.string()),
        ("file_offset", pa.int64()),
        ("line_number", pa.int32()),
        ("file_id", pa.int32()),
    ]
)

CONNECTION_SCHEMA = pa.schema(
    [
        ("timestamp", pa.string()),
        ("ts_epoch", pa.int64()),
        ("event", pa.string()),
        ("connection_id", pa.string()),
        ("remote_address", pa.string()),
        ("connection_count", pa.int32()),
        ("app_name", pa.string()),
        ("driver", pa.string()),
        ("file_path", pa.string()),
        ("file_offset", pa.int64()),
        ("line_number", pa.int32()),
        ("file_id", pa.int32()),
    ]
)


def _prepare_rows(records: Sequence[Any], file_id: Optional[int]) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    for record in records:
        payload = record.as_dict()
        payload["file_id"] = file_id if file_id is not None else -1
        rows.append(payload)
    return rows


class ParquetBatchWriter:
    """Minimal batching wrapper around :class:`pyarrow.parquet.ParquetWriter`."""

    def __init__(self, destination: Path, schema: pa.Schema, *, compression: str) -> None:
        self.destination = Path(destination)
        self.schema = schema
        self.compression = compression
        self._writer: Optional[pq.ParquetWriter] = None
        self._rows_written = 0

    def write_rows(self, rows: list[Dict[str, Any]]) -> None:
        if not rows:
            return
        table = pa.Table.from_pylist(rows, schema=self.schema)
        if self._writer is None:
            self.destination.parent.mkdir(parents=True, exist_ok=True)
            self._writer = pq.ParquetWriter(
                self.destination, self.schema, compression=self.compression
            )
        assert self._writer is not None
        self._writer.write_table(table)
        self._rows_written += int(table.num_rows)
        LOGGER.debug(
            "Appended %d rows to %s (total=%d)",
            table.num_rows,
            self.destination,
            self._rows_written,
        )

    def finalize(self) -> Dict[str, Any]:
        if self._writer is not None:
            self._writer.close()
        return {"rows_written": self._rows_written, "path": str(self.destination)}


class SlowQueryBatchWriter(ParquetBatchWriter):
    def __init__(self, destination: Path, *, file_id: Optional[int], compression: str) -> None:
        super().__init__(destination, SLOW_QUERY_SCHEMA, compression=compression)
        self.file_id = file_id if file_id is not None else -1

    def write_records(self, records: Sequence[SlowQueryRecord]) -> None:
        if not records:
            return
        rows = _prepare_rows(records, self.file_id)
        self.write_rows(rows)


class AuthenticationBatchWriter(ParquetBatchWriter):
    def __init__(self, destination: Path, *, file_id: Optional[int], compression: str) -> None:
        super().__init__(destination, AUTHENTICATION_SCHEMA, compression=compression)
        self.file_id = file_id if file_id is not None else -1

    def write_records(self, records: Sequence[AuthenticationRecord]) -> None:
        if not records:
            return
        rows = _prepare_rows(records, self.file_id)
        self.write_rows(rows)


class ConnectionBatchWriter(ParquetBatchWriter):
    def __init__(self, destination: Path, *, file_id: Optional[int], compression: str) -> None:
        super().__init__(destination, CONNECTION_SCHEMA, compression=compression)
        self.file_id = file_id if file_id is not None else -1

    def write_records(self, records: Sequence[ConnectionRecord]) -> None:
        if not records:
            return
        rows = _prepare_rows(records, self.file_id)
        self.write_rows(rows)


def write_slow_queries(
    records: Iterable[SlowQueryRecord],
    destination: Path,
    *,
    file_id: Optional[int] = None,
    compression: str = "snappy",
) -> Dict[str, Any]:
    writer = ParquetBatchWriter(destination, SLOW_QUERY_SCHEMA, compression=compression)
    batch: list[SlowQueryRecord] = []
    for record in records:
        batch.append(record)
        if len(batch) >= 1000:
            writer.write_rows(_prepare_rows(batch, file_id))
            batch = []
    if batch:
        writer.write_rows(_prepare_rows(batch, file_id))
    return writer.finalize()


def write_authentications(
    records: Iterable[AuthenticationRecord],
    destination: Path,
    *,
    file_id: Optional[int] = None,
    compression: str = "snappy",
) -> Dict[str, Any]:
    writer = ParquetBatchWriter(destination, AUTHENTICATION_SCHEMA, compression=compression)
    batch: list[AuthenticationRecord] = []
    for record in records:
        batch.append(record)
        if len(batch) >= 1000:
            writer.write_rows(_prepare_rows(batch, file_id))
            batch = []
    if batch:
        writer.write_rows(_prepare_rows(batch, file_id))
    return writer.finalize()


def write_connections(
    records: Iterable[ConnectionRecord],
    destination: Path,
    *,
    file_id: Optional[int] = None,
    compression: str = "snappy",
) -> Dict[str, Any]:
    writer = ParquetBatchWriter(destination, CONNECTION_SCHEMA, compression=compression)
    batch: list[ConnectionRecord] = []
    for record in records:
        batch.append(record)
        if len(batch) >= 1000:
            writer.write_rows(_prepare_rows(batch, file_id))
            batch = []
    if batch:
        writer.write_rows(_prepare_rows(batch, file_id))
    return writer.finalize()
