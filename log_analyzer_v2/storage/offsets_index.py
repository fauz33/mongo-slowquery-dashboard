"""Offset index generation for raw detail lookups."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, Any, Sequence, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from ..ingest.parser import SlowQueryRecord
from ..utils.logging_utils import get_logger

LOGGER = get_logger("storage.offsets_index")

QUERY_OFFSET_SCHEMA = pa.schema(
    [
        ("query_hash", pa.string()),
        ("timestamp", pa.string()),
        ("ts_epoch", pa.int64()),
        ("database", pa.string()),
        ("collection", pa.string()),
        ("file_id", pa.int32()),
        ("file_offset", pa.int64()),
        ("line_length", pa.int32()),
        ("line_number", pa.int32()),
    ]
)


def _rows_from_records(records: Sequence[SlowQueryRecord], file_id: int) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    for record in records:
        rows.append(
            {
                "query_hash": record.query_hash,
                "timestamp": record.timestamp,
                "ts_epoch": record.ts_epoch,
                "database": record.database,
                "collection": record.collection,
                "file_id": file_id,
                "file_offset": record.file_offset,
                "line_length": record.line_length,
                "line_number": record.line_number,
            }
        )
    return rows


class QueryOffsetBatchWriter:
    def __init__(self, destination: Path, *, file_id: Optional[int], compression: str) -> None:
        self.destination = Path(destination)
        self.file_id = file_id if file_id is not None else -1
        self.compression = compression
        self._writer: Optional[pq.ParquetWriter] = None
        self._rows_written = 0

    def write_records(self, records: Sequence[SlowQueryRecord]) -> None:
        rows = _rows_from_records(records, self.file_id)
        if not rows:
            return
        table = pa.Table.from_pylist(rows, schema=QUERY_OFFSET_SCHEMA)
        if self._writer is None:
            self.destination.parent.mkdir(parents=True, exist_ok=True)
            self._writer = pq.ParquetWriter(
                self.destination, QUERY_OFFSET_SCHEMA, compression=self.compression
            )
        assert self._writer is not None
        self._writer.write_table(table)
        self._rows_written += int(table.num_rows)

    def finalize(self) -> Dict[str, Any]:
        if self._writer is not None:
            self._writer.close()
        else:
            self.destination.parent.mkdir(parents=True, exist_ok=True)
            empty_table = pa.Table.from_pylist([], schema=QUERY_OFFSET_SCHEMA)
            pq.write_table(empty_table, self.destination, compression=self.compression)
            LOGGER.info("Created empty offset index at %s", self.destination)
        return {"rows_written": self._rows_written, "path": str(self.destination)}


def write_query_offsets(
    rows: list[Dict[str, Any]],
    destination: Path,
    *,
    compression: str,
) -> Dict[str, Any]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=QUERY_OFFSET_SCHEMA)
    pq.write_table(table, destination, compression=compression)
    LOGGER.info("Wrote %d offset rows to %s", table.num_rows, destination)
    return {"rows_written": int(table.num_rows), "path": str(destination)}
