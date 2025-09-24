"""Log parsing utilities for the v2 ingest pipeline."""

from __future__ import annotations

import json
import time
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha1
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from ..utils.logging_utils import get_logger

LOGGER = get_logger("ingest.parser")


# ---------------------------------------------------------------------------
# Dataclasses representing normalized events


@dataclass
class SlowQueryRecord:
    """Normalized representation of a slow query log event."""

    timestamp: str
    ts_epoch: int
    duration_ms: int
    docs_examined: int
    docs_returned: int
    keys_examined: int
    query_hash: str
    database: str
    collection: str
    namespace: str
    plan_summary: str
    query_text: str
    operation: str
    connection_id: Optional[str]
    username: Optional[str]
    file_path: str
    file_offset: int
    line_number: int
    line_length: int

    def as_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "ts_epoch": self.ts_epoch,
            "duration_ms": self.duration_ms,
            "docs_examined": self.docs_examined,
            "docs_returned": self.docs_returned,
            "keys_examined": self.keys_examined,
            "query_hash": self.query_hash,
            "database": self.database,
            "collection": self.collection,
            "namespace": self.namespace,
            "plan_summary": self.plan_summary,
            "query_text": self.query_text,
            "operation": self.operation,
            "connection_id": self.connection_id,
            "username": self.username,
            "file_path": self.file_path,
            "file_offset": self.file_offset,
            "line_number": self.line_number,
            "line_length": self.line_length,
        }


@dataclass
class AuthenticationRecord:
    """Normalized authentication audit event."""

    timestamp: str
    ts_epoch: int
    user: Optional[str]
    database: Optional[str]
    mechanism: Optional[str]
    result: str
    connection_id: Optional[str]
    remote_address: Optional[str]
    app_name: Optional[str]
    error: Optional[str]
    file_path: str
    file_offset: int
    line_number: int

    def as_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "ts_epoch": self.ts_epoch,
            "user": self.user,
            "database": self.database,
            "mechanism": self.mechanism,
            "result": self.result,
            "connection_id": self.connection_id,
            "remote_address": self.remote_address,
            "app_name": self.app_name,
            "error": self.error,
            "file_path": self.file_path,
            "file_offset": self.file_offset,
            "line_number": self.line_number,
        }


@dataclass
class ConnectionRecord:
    """Normalized connection lifecycle event."""

    timestamp: str
    ts_epoch: int
    event: str  # "accepted" or "ended"
    connection_id: Optional[str]
    remote_address: Optional[str]
    connection_count: Optional[int]
    app_name: Optional[str]
    driver: Optional[str]
    file_path: str
    file_offset: int
    line_number: int

    def as_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "ts_epoch": self.ts_epoch,
            "event": self.event,
            "connection_id": self.connection_id,
            "remote_address": self.remote_address,
            "connection_count": self.connection_count,
            "app_name": self.app_name,
            "driver": self.driver,
            "file_path": self.file_path,
            "file_offset": self.file_offset,
            "line_number": self.line_number,
        }


@dataclass
class ParsedBatch:
    """Container for a chunk of parsed events."""

    slow_queries: List[SlowQueryRecord]
    authentications: List[AuthenticationRecord]
    connections: List[ConnectionRecord]

    def is_empty(self) -> bool:
        return not (self.slow_queries or self.authentications or self.connections)


# ---------------------------------------------------------------------------
# Timestamp parsing helpers


ISO_PARSE_FORMATS = (
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S%z",
)


def _parse_timestamp(raw: str) -> Tuple[str, int]:
    for fmt in ISO_PARSE_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.isoformat(), int(dt.timestamp())
        except ValueError:
            continue

    try:
        dt = datetime.fromisoformat(raw)
        return dt.isoformat(), int(dt.timestamp())
    except ValueError:
        LOGGER.warning("Failed to parse timestamp %s; defaulting to epoch=0", raw)
        return raw, 0


# ---------------------------------------------------------------------------
# Slow query helpers


def _safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def _stringify_command(command: Any) -> str:
    if command is None:
        return "{}"
    if isinstance(command, (str, bytes)):
        return command.decode() if isinstance(command, bytes) else command
    try:
        return json.dumps(command, default=str, separators=(",", ":"))
    except (TypeError, ValueError):
        return str(command)


def _infer_operation(attr: Dict[str, Any], command: Any) -> str:
    command_name = attr.get("commandName")
    if command_name:
        return str(command_name)

    if isinstance(command, dict):
        for key in ("find", "aggregate", "update", "delete", "insert", "getMore"):
            if key in command:
                return key
        op_name = command.get("commandName") or command.get("operation")
        if isinstance(op_name, str) and op_name:
            return op_name
        if len(command) == 1:
            sole = next(iter(command))
            if isinstance(sole, str):
                return sole

        if "updates" in command and isinstance(command["updates"], list):
            return "update"
        if "deletes" in command and isinstance(command["deletes"], list):
            return "delete"
        if "inserts" in command and isinstance(command["inserts"], list):
            return "insert"
        if "q" in command and "u" in command:
            return "update"
        if "$query" in command and isinstance(command["$query"], dict):
            nested = command["$query"].get("commandName")
            if isinstance(nested, str) and nested:
                return nested

    plan_summary = attr.get("planSummary")
    if plan_summary:
        return str(plan_summary)

    return "unknown"


def _ensure_hash(
    attr: Dict[str, Any],
    namespace: str,
    database: str,
    collection: str,
    query_text: str,
) -> str:
    existing = attr.get("queryHash")
    if existing:
        return str(existing)
    return _generate_synthetic_query_hash(
        query_text=query_text,
        database=database,
        collection=collection,
        namespace=namespace,
    )


def _generate_synthetic_query_hash(
    *,
    query_text: str,
    database: str,
    collection: str,
    namespace: str,
) -> str:
    database = database or "unknown"
    collection = collection or "unknown"

    normalized_parts: List[str] = [f"{database}.{collection}"]
    trimmed = query_text.strip() if isinstance(query_text, str) else ""

    if trimmed:
        if trimmed.startswith("{"):
            try:
                query_obj = json.loads(trimmed)
            except json.JSONDecodeError:
                normalized_parts.append(_normalize_text_query(trimmed))
            else:
                if isinstance(query_obj, dict):
                    op_found = False
                    for key in ("find", "aggregate", "update", "delete", "insert", "command"):
                        if key in query_obj:
                            normalized_parts.append(f"op:{key}")
                            op_found = True
                            break
                    if not op_found:
                        normalized_parts.append("op:filter")
                        filter_keys = _extract_query_structure(query_obj)
                        if filter_keys:
                            normalized_parts.append(f"filter:{','.join(sorted(filter_keys))}")

                    if "filter" in query_obj:
                        filter_keys = _extract_query_structure(query_obj["filter"])
                        if filter_keys:
                            normalized_parts.append(f"filter:{','.join(sorted(filter_keys))}")

                    if "pipeline" in query_obj and isinstance(query_obj["pipeline"], list):
                        pipeline_ops: List[str] = []
                        sort_parts: List[str] = []
                        match_fields: set[str] = set()
                        for stage in query_obj["pipeline"]:
                            if not isinstance(stage, dict):
                                continue
                            for op in stage.keys():
                                if isinstance(op, str) and op.startswith("$"):
                                    pipeline_ops.append(op)
                            if "$sort" in stage and isinstance(stage["$sort"], dict):
                                for field, direction in stage["$sort"].items():
                                    try:
                                        direction_val = int(direction)
                                    except Exception:
                                        direction_val = 1
                                    sort_parts.append(f"{field}:{direction_val}")
                            if "$match" in stage and isinstance(stage["$match"], dict):
                                match_keys = _extract_query_structure(stage["$match"])
                                match_fields.update(match_keys)
                                match_hash = hashlib.md5(
                                    json.dumps(stage["$match"], sort_keys=True).encode()
                                ).hexdigest()[:8]
                                match_fields.add(f"match_values_{match_hash}")
                        if pipeline_ops:
                            normalized_parts.append(f"pipeline:{','.join(pipeline_ops)}")
                        if sort_parts:
                            normalized_parts.append(f"pipeline_sort:{','.join(sort_parts)}")
                        if match_fields:
                            normalized_parts.append(f"pipeline_match:{','.join(sorted(match_fields))}")

                    if "updates" in query_obj and isinstance(query_obj["updates"], list):
                        update_filters: set[str] = set()
                        for update in query_obj["updates"]:
                            if isinstance(update, dict) and "q" in update:
                                update_filters.update(_extract_query_structure(update["q"]))
                        if update_filters:
                            normalized_parts.append(f"updates_filter:{','.join(sorted(update_filters))}")

                    if "deletes" in query_obj and isinstance(query_obj["deletes"], list):
                        delete_filters: set[str] = set()
                        for delete in query_obj["deletes"]:
                            if isinstance(delete, dict) and "q" in delete:
                                delete_filters.update(_extract_query_structure(delete["q"]))
                        if delete_filters:
                            normalized_parts.append(f"deletes_filter:{','.join(sorted(delete_filters))}")

                    if "sort" in query_obj and isinstance(query_obj["sort"], dict):
                        sort_parts: List[str] = []
                        for field, direction in query_obj["sort"].items():
                            try:
                                direction_val = int(direction)
                            except Exception:
                                direction_val = 1
                            sort_parts.append(f"{field}:{direction_val}")
                        if sort_parts:
                            normalized_parts.append(f"sort:{','.join(sort_parts)}")
        else:
            normalized_parts.append(_normalize_text_query(trimmed))
    else:
        normalized_parts.append("query:unknown")

    normalized_query = "|".join(part for part in normalized_parts if part)
    return hashlib.md5(normalized_query.encode("utf-8", "ignore")).hexdigest()


def _normalize_text_query(query_text: str) -> str:
    lowered = query_text.lower()
    if "command" in lowered:
        command_match = re.search(r"command\s+(\w+)", query_text, re.IGNORECASE)
        if command_match:
            return f"command:{command_match.group(1)}"
    if "slow query" in lowered:
        return "slow_query"
    return re.sub(r"\s+", " ", query_text[:50]).strip()


def _extract_query_structure(
    filter_obj: Any,
    max_depth: int = 2,
    current_depth: int = 0,
) -> set[str]:
    if current_depth >= max_depth or not isinstance(filter_obj, dict):
        return set()

    field_names: set[str] = set()
    for key, value in filter_obj.items():
        if isinstance(key, str) and not key.startswith("$"):
            field_names.add(key)
            if isinstance(value, dict) and "$regex" in value:
                pattern_info = value.get("$regex", "")
                if isinstance(pattern_info, dict) and "$regularExpression" in pattern_info:
                    pattern_info = pattern_info["$regularExpression"].get("pattern", "")
                pattern_hash = hashlib.md5(str(pattern_info).encode()).hexdigest()[:8]
                field_names.add(f"{key}_regex_{pattern_hash}")
        if isinstance(value, dict):
            field_names.update(
                _extract_query_structure(value, max_depth=max_depth, current_depth=current_depth + 1)
            )
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    field_names.update(
                        _extract_query_structure(item, max_depth=max_depth, current_depth=current_depth + 1)
                    )
    return field_names


# ---------------------------------------------------------------------------
# Authentication / connection helpers


_AUTH_MESSAGE_RESULT = {
    "successfully authenticated": "success",
    "authentication succeeded": "success",
    "authentication failed": "failure",
}


def _match_auth_result(message_lower: str) -> Optional[str]:
    for needle, result in _AUTH_MESSAGE_RESULT.items():
        if needle in message_lower:
            return result
    return None


def _extract_remote(attr: Dict[str, Any]) -> Optional[str]:
    return (
        attr.get("remote")
        or attr.get("client")
        or attr.get("remoteAddr")
        or attr.get("remote_address")
    )


def _extract_connection_id(attr: Dict[str, Any], ctx: Optional[str]) -> Optional[str]:
    return attr.get("connectionId") or attr.get("connId") or ctx


# ---------------------------------------------------------------------------
# Public parsing API


def parse_log_file(filepath: Path, *, batch_size: int = 1000) -> Iterator[ParsedBatch]:
    """Parse *filepath* yielding batches of normalized events."""

    path = Path(filepath)
    slow_queries: List[SlowQueryRecord] = []
    authentications: List[AuthenticationRecord] = []
    connections: List[ConnectionRecord] = []

    total_slow = 0
    total_auth = 0
    total_conn = 0
    batch_index = 0
    start_time = time.perf_counter()

    def emit_batch() -> Iterator[ParsedBatch]:
        nonlocal slow_queries, authentications, connections
        nonlocal total_slow, total_auth, total_conn, batch_index
        if slow_queries or authentications or connections:
            batch_index += 1
            slow_count = len(slow_queries)
            auth_count = len(authentications)
            conn_count = len(connections)
            total_slow += slow_count
            total_auth += auth_count
            total_conn += conn_count
            LOGGER.debug(
                "Emitting batch %d from %s: slow=%d auth=%d conn=%d (cumulative slow=%d auth=%d conn=%d)",
                batch_index,
                path.name,
                slow_count,
                auth_count,
                conn_count,
                total_slow,
                total_auth,
                total_conn,
            )
            batch = ParsedBatch(
                slow_queries=slow_queries,
                authentications=authentications,
                connections=connections,
            )
            slow_queries = []
            authentications = []
            connections = []
            yield batch

    line_number = 0
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        while True:
            offset = handle.tell()
            line = handle.readline()
            if not line:
                break
            line_number += 1
            line_length = len(line)

            stripped = line.strip()
            if not stripped or not stripped.startswith("{"):
                continue

            try:
                entry = json.loads(stripped)
            except json.JSONDecodeError:
                LOGGER.debug("Skipping unparsable line %s:%d", path, line_number)
                continue

            attr = entry.get("attr", {}) or {}
            message = entry.get("msg", "")
            message_lower = message.lower()
            timestamp_raw = entry.get("t", {}).get("$date")
            if not timestamp_raw:
                LOGGER.debug("Missing timestamp in %s:%d", path, line_number)
                continue

            timestamp_iso, ts_epoch = _parse_timestamp(str(timestamp_raw))
            ctx = entry.get("ctx")

            # Slow query event
            if "slow query" in message_lower:
                command = attr.get("command") or attr.get("commandBody") or {}
                query_text = _stringify_command(command)
                database = attr.get("db") or attr.get("ns", "").split(".")[0] or "unknown"
                collection = (
                    attr.get("collection")
                    or (attr.get("ns") or "unknown.unknown").split(".")[-1]
                    or "unknown"
                )
                namespace = attr.get("ns") or f"{database}.{collection}"
                database_str = str(database or "unknown")
                collection_str = str(collection or "unknown")
                namespace_str = str(namespace or "unknown.unknown")
                slow_queries.append(
                    SlowQueryRecord(
                        timestamp=timestamp_iso,
                        ts_epoch=ts_epoch,
                        duration_ms=int(attr.get("durationMillis", 0) or 0),
                        docs_examined=int(attr.get("docsExamined", 0) or 0),
                        docs_returned=int(
                            attr.get("nReturned", 0)
                            or attr.get("docsReturned", 0)
                            or 0
                        ),
                        keys_examined=int(attr.get("keysExamined", 0) or 0),
                        query_hash=_ensure_hash(
                            attr,
                            namespace_str,
                            database_str,
                            collection_str,
                            query_text,
                        ),
                        database=database_str,
                        collection=collection_str,
                        namespace=namespace_str,
                        plan_summary=str(attr.get("planSummary", "None")),
                        query_text=query_text,
                        operation=_infer_operation(attr, command),
                        connection_id=_safe_str(_extract_connection_id(attr, ctx)),
                        username=_safe_str(attr.get("appName") or attr.get("user")),
                        file_path=str(path),
                        file_offset=offset,
                        line_number=line_number,
                        line_length=line_length,
                    )
                )
                if (
                    len(slow_queries)
                    + len(authentications)
                    + len(connections)
                ) >= batch_size:
                    yield from emit_batch()
                continue

            # Connection lifecycle event
            if "connection accepted" in message_lower or "connection ended" in message_lower:
                event = "accepted" if "connection accepted" in message_lower else "ended"
                remote = _extract_remote(attr)
                connection_id = _safe_str(_extract_connection_id(attr, ctx))
                connection_count = attr.get("connectionCount")
                try:
                    if connection_count is not None:
                        connection_count = int(connection_count)
                except (TypeError, ValueError):
                    LOGGER.debug(
                        "Failed to normalize connectionCount in %s:%d", path, line_number
                    )
                    connection_count = None

                connections.append(
                    ConnectionRecord(
                        timestamp=timestamp_iso,
                        ts_epoch=ts_epoch,
                        event=event,
                        connection_id=connection_id,
                        remote_address=_safe_str(remote),
                        connection_count=connection_count,
                        app_name=_safe_str(attr.get("appName")),
                        driver=_safe_str(attr.get("driver")),
                        file_path=str(path),
                        file_offset=offset,
                        line_number=line_number,
                    )
                )
                if (
                    len(slow_queries)
                    + len(authentications)
                    + len(connections)
                ) >= batch_size:
                    yield from emit_batch()
                continue

            # Authentication audit event
            if entry.get("c") == "ACCESS":
                result = _match_auth_result(message_lower)
                if result is None:
                    continue

                raw_user = attr.get("user")
                username: Optional[str] = None
                database: Optional[str] = None

                if isinstance(raw_user, dict):
                    username = _safe_str(
                        raw_user.get("user")
                        or raw_user.get("userName")
                        or raw_user.get("username")
                        or raw_user.get("name")
                    )
                    database = _safe_str(
                        raw_user.get("db")
                        or raw_user.get("dbName")
                        or raw_user.get("database")
                    )
                else:
                    username = _safe_str(raw_user)

                if not username:
                    username = _safe_str(
                        attr.get("principalName")
                        or attr.get("principal")
                        or attr.get("principal_user")
                    )

                if database is None:
                    database = _safe_str(
                        attr.get("db")
                        or attr.get("authenticationDatabase")
                        or attr.get("principalDb")
                    )

                authentications.append(
                    AuthenticationRecord(
                        timestamp=timestamp_iso,
                        ts_epoch=ts_epoch,
                        user=username,
                        database=database,
                        mechanism=_safe_str(attr.get("mechanism") or attr.get("mechanismName")),
                        result=result,
                        connection_id=_safe_str(_extract_connection_id(attr, ctx)),
                        remote_address=_safe_str(_extract_remote(attr)),
                        app_name=_safe_str(attr.get("appName")),
                        error=_safe_str(attr.get("error") or attr.get("err")),
                        file_path=str(path),
                        file_offset=offset,
                        line_number=line_number,
                    )
                )
                if (
                    len(slow_queries)
                    + len(authentications)
                    + len(connections)
                ) >= batch_size:
                    yield from emit_batch()
                continue

    yield from emit_batch()
    duration = time.perf_counter() - start_time
    LOGGER.info(
        "Parsed %s: lines=%d slow=%d auth=%d conn=%d batches=%d in %.2fs",
        path,
        line_number,
        total_slow,
        total_auth,
        total_conn,
        batch_index,
        duration,
    )


# ---------------------------------------------------------------------------
# Convenience wrappers


def iter_slow_queries(filepath: Path) -> Iterator[SlowQueryRecord]:
    for batch in parse_log_file(filepath):
        yield from batch.slow_queries


def collect_slow_queries(filepath: Path) -> Iterable[SlowQueryRecord]:
    return list(iter_slow_queries(filepath))


def collect_authentications(filepath: Path) -> Iterable[AuthenticationRecord]:
    records: List[AuthenticationRecord] = []
    for batch in parse_log_file(filepath):
        records.extend(batch.authentications)
    return records


def collect_connections(filepath: Path) -> Iterable[ConnectionRecord]:
    records: List[ConnectionRecord] = []
    for batch in parse_log_file(filepath):
        records.extend(batch.connections)
    return records
