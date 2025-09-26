"""DuckDB-backed analytics helpers for Parquet datasets."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Mapping

from ..config import settings
from ..storage.manifest import load_manifest
from ..storage.file_map import load_file_map
from ..utils.logging_utils import get_logger
from .index_suggestions import generate_index_suggestions

try:
    import duckdb  # type: ignore
except ImportError as exc:  # pragma: no cover - optional dependency
    raise RuntimeError(
        "DuckDB is required for analytics (pip install duckdb)."
    ) from exc

LOGGER = get_logger("analytics.duckdb")

_SYSTEM_DATABASES = (
    "admin",
    "local",
    "config",
    "$external",
    "unknown",
)

_SYSTEM_DATABASE_SET = set(_SYSTEM_DATABASES)

_SYSTEM_USERS = (
    "__system",
    "admin",
    "root",
    "mongodb",
    "system",
)


class DuckDBService:
    """Thin wrapper providing analytical queries over Parquet outputs."""

    def __init__(
        self,
        *,
        dataset_root: Path | None = None,
        eager: bool = True,
    ) -> None:
        self.dataset_root = Path(dataset_root) if dataset_root else settings.output_root
        self._conn = duckdb.connect(database=":memory:")
        self._available_views: Dict[str, bool] = {}
        self._file_map_cache: Dict[int, str] | None = None
        if eager:
            self.refresh()

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self._conn

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------
    # Dataset discovery / registration

    def refresh(self) -> None:
        """Re-scan the dataset root and register Parquet views."""

        LOGGER.debug("Refreshing DuckDB views under %s", self.dataset_root)
        self._register_parquet_view("slow_queries", self._collect_files("slow_queries"))
        self._register_parquet_view(
            "authentications", self._collect_files("authentications")
        )
        self._register_parquet_view("connections", self._collect_files("connections"))
        self._register_parquet_view("query_offsets", self._collect_files("index"))
        self._available_views["manifest"] = (self.dataset_root / "manifest.json").exists()
        self._file_map_cache = None

    def _collect_files(self, subdir: str) -> List[str]:
        directory = self.dataset_root / subdir
        if not directory.exists():
            return []
        return [str(path.resolve()) for path in sorted(directory.glob("*.parquet"))]

    def _register_parquet_view(self, name: str, files: List[str]) -> None:
        if files:
            file_array = ", ".join(f"'{f.replace("'", "''")}'" for f in files)
            self._conn.execute(
                f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet([{file_array}])"
            )
            self._available_views[name] = True
            LOGGER.debug("Registered view %s with %d files", name, len(files))
        else:
            self._conn.execute(f"DROP VIEW IF EXISTS {name}")
            self._available_views[name] = False
            LOGGER.debug("View %s dropped (no files)", name)

    # ------------------------------------------------------------------
    # Basic analytics

    def get_namespace_summary(
        self, *, limit: int = 50, filters: Dict[str, Any] | None = None
    ) -> List[Dict[str, Any]]:
        """Return aggregate slow query stats grouped by namespace."""

        if not self._available_views.get("slow_queries"):
            return []

        where_clause, params = self._build_slow_query_filters(filters)
        query = f"""
            SELECT
                namespace,
                COUNT(*) AS executions,
                AVG(duration_ms) AS avg_duration_ms,
                MAX(duration_ms) AS max_duration_ms,
                SUM(docs_examined) AS total_docs_examined,
                SUM(docs_returned) AS total_docs_returned
            FROM slow_queries
            {where_clause}
            GROUP BY namespace
            ORDER BY avg_duration_ms DESC
            LIMIT ?
        """
        cursor = self._conn.execute(query, [*params, limit])
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_plan_summary(
        self, *, limit: int = 50, filters: Dict[str, Any] | None = None
    ) -> List[Dict[str, Any]]:
        """Aggregate slow queries by plan summary."""

        if not self._available_views.get("slow_queries"):
            return []

        where_clause, params = self._build_slow_query_filters(filters)
        query = f"""
            SELECT
                COALESCE(NULLIF(plan_summary, ''), 'None') AS plan_summary,
                COUNT(*) AS executions,
                AVG(duration_ms) AS avg_duration_ms,
                SUM(docs_examined) AS total_docs_examined,
                SUM(docs_returned) AS total_docs_returned
            FROM slow_queries
            {where_clause}
            GROUP BY plan_summary
            ORDER BY executions DESC
            LIMIT ?
        """
        cursor = self._conn.execute(query, [*params, limit])
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_operation_mix(
        self, *, filters: Dict[str, Any] | None = None
    ) -> List[Dict[str, Any]]:
        """Return execution counts per operation type."""

        if not self._available_views.get("slow_queries"):
            return []

        where_clause, params = self._build_slow_query_filters(filters)
        query = f"""
            SELECT
                COALESCE(NULLIF(operation, ''), 'unknown') AS operation,
                COUNT(*) AS executions,
                AVG(duration_ms) AS avg_duration_ms,
                MAX(duration_ms) AS max_duration_ms
            FROM slow_queries
            {where_clause}
            GROUP BY operation
            ORDER BY executions DESC
        """
        cursor = self._conn.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_recent_slow_queries(
        self, *, limit: int = 100, filters: Dict[str, Any] | None = None
    ) -> List[Dict[str, Any]]:
        """Return the most recent slow query executions."""

        if not self._available_views.get("slow_queries"):
            return []

        where_clause, params = self._build_slow_query_filters(filters)
        query = f"""
            SELECT
                timestamp,
                namespace,
                duration_ms,
                query_hash,
                plan_summary,
                docs_examined,
                docs_returned,
                operation,
                connection_id,
                username
            FROM slow_queries
            {where_clause}
            ORDER BY ts_epoch DESC
            LIMIT ?
        """
        cursor = self._conn.execute(query, [*params, limit])
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def list_slow_query_databases(
        self, *, exclude_system: bool = True
    ) -> List[str]:
        """Return distinct databases found in slow query data."""

        if not self._available_views.get("slow_queries"):
            return []

        rows = self._conn.execute(
            """
            SELECT DISTINCT
                COALESCE(NULLIF(database, ''), 'unknown') AS database
            FROM slow_queries
            WHERE database IS NOT NULL AND TRIM(database) != ''
            ORDER BY database
            """
        ).fetchall()

        databases = [db for (db,) in rows if db]
        if exclude_system:
            databases = [db for db in databases if db not in _SYSTEM_DATABASE_SET]
        return databases

    def list_slow_query_namespaces(
        self,
        *,
        database: Optional[str] = None,
        exclude_system: bool = True,
    ) -> List[str]:
        """Return distinct namespaces for slow query filtering."""

        if not self._available_views.get("slow_queries"):
            return []

        clauses = ["namespace IS NOT NULL", "TRIM(namespace) != ''"]
        params: List[Any] = []

        normalized_db = (database or "").strip()
        if normalized_db and normalized_db.lower() != "all":
            clauses.append("database = ?")
            params.append(normalized_db)
        elif exclude_system:
            placeholders = ", ".join(["?"] * len(_SYSTEM_DATABASES))
            clauses.append(f"database NOT IN ({placeholders})")
            params.extend(_SYSTEM_DATABASES)

        where_clause = " WHERE " + " AND ".join(clauses) if clauses else ""

        query = f"""
            SELECT DISTINCT namespace
            FROM slow_queries
            {where_clause}
            ORDER BY namespace
        """

        rows = self._conn.execute(query, params).fetchall()
        return [namespace for (namespace,) in rows if namespace]

    def fetch_slow_query_executions(
        self,
        *,
        threshold_ms: int = 100,
        database: Optional[str] = None,
        namespace: Optional[str] = None,
        plan_summary: Optional[str] = None,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        page: int = 1,
        per_page: int = 100,
        exclude_system: bool = False,
    ) -> Dict[str, Any]:
        """Return paginated execution records honoring the given filters."""

        if not self._available_views.get("slow_queries"):
            return {"items": [], "total": 0, "page": page, "per_page": per_page}

        per_page = max(int(per_page or 100), 1)
        page = max(int(page or 1), 1)
        offset = (page - 1) * per_page

        where_clause, params = self._compose_slow_query_clause(
            threshold_ms=threshold_ms,
            database=database,
            namespace=namespace,
            plan_summary=plan_summary,
            start_ts=start_ts,
            end_ts=end_ts,
            exclude_system=exclude_system,
        )

        total_row = self._conn.execute(
            f"SELECT COUNT(*) FROM slow_queries {where_clause}", params
        ).fetchone()
        total = int(total_row[0]) if total_row and total_row[0] is not None else 0

        query = f"""
            SELECT
                timestamp,
                ts_epoch,
                COALESCE(NULLIF(database, ''), 'unknown') AS database,
                COALESCE(NULLIF(collection, ''), 'unknown') AS collection,
                COALESCE(NULLIF(namespace, ''), 'unknown.unknown') AS namespace,
                duration_ms,
                docs_examined,
                docs_returned,
                keys_examined,
                COALESCE(NULLIF(query_hash, ''), 'unknown') AS query_hash,
                COALESCE(NULLIF(plan_summary, ''), 'None') AS plan_summary,
                query_text,
                connection_id,
                username,
                file_id,
                file_path,
                line_number,
                line_length,
                operation
            FROM slow_queries
            {where_clause}
            ORDER BY duration_ms DESC, ts_epoch DESC
            LIMIT ? OFFSET ?
        """
        rows = self._conn.execute(query, [*params, per_page, offset]).fetchall()
        columns = [desc[0] for desc in self._conn.description]

        items: List[Dict[str, Any]] = []
        for row in rows:
            record = dict(zip(columns, row))
            raw_timestamp = record.get("timestamp")
            parsed = self._parse_log_timestamp(raw_timestamp)
            record["timestamp_raw"] = raw_timestamp
            if parsed is not None:
                record["timestamp"] = parsed
            record.setdefault("duration", record.get("duration_ms", 0))
            record.setdefault("docsExamined", record.get("docs_examined", 0))
            record.setdefault("nReturned", record.get("docs_returned", 0))
            record.setdefault("keysExamined", record.get("keys_examined", 0))
            items.append(record)

        return {
            "items": items,
            "total": total,
            "page": page,
            "per_page": per_page,
        }

    def fetch_slow_query_patterns(
        self,
        *,
        grouping: str = "pattern_key",
        threshold_ms: int = 100,
        database: Optional[str] = None,
        namespace: Optional[str] = None,
        plan_summary: Optional[str] = None,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        limit: int = 100,
        offset: int = 0,
        exclude_system: bool = False,
    ) -> Dict[str, Any]:
        """Return aggregated pattern statistics for the requested grouping."""

        if not self._available_views.get("slow_queries"):
            return {"items": [], "total": 0}

        limit = max(int(limit or 100), 1)
        offset = max(int(offset or 0), 0)

        where_clause, params = self._compose_slow_query_clause(
            threshold_ms=threshold_ms,
            database=database,
            namespace=namespace,
            plan_summary=plan_summary,
            start_ts=start_ts,
            end_ts=end_ts,
            exclude_system=exclude_system,
        )

        normalized_cte = f"""
            WITH base AS (
                SELECT
                    COALESCE(NULLIF(timestamp, ''), '') AS timestamp,
                    ts_epoch,
                    duration_ms,
                    docs_examined,
                    docs_returned,
                    keys_examined,
                    COALESCE(NULLIF(query_hash, ''), 'unknown') AS query_hash,
                    COALESCE(NULLIF(database, ''), 'unknown') AS database,
                    COALESCE(NULLIF(collection, ''), 'unknown') AS collection,
                    COALESCE(NULLIF(namespace, ''), 'unknown.unknown') AS namespace,
                    COALESCE(NULLIF(plan_summary, ''), 'None') AS plan_summary,
                    query_text,
                    connection_id,
                    username,
                    operation
                FROM slow_queries
                {where_clause}
            )
        """

        grouping_key = (grouping or "pattern_key").lower()
        if grouping_key == "namespace":
            group_fields = "namespace"
        elif grouping_key == "query_hash":
            group_fields = "query_hash"
        else:
            group_fields = "namespace, plan_summary, query_hash, database, collection"

        namespace_expr = (
            "CASE WHEN COUNT(DISTINCT namespace) = 1 "
            "THEN MAX(namespace) ELSE 'MIXED' END AS namespace"
        )
        database_expr = (
            "CASE WHEN COUNT(DISTINCT database) = 1 "
            "THEN MAX(database) ELSE 'MIXED' END AS database"
        )
        collection_expr = (
            "CASE WHEN COUNT(DISTINCT collection) = 1 "
            "THEN MAX(collection) ELSE 'MIXED' END AS collection"
        )
        query_hash_expr = (
            "CASE WHEN COUNT(DISTINCT query_hash) = 1 "
            "THEN MAX(query_hash) ELSE 'MIXED' END AS query_hash"
        )
        plan_summary_expr = (
            "CASE WHEN COUNT(DISTINCT plan_summary) = 1 "
            "THEN MAX(plan_summary) ELSE 'MIXED' END AS plan_summary"
        )

        total_sql = (
            f"{normalized_cte}"
            f"SELECT COUNT(*) FROM (SELECT 1 FROM base GROUP BY {group_fields}) AS counts"
        )
        total_row = self._conn.execute(total_sql, params).fetchone()
        total = int(total_row[0]) if total_row and total_row[0] is not None else 0

        select_sql = (
            f"{normalized_cte}"
            f"SELECT\n"
            f"    {namespace_expr},\n"
            f"    {database_expr},\n"
            f"    {collection_expr},\n"
            f"    {query_hash_expr},\n"
            f"    {plan_summary_expr},\n"
            f"    COUNT(*) AS execution_count,\n"
            f"    AVG(duration_ms) AS avg_duration,\n"
            f"    MIN(duration_ms) AS min_duration,\n"
            f"    MAX(duration_ms) AS max_duration,\n"
            f"    SUM(duration_ms) AS total_duration,\n"
            f"    SUM(docs_examined) AS sum_docs_examined,\n"
            f"    SUM(docs_returned) AS sum_docs_returned,\n"
            f"    SUM(keys_examined) AS sum_keys_examined,\n"
            f"    AVG(docs_examined) AS avg_docs_examined,\n"
            f"    AVG(docs_returned) AS avg_docs_returned,\n"
            f"    AVG(keys_examined) AS avg_keys_examined,\n"
            f"    ARG_MIN(timestamp, ts_epoch) AS first_seen,\n"
            f"    ARG_MAX(timestamp, ts_epoch) AS last_seen,\n"
            f"    ARG_MAX(query_text, duration_ms) AS sample_query\n"
            f"FROM base\n"
            f"GROUP BY {group_fields}\n"
            f"ORDER BY avg_duration DESC, execution_count DESC\n"
            f"LIMIT ? OFFSET ?"
        )

        rows = self._conn.execute(select_sql, [*params, limit, offset]).fetchall()
        columns = [desc[0] for desc in self._conn.description]

        plan_breakdown_map: Dict[tuple[str, str, str, str], List[Dict[str, Any]]] = {}
        if grouping_key == "pattern_key" and rows:
            plan_sql = (
                f"{normalized_cte}"
                "SELECT namespace, database, collection, query_hash, plan_summary, COUNT(*) AS plan_count\n"
                "FROM base\n"
                "GROUP BY namespace, database, collection, query_hash, plan_summary"
            )
            plan_rows = self._conn.execute(plan_sql, params).fetchall()
            for ns, db, coll, qh, plan, count in plan_rows:
                key = (
                    (ns or "unknown.unknown"),
                    (db or "unknown"),
                    (coll or "unknown"),
                    (qh or "unknown"),
                )
                plan_breakdown_map.setdefault(key, []).append(
                    {
                        "plan_summary": plan or "None",
                        "count": int(count or 0),
                    }
                )
            for breakdown in plan_breakdown_map.values():
                breakdown.sort(key=lambda entry: entry.get("count", 0), reverse=True)

        items: List[Dict[str, Any]] = []
        for row in rows:
            record = dict(zip(columns, row))
            first_parsed = self._parse_log_timestamp(record.get("first_seen"))
            last_parsed = self._parse_log_timestamp(record.get("last_seen"))
            if first_parsed is not None:
                record["first_seen"] = first_parsed
            if last_parsed is not None:
                record["last_seen"] = last_parsed

            namespace_val = record.get("namespace") or "unknown.unknown"
            plan_val = record.get("plan_summary") or "None"
            hash_val = record.get("query_hash") or "MIXED"
            record["pattern_key"] = "::".join([namespace_val, plan_val, hash_val])
            if "max_duration" in record and record.get("duration") is None:
                record["duration"] = record.get("max_duration")

            ns_key = namespace_val
            db_key = record.get("database") or "unknown"
            coll_key = record.get("collection") or "unknown"
            qh_key = record.get("query_hash") or "unknown"
            record["plan_breakdown"] = plan_breakdown_map.get(
                (ns_key, db_key, coll_key, qh_key),
                [],
            )

            items.append(record)

        return {"items": items, "total": total}

    def get_authentication_summary(
        self, *, filters: Dict[str, Any] | None = None
    ) -> List[Dict[str, Any]]:
        """Return aggregated authentication metrics."""

        if not self._available_views.get("authentications"):
            return []

        where_clause, params = self._build_auth_filters(filters)
        query = f"""
            SELECT
                COALESCE(result, 'unknown') AS result,
                COALESCE(mechanism, 'unknown') AS mechanism,
                COUNT(*) AS events,
                COUNT(DISTINCT user) AS unique_users,
                COUNT(DISTINCT remote_address) AS unique_hosts
            FROM authentications
            {where_clause}
            GROUP BY result, mechanism
            ORDER BY events DESC
        """
        cursor = self._conn.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_authentication_records(
        self,
        *,
        filters: Dict[str, Any] | None = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """Return raw authentication entries for detailed inspection."""

        if not self._available_views.get("authentications"):
            return []

        where_clause, params = self._build_auth_filters(filters)
        query = f"""
            SELECT
                timestamp,
                ts_epoch,
                user,
                database,
                mechanism,
                result,
                connection_id,
                remote_address,
                app_name,
                error
            FROM authentications
            {where_clause}
            ORDER BY ts_epoch DESC
            LIMIT ?
        """
        cursor = self._conn.execute(query, [*params, limit])
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_connection_activity(
        self, *, filters: Dict[str, Any] | None = None
    ) -> List[Dict[str, Any]]:
        """Summarise connection lifecycle events by remote address."""

        if not self._available_views.get("connections"):
            return []

        where_clause, params = self._build_connection_filters(filters)
        query = f"""
            SELECT
                COALESCE(remote_address, 'unknown') AS remote_address,
                SUM(CASE WHEN event = 'accepted' THEN 1 ELSE 0 END) AS accepted,
                SUM(CASE WHEN event = 'ended' THEN 1 ELSE 0 END) AS ended,
                MAX(connection_count) AS max_connection_count,
                COUNT(DISTINCT connection_id) AS unique_connections
            FROM connections
            {where_clause}
            GROUP BY remote_address
            ORDER BY accepted DESC
        """
        cursor = self._conn.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_trend_summary(
        self,
        *,
        grouping: str = "namespace",
        filters: Dict[str, Any] | None = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Aggregate slow queries by grouping for trend analysis."""

        if not self._available_views.get("slow_queries"):
            return []

        group_expr, alias = self._resolve_grouping(grouping)
        where_clause, params = self._build_slow_query_filters(filters)
        query = f"""
            SELECT
                {group_expr} AS {alias},
                COUNT(*) AS executions,
                AVG(duration_ms) AS avg_duration_ms,
                MAX(duration_ms) AS max_duration_ms,
                SUM(duration_ms) AS total_duration_ms,
                SUM(docs_examined) AS total_docs_examined,
                SUM(docs_returned) AS total_docs_returned
            FROM slow_queries
            {where_clause}
            GROUP BY {alias}
            ORDER BY total_duration_ms DESC
            LIMIT ?
        """
        cursor = self._conn.execute(query, [*params, limit])
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_trend_series(
        self,
        *,
        grouping: str = "namespace",
        filters: Dict[str, Any] | None = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """Return individual executions for scatter/series charts."""

        if not self._available_views.get("slow_queries"):
            return []

        group_expr, alias = self._resolve_grouping(grouping)
        where_clause, params = self._build_slow_query_filters(filters)
        query = f"""
            SELECT
                timestamp,
                ts_epoch,
                duration_ms,
                query_hash,
                namespace,
                plan_summary,
                operation,
                {group_expr} AS {alias}
            FROM slow_queries
            {where_clause}
            ORDER BY ts_epoch ASC
            LIMIT ?
        """
        cursor = self._conn.execute(query, [*params, limit])
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_query_trend_dataset(
        self,
        *,
        grouping: str = "pattern_key",
        filters: Dict[str, Any] | None = None,
        summary_limit: int = 500,
        execution_limit: int = 2000,
        bucket_minutes: int = 0,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Return aggregated pattern stats plus execution samples for trend views."""

        if not self._available_views.get("slow_queries"):
            return {"patterns": [], "executions": []}

        group_expr, alias = self._resolve_grouping(grouping)

        normalized_filters: Dict[str, Any] | None = None
        if filters:
            normalized_filters = {}
            for key, value in filters.items():
                if isinstance(value, str) and value.lower() == "all":
                    continue
                if value is None or value == "":
                    continue
                normalized_filters[key] = value

        where_clause, params = self._build_slow_query_filters(normalized_filters)

        namespace_expr = (
            "CASE WHEN COUNT(DISTINCT namespace) = 1 "
            "THEN MAX(namespace) ELSE 'MIXED' END AS namespace"
        )
        database_expr = (
            "CASE WHEN COUNT(DISTINCT database) = 1 "
            "THEN MAX(database) ELSE 'MIXED' END AS database"
        )
        collection_expr = (
            "CASE WHEN COUNT(DISTINCT collection) = 1 "
            "THEN MAX(collection) ELSE 'MIXED' END AS collection"
        )
        operation_expr = (
            "CASE WHEN COUNT(DISTINCT operation) = 1 "
            "THEN MAX(operation) ELSE 'MIXED' END AS operation"
        )
        plan_expr = (
            "CASE WHEN COUNT(DISTINCT plan_summary) = 1 "
            "THEN MAX(plan_summary) ELSE 'MIXED' END AS plan_summary"
        )
        hash_expr = (
            "CASE WHEN COUNT(DISTINCT query_hash) = 1 "
            "THEN MAX(query_hash) ELSE 'MIXED' END AS query_hash"
        )

        coalesced_cte = f"""
            WITH base AS (
                SELECT
                    COALESCE(NULLIF(timestamp, ''), '') AS timestamp,
                    ts_epoch,
                    duration_ms,
                    docs_examined,
                    docs_returned,
                    COALESCE(NULLIF(namespace, ''), 'unknown.unknown') AS namespace,
                    COALESCE(NULLIF(database, ''), 'unknown') AS database,
                    COALESCE(NULLIF(collection, ''), 'unknown') AS collection,
                    COALESCE(NULLIF(operation, ''), 'unknown') AS operation,
                    COALESCE(NULLIF(plan_summary, ''), 'None') AS plan_summary,
                    COALESCE(NULLIF(query_hash, ''), 'unknown') AS query_hash,
                    query_text
                FROM slow_queries
                {where_clause}
            )
        """

        order_by = None
        order_dir = "DESC"
        if isinstance(filters, dict):
            requested_order = filters.get("order_by")
            requested_dir = filters.get("order_dir")
            if requested_order in {
                "execution_count",
                "avg_duration_ms",
                "max_duration_ms",
                "total_duration_ms",
            }:
                order_by = requested_order
            if isinstance(requested_dir, str) and requested_dir.lower() == "asc":
                order_dir = "ASC"

        if order_by is None:
            order_clause = "ORDER BY total_duration_ms DESC"
        else:
            order_clause = f"ORDER BY {order_by} {order_dir}, total_duration_ms DESC"

        pattern_sql = f"""
            {coalesced_cte}
            SELECT
                {group_expr} AS {alias},
                {namespace_expr},
                {database_expr},
                {collection_expr},
                {operation_expr},
                {plan_expr},
                {hash_expr},
                COUNT(*) AS execution_count,
                AVG(duration_ms) AS avg_duration_ms,
                MIN(duration_ms) AS min_duration_ms,
                MAX(duration_ms) AS max_duration_ms,
                SUM(duration_ms) AS total_duration_ms,
                SUM(docs_examined) AS docs_examined,
                SUM(docs_returned) AS docs_returned,
                ARG_MAX(query_text, duration_ms) AS sample_query
            FROM base
            GROUP BY {alias}
            {order_clause}
            LIMIT ?
        """

        duration_cursor = self._conn.execute(pattern_sql, [*params, summary_limit])
        pattern_columns = [desc[0] for desc in duration_cursor.description]
        duration_rows = duration_cursor.fetchall()

        count_sql = f"""
            {coalesced_cte}
            SELECT
                {group_expr} AS {alias},
                {namespace_expr},
                {database_expr},
                {collection_expr},
                {operation_expr},
                {plan_expr},
                {hash_expr},
                COUNT(*) AS execution_count,
                AVG(duration_ms) AS avg_duration_ms,
                MIN(duration_ms) AS min_duration_ms,
                MAX(duration_ms) AS max_duration_ms,
                SUM(duration_ms) AS total_duration_ms,
                SUM(docs_examined) AS docs_examined,
                SUM(docs_returned) AS docs_returned,
                ARG_MAX(query_text, duration_ms) AS sample_query
            FROM base
            GROUP BY {alias}
            ORDER BY execution_count DESC, total_duration_ms DESC
            LIMIT ?
        """

        count_cursor = self._conn.execute(count_sql, [*params, summary_limit])
        count_rows = count_cursor.fetchall()

        pattern_map: Dict[str, Dict[str, Any]] = {}
        for row in duration_rows:
            record = dict(zip(pattern_columns, row))
            key_val = record.get(alias)
            if key_val is not None:
                pattern_map[str(key_val)] = record

        for row in count_rows:
            record = dict(zip(pattern_columns, row))
            key_val = record.get(alias)
            if key_val is None:
                continue
            key = str(key_val)
            if key not in pattern_map:
                pattern_map[key] = record

        patterns = list(pattern_map.values())
        if order_by is None:
            patterns.sort(key=lambda r: (float(r.get("total_duration_ms") or 0), int(r.get("execution_count") or 0)), reverse=True)
        else:
            def _order_value(record: Dict[str, Any]) -> tuple[float, float, float]:
                primary = record.get(order_by)
                try:
                    primary_value = float(primary) if primary is not None else 0.0
                except (TypeError, ValueError):
                    primary_value = 0.0
                if order_dir == "ASC":
                    primary_value = -primary_value
                total_val = float(record.get("total_duration_ms") or 0.0)
                count_val = float(record.get("execution_count") or 0.0)
                return (primary_value, total_val, count_val)

            patterns.sort(key=_order_value, reverse=True)

        if len(patterns) > summary_limit:
            patterns = patterns[:summary_limit]

        if not patterns:
            return {"patterns": [], "executions": []}

        group_keys = [record.get(alias) for record in patterns if record.get(alias) is not None]
        if not group_keys:
            return {"patterns": patterns, "executions": []}

        key_placeholders = ", ".join(["?"] * len(group_keys))
        exec_where = f"WHERE {alias} IN ({key_placeholders})"
        exec_params = [*params, *group_keys]

        if bucket_minutes and bucket_minutes > 0:
            bucket_seconds = max(int(bucket_minutes), 1) * 60
            execution_sql = f"""
                {coalesced_cte}
                SELECT
                    {group_expr} AS {alias},
                    MIN(ts_epoch) AS bucket_start_epoch,
                    MAX(ts_epoch) AS bucket_end_epoch,
                    AVG(duration_ms) AS avg_duration_ms,
                    MAX(duration_ms) AS max_duration_ms,
                    SUM(duration_ms) AS total_duration_ms,
                    COUNT(*) AS bucket_execution_count,
                    ANY_VALUE(COALESCE(NULLIF(operation, ''), 'unknown')) AS operation,
                    ANY_VALUE(COALESCE(NULLIF(namespace, ''), 'unknown')) AS namespace,
                    ANY_VALUE(COALESCE(NULLIF(database, ''), 'unknown')) AS database,
                    ANY_VALUE(COALESCE(NULLIF(collection, ''), 'unknown')) AS collection,
                    ANY_VALUE(COALESCE(NULLIF(plan_summary, ''), 'None')) AS plan_summary,
                    ANY_VALUE(COALESCE(NULLIF(query_hash, ''), 'unknown')) AS query_hash
                FROM base
                {exec_where}
                GROUP BY {alias}, FLOOR(ts_epoch / {bucket_seconds})
                ORDER BY bucket_start_epoch ASC
            """

            execution_cursor = self._conn.execute(execution_sql, exec_params)
            execution_columns = [desc[0] for desc in execution_cursor.description]
            executions_raw = [dict(zip(execution_columns, row)) for row in execution_cursor.fetchall()]

            executions: List[Dict[str, Any]] = []
            for record in executions_raw:
                start_epoch = record.get("bucket_start_epoch")
                end_epoch = record.get("bucket_end_epoch")
                timestamp_iso: str | None = None
                end_iso: str | None = None
                if isinstance(start_epoch, (int, float)):
                    timestamp_iso = datetime.fromtimestamp(float(start_epoch), tz=timezone.utc).isoformat()
                if isinstance(end_epoch, (int, float)):
                    end_iso = datetime.fromtimestamp(float(end_epoch), tz=timezone.utc).isoformat()

                avg_duration = record.get("avg_duration_ms")
                max_duration = record.get("max_duration_ms")
                total_duration = record.get("total_duration_ms")
                bucket_count = record.get("bucket_execution_count")

                record.update(
                    {
                        "timestamp": timestamp_iso,
                        "bucket_end": end_iso,
                        "duration_ms": avg_duration,
                        "avg_duration_ms": avg_duration,
                        "max_duration_ms": max_duration,
                        "total_duration_ms": total_duration,
                        "bucket_execution_count": bucket_count,
                    }
                )
                executions.append(record)
        else:
            execution_sql = f"""
                {coalesced_cte}
                SELECT
                    {group_expr} AS {alias},
                    timestamp,
                    ts_epoch,
                    duration_ms,
                    COALESCE(NULLIF(operation, ''), 'unknown') AS operation,
                    COALESCE(NULLIF(namespace, ''), 'unknown') AS namespace,
                    COALESCE(NULLIF(database, ''), 'unknown') AS database,
                    COALESCE(NULLIF(collection, ''), 'unknown') AS collection,
                    COALESCE(NULLIF(plan_summary, ''), 'None') AS plan_summary,
                    COALESCE(NULLIF(query_hash, ''), 'unknown') AS query_hash,
                    docs_examined,
                    docs_returned,
                    query_text
                FROM base
                {exec_where}
                ORDER BY ts_epoch ASC
                LIMIT ?
            """

            execution_cursor = self._conn.execute(
                execution_sql, [*exec_params, execution_limit]
            )
            execution_columns = [desc[0] for desc in execution_cursor.description]
            executions = [dict(zip(execution_columns, row)) for row in execution_cursor.fetchall()]

        return {"patterns": patterns, "executions": executions}

    def get_index_suggestions(
        self,
        *,
        limit: int = 50,
        filters: Dict[str, Any] | None = None,
    ) -> List[Dict[str, Any]]:
        """Surface basic index suggestions from COLLSCAN slow queries."""

        if not self._available_views.get("slow_queries"):
            return []

        where_clause, params = self._build_slow_query_filters(filters)
        where_clause = f"{where_clause} AND plan_summary = 'COLLSCAN'" if where_clause else "WHERE plan_summary = 'COLLSCAN'"

        query = f"""
            SELECT
                namespace,
                query_hash,
                AVG(duration_ms) AS avg_duration_ms,
                COUNT(*) AS executions,
                SUM(docs_examined) AS docs_examined,
                ANY_VALUE(query_text) AS sample_query
            FROM slow_queries
            {where_clause}
            GROUP BY namespace, query_hash
            ORDER BY docs_examined DESC
            LIMIT ?
        """
        cursor = self._conn.execute(query, [*params, limit])
        columns = [desc[0] for desc in cursor.description]
        suggestions = []
        for row in cursor.fetchall():
            record = dict(zip(columns, row))
            suggestions.append(
                {
                    "namespace": record.get("namespace"),
                    "query_hash": record.get("query_hash"),
                    "avg_duration_ms": record.get("avg_duration_ms"),
                    "executions": record.get("executions"),
                    "docs_examined": record.get("docs_examined"),
                    "sample_query": record.get("sample_query"),
                    "recommendation": "Consider adding an index to reduce collection scans.",
                }
            )
        return suggestions

    def export_slow_queries(
        self,
        *,
        view: str = "executions",
        limit: int = 5000,
        filters: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        if not self._available_views.get("slow_queries"):
            return {"mode": view, "entries": []}

        where_clause, params = self._build_slow_query_filters(filters)
        if view == "unique_patterns":
            group_expr, alias = self._resolve_grouping("pattern_key")
            query = f"""
                SELECT
                    {alias} AS pattern_key,
                    namespace,
                    COUNT(*) AS executions,
                    AVG(duration_ms) AS avg_duration_ms,
                    MIN(duration_ms) AS min_duration_ms,
                    MAX(duration_ms) AS max_duration_ms,
                    SUM(duration_ms) AS total_duration_ms,
                    SUM(docs_examined) AS docs_examined,
                    SUM(docs_returned) AS docs_returned,
                    ANY_VALUE(query_text) AS sample_query
                FROM slow_queries
                {where_clause}
                GROUP BY {alias}, namespace
                ORDER BY total_duration_ms DESC
                LIMIT ?
            """
            rows = self._conn.execute(query, [*params, limit]).fetchall()
            columns = [desc[0] for desc in self._conn.description]
            return {
                "mode": "unique_patterns",
                "entries": [dict(zip(columns, row)) for row in rows],
            }

        query = f"""
            SELECT
                timestamp,
                ts_epoch,
                namespace,
                query_hash,
                plan_summary,
                duration_ms,
                docs_examined,
                docs_returned,
                keys_examined,
                query_text,
                connection_id,
                username
            FROM slow_queries
            {where_clause}
            ORDER BY ts_epoch DESC
            LIMIT ?
        """
        rows = self._conn.execute(query, [*params, limit]).fetchall()
        columns = [desc[0] for desc in self._conn.description]
        return {
            "mode": "executions",
            "entries": [dict(zip(columns, row)) for row in rows],
        }

    def export_query_analysis(
        self,
        *,
        grouping: str = "namespace",
        limit: int = 500,
        filters: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        summary = self.get_trend_summary(grouping=grouping, filters=filters, limit=limit)
        series = self.get_trend_series(grouping=grouping, filters=filters, limit=limit)
        return {
            "grouping": grouping,
            "summary": summary,
            "series": series,
        }

    def get_dashboard_summary(
        self,
        *,
        limit: int = 10,
        filters: Dict[str, Any] | None = None,
        ip_filter: Optional[str] = None,
        user_filter: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return legacy-compatible dashboard statistics."""

        if filters is None:
            filters = {}

        # Prefer explicit keyword arguments when provided; fall back to filter keys
        if ip_filter is None:
            ip_filter = filters.get("remote_address") or filters.get("ip")  # type: ignore[arg-type]
        if user_filter is None:
            user_filter = filters.get("user")  # type: ignore[arg-type]

        def _maybe_int(value: Any) -> Optional[int]:
            try:
                if value in (None, ""):
                    return None
                return int(value)
            except (TypeError, ValueError):
                return None

        start_ts = _maybe_int(filters.get("start_ts"))
        end_ts = _maybe_int(filters.get("end_ts"))
        database_filter = filters.get("database") if filters.get("database") else None
        namespace_filter = filters.get("namespace") if filters.get("namespace") else None

        slow_queries_available = self._available_views.get("slow_queries", False)
        auth_available = self._available_views.get("authentications", False)
        connections_available = self._available_views.get("connections", False)

        slow_clauses: List[str] = [
            "ts_epoch IS NOT NULL",
            "ts_epoch > 0",
            f"database NOT IN ({', '.join(repr(db) for db in _SYSTEM_DATABASES)})",
        ]
        slow_params: List[Any] = []
        if database_filter:
            slow_clauses.append("database = ?")
            slow_params.append(database_filter)
        if namespace_filter:
            slow_clauses.append("namespace = ?")
            slow_params.append(namespace_filter)
        if start_ts is not None:
            slow_clauses.append("ts_epoch >= ?")
            slow_params.append(start_ts)
        if end_ts is not None:
            slow_clauses.append("ts_epoch <= ?")
            slow_params.append(end_ts)
        slow_where = " WHERE " + " AND ".join(slow_clauses) if slow_clauses else ""

        auth_clauses: List[str] = [
            "ts_epoch IS NOT NULL",
            "ts_epoch > 0",
        ]
        auth_params: List[Any] = []
        if start_ts is not None:
            auth_clauses.append("ts_epoch >= ?")
            auth_params.append(start_ts)
        if end_ts is not None:
            auth_clauses.append("ts_epoch <= ?")
            auth_params.append(end_ts)
        auth_where = " WHERE " + " AND ".join(auth_clauses) if auth_clauses else ""

        top_ips: List[Dict[str, Any]] = []
        top_namespaces: List[Dict[str, Any]] = []
        top_patterns: List[Dict[str, Any]] = []
        top_users: List[Dict[str, Any]] = []
        recent_auths: List[Dict[str, Any]] = []

        slow_queries_count = 0
        unique_namespaces = 0
        unique_databases = 0
        unique_patterns = 0
        avg_duration = 0.0
        performance_issues = 0
        connections_count = 0
        total_authentications = 0
        unique_ips = 0
        unique_users = 0
        auth_methods = 0

        if slow_queries_available:
            basic_sql = f"""
                SELECT
                    COUNT(*) AS total_queries,
                    COUNT(DISTINCT namespace) AS unique_namespaces,
                    COUNT(DISTINCT database) AS unique_databases,
                    COUNT(DISTINCT query_hash) AS unique_patterns,
                    AVG(duration_ms) AS avg_duration,
                    SUM(CASE WHEN plan_summary = 'COLLSCAN' THEN 1 ELSE 0 END) AS collscan_count
                FROM slow_queries
                {slow_where}
            """
            row = self._conn.execute(basic_sql, slow_params).fetchone()
            if row:
                (
                    slow_queries_count,
                    unique_namespaces,
                    unique_databases,
                    unique_patterns,
                    avg_duration,
                    performance_issues,
                ) = row
                avg_duration = float(avg_duration or 0.0)
                performance_issues = int(performance_issues or 0)

            namespace_sql = f"""
                SELECT
                    COALESCE(NULLIF(namespace, ''), 'unknown') AS namespace,
                    COUNT(*) AS query_count,
                    AVG(duration_ms) AS avg_duration,
                    SUM(CASE WHEN plan_summary = 'COLLSCAN' THEN 1 ELSE 0 END) AS collscan_count,
                    SUM(CASE WHEN UPPER(plan_summary) LIKE '%IXSCAN%' THEN 1 ELSE 0 END) AS ixscan_count
                FROM slow_queries
                {slow_where}
                GROUP BY namespace
                ORDER BY query_count DESC
                LIMIT ?
            """
            rows = self._conn.execute(namespace_sql, [*slow_params, limit]).fetchall()
            cols = [desc[0] for desc in self._conn.description]
            top_namespaces = [dict(zip(cols, row)) for row in rows]

            pattern_sql = f"""
                SELECT
                    query_hash,
                    COALESCE(NULLIF(namespace, ''), 'unknown') AS namespace,
                    COUNT(*) AS executions,
                    AVG(duration_ms) AS avg_duration_ms
                FROM slow_queries
                {slow_where}
                GROUP BY query_hash, namespace
                ORDER BY avg_duration_ms DESC
                LIMIT ?
            """
            rows = self._conn.execute(pattern_sql, [*slow_params, limit]).fetchall()
            cols = [desc[0] for desc in self._conn.description]
            top_patterns = [dict(zip(cols, row)) for row in rows]

        if connections_available:
            connections_count = self._conn.execute(
                "SELECT COUNT(*) FROM connections"
            ).fetchone()[0]

        if auth_available:
            # Top IPs (optionally filtered)
            ip_clauses = auth_clauses + [
                "remote_address IS NOT NULL",
                "TRIM(remote_address) != ''",
            ]
            ip_params = list(auth_params)
            if ip_filter:
                ip_clauses.append("LOWER(remote_address) LIKE ?")
                ip_params.append(f"%{ip_filter.lower()}%")
            ip_where = " WHERE " + " AND ".join(ip_clauses)
            ip_sql = f"""
                SELECT
                    remote_address AS ip,
                    COUNT(DISTINCT connection_id) AS connection_count,
                    COUNT(DISTINCT user) AS unique_users,
                    COUNT(*) AS auth_count
                FROM authentications
                {ip_where}
                GROUP BY remote_address
                ORDER BY auth_count DESC
                LIMIT ?
            """
            rows = self._conn.execute(ip_sql, [*ip_params, limit]).fetchall()
            cols = [desc[0] for desc in self._conn.description]
            top_ips = [dict(zip(cols, row)) for row in rows]

            # Top users excluding system accounts
            system_user_list = ", ".join(repr(user.lower()) for user in _SYSTEM_USERS)
            user_clauses = auth_clauses + [
                "COALESCE(TRIM(user), '') != ''",
                f"LOWER(COALESCE(user, '')) NOT IN ({system_user_list})",
            ]
            user_params = list(auth_params)
            if user_filter:
                user_clauses.append("LOWER(user) LIKE ?")
                user_params.append(f"%{user_filter.lower()}%")
            user_where = " WHERE " + " AND ".join(user_clauses)
            user_sql = f"""
                SELECT
                    user,
                    COUNT(DISTINCT connection_id) AS sessions,
                    SUM(CASE WHEN result = 'success' THEN 1 ELSE 0 END) AS success_logins,
                    SUM(CASE WHEN result = 'failure' THEN 1 ELSE 0 END) AS failed_logins,
                    MAX(ts_epoch) AS last_seen_epoch,
                    ARG_MAX(remote_address, ts_epoch) AS last_login_ip,
                    ARG_MAX(connection_id, ts_epoch) AS last_login_conn_id,
                    ARG_MAX(timestamp, ts_epoch) AS last_seen
                FROM authentications
                {user_where}
                GROUP BY user
                ORDER BY (success_logins + failed_logins) DESC
                LIMIT ?
            """
            rows = self._conn.execute(user_sql, [*user_params, limit]).fetchall()
            cols = [desc[0] for desc in self._conn.description]
            top_users = [dict(zip(cols, row)) for row in rows]
            for entry in top_users:
                successes = int(entry.get("success_logins") or 0)
                failures = int(entry.get("failed_logins") or 0)
                entry["total_activity"] = successes + failures

            # Recent authentications (respect date/ip filters)
            recent_clauses = ip_clauses  # reuse same base including optional ip filter
            recent_where = " WHERE " + " AND ".join(recent_clauses)
            recent_sql = f"""
                SELECT timestamp, user, result, mechanism, remote_address
                FROM authentications
                {recent_where}
                ORDER BY ts_epoch DESC
                LIMIT ?
            """
            rows = self._conn.execute(recent_sql, [*ip_params, limit]).fetchall()
            cols = [desc[0] for desc in self._conn.description]
            recent_auths = [dict(zip(cols, row)) for row in rows]

            # Authentication summary statistics
            stats_clauses = auth_clauses + [
                f"LOWER(COALESCE(user, '')) NOT IN ({system_user_list})",
            ]
            stats_params = list(auth_params)
            if user_filter:
                stats_clauses.append("LOWER(user) LIKE ?")
                stats_params.append(f"%{user_filter.lower()}%")
            stats_where = " WHERE " + " AND ".join(stats_clauses)
            stats_sql = f"""
                SELECT
                    COUNT(*) AS total_auths,
                    COUNT(DISTINCT remote_address) AS unique_ips,
                    COUNT(DISTINCT user) AS unique_users,
                    COUNT(DISTINCT mechanism) AS auth_methods
                FROM authentications
                {stats_where}
            """
            stats_row = self._conn.execute(stats_sql, stats_params).fetchone()
            if stats_row:
                total_authentications, unique_ips, unique_users, auth_methods = stats_row

        if not top_ips and self._available_views.get("connections"):
            rows = self._conn.execute(
                """
                SELECT
                    remote_address AS ip,
                    COUNT(*) AS event_count,
                    COUNT(DISTINCT connection_id) AS unique_connections
                FROM connections
                WHERE COALESCE(TRIM(remote_address), '') != ''
                GROUP BY remote_address
                ORDER BY event_count DESC
                LIMIT ?
                """,
                [limit],
            ).fetchall()
            cols = [desc[0] for desc in self._conn.description]
            top_ips = []
            for row in rows:
                entry = dict(zip(cols, row))
                entry["auth_count"] = entry.get("event_count", 0)
                entry.setdefault("unique_users", entry.get("unique_connections", 0))
                top_ips.append(entry)

        if not top_users and slow_queries_available:
            rows = self._conn.execute(
                """
                SELECT
                    COALESCE(NULLIF(username, ''), 'unknown') AS user,
                    COUNT(*) AS executions,
                    COUNT(DISTINCT connection_id) AS unique_connections,
                    MAX(timestamp) AS last_seen
                FROM slow_queries
                WHERE COALESCE(NULLIF(username, ''), '') != ''
                GROUP BY user
                ORDER BY executions DESC
                LIMIT ?
                """,
                [limit],
            ).fetchall()
            cols = [desc[0] for desc in self._conn.description]
            fallback_users = [dict(zip(cols, row)) for row in rows]
            for entry in fallback_users:
                executions = int(entry.get("executions") or 0)
                entry["sessions"] = entry.get("unique_connections") or 0
                entry["success_logins"] = executions
                entry["failed_logins"] = 0
                entry.setdefault("last_login_ip", None)
                entry.setdefault("last_login_conn_id", None)
                entry["total_activity"] = executions
            if fallback_users:
                top_users = fallback_users
                top_user = top_users[0]

        top_ip = top_ips[0] if top_ips else {"ip": "N/A", "auth_count": 0}
        top_namespace = (
            top_namespaces[0]
            if top_namespaces
            else {"namespace": "N/A", "query_count": 0, "avg_duration": 0, "collscan_count": 0, "ixscan_count": 0}
        )
        top_user = (
            top_users[0]
            if top_users
            else {"user": "N/A", "total_activity": 0}
        )

        result = {
            "slow_queries_count": slow_queries_count,
            "avg_duration": round(float(avg_duration or 0.0), 2),
            "unique_namespaces": unique_namespaces,
            "unique_databases": unique_databases,
            "unique_patterns": unique_patterns,
            "performance_issues": performance_issues,
            "top_ip": top_ip,
            "top_ips": top_ips,
            "top_namespace": top_namespace,
            "top_namespaces": top_namespaces,
            "top_user": top_user,
            "top_users": top_users,
            "total_authentications": total_authentications,
            "unique_ips": unique_ips,
            "unique_users": unique_users,
            "auth_methods": auth_methods,
            "connections_count": connections_count,
            "auth_count": total_authentications,
            "namespaces_count": unique_namespaces,
            "top_patterns": top_patterns,
            "recent_authentications": recent_auths,
            # Legacy compatibility fields
            "auth_success_count": 0,
            "auth_failure_count": 0,
            "total_connections": connections_count,
            "connections_by_ip": {},
            "connections_timeline": [],
            "database_access": [],
            "authentications": [],
        }

        return result

    def get_available_date_range(self) -> tuple[Optional[datetime], Optional[datetime]]:
        """Return the overall min/max timestamp across registered datasets."""

        min_epoch: Optional[int] = None
        max_epoch: Optional[int] = None
        min_iso: Optional[str] = None
        max_iso: Optional[str] = None

        for view in ("slow_queries", "connections", "authentications"):
            if not self._available_views.get(view):
                continue
            row = self._conn.execute(
                f"""
                SELECT
                    MIN(ts_epoch) AS min_epoch,
                    ARG_MIN(timestamp, ts_epoch) AS min_timestamp,
                    MAX(ts_epoch) AS max_epoch,
                    ARG_MAX(timestamp, ts_epoch) AS max_timestamp
                FROM {view}
                WHERE ts_epoch IS NOT NULL AND ts_epoch > 0
                """
            ).fetchone()
            if not row:
                continue
            view_min_epoch, view_min_iso, view_max_epoch, view_max_iso = row
            if view_min_epoch is not None and (
                min_epoch is None or view_min_epoch < min_epoch
            ):
                min_epoch = int(view_min_epoch)
                min_iso = view_min_iso
            if view_max_epoch is not None and (
                max_epoch is None or view_max_epoch > max_epoch
            ):
                max_epoch = int(view_max_epoch)
                max_iso = view_max_iso

        if min_epoch is None or max_epoch is None:
            return None, None

        min_dt = self._parse_log_timestamp(min_iso) if min_iso else None
        max_dt = self._parse_log_timestamp(max_iso) if max_iso else None

        if min_dt is None:
            min_dt = datetime.fromtimestamp(min_epoch, tz=timezone.utc)
        if max_dt is None:
            max_dt = datetime.fromtimestamp(max_epoch, tz=timezone.utc)

        return min_dt, max_dt

    def get_date_offset_map(self) -> Dict[str, str]:
        """Return mapping of ISO dates to observed timezone offsets."""

        offsets: Dict[str, str] = {}

        for view in ("slow_queries", "connections", "authentications"):
            if not self._available_views.get(view):
                continue
            rows = self._conn.execute(
                f"""
                SELECT DISTINCT
                    substr(timestamp, 1, 10) AS day,
                    COALESCE(regexp_extract(timestamp, '([+-][0-9]{{2}}:[0-9]{{2}}|Z)$'), 'Z') AS offset
                FROM {view}
                WHERE timestamp IS NOT NULL AND TRIM(timestamp) != ''
                """
            ).fetchall()
            for day, offset in rows:
                if not day:
                    continue
                offsets.setdefault(day, offset or 'Z')

        return offsets

    def resolve_timestamp_for_local(
        self,
        local_value: str,
        *,
        direction: str = "start",
        hint: Optional[str] = None,
    ) -> Optional[tuple[str, int]]:
        """Return the best matching ISO timestamp (and epoch) for a local value."""

        if not local_value or len(local_value) < 16:
            return None

        normalized = local_value.strip()
        if len(normalized) < 19:
            normalized = normalized[:16] + ":00"

        minute_prefix = normalized[:16]
        comparator = ">=" if direction != "end" else "<="
        order_keyword = "ASC" if direction != "end" else "DESC"

        best: tuple[int, str] | None = None

        def _maybe_update(candidate: Optional[tuple[Any, Any]]) -> None:
            nonlocal best
            if not candidate:
                return
            ts_epoch, timestamp = candidate
            if timestamp is None or ts_epoch is None:
                return
            try:
                ts_epoch_int = int(ts_epoch)
            except (TypeError, ValueError):
                return
            if best is None:
                best = (ts_epoch_int, str(timestamp))
                return
            if direction == "end":
                if ts_epoch_int > best[0]:
                    best = (ts_epoch_int, str(timestamp))
            else:
                if ts_epoch_int < best[0]:
                    best = (ts_epoch_int, str(timestamp))

        for view in ("slow_queries", "connections", "authentications"):
            if not self._available_views.get(view):
                continue
            exact = self._conn.execute(
                f"""
                SELECT ts_epoch, timestamp
                FROM {view}
                WHERE substr(timestamp, 1, 16) = ?
                  AND ts_epoch IS NOT NULL
                ORDER BY ts_epoch {order_keyword}
                LIMIT 1
                """,
                [minute_prefix],
            ).fetchone()
            _maybe_update(exact)

        if best is not None:
            return best[1], best[0]

        normalized_seconds = normalized[:19]
        for view in ("slow_queries", "connections", "authentications"):
            if not self._available_views.get(view):
                continue
            fallback = self._conn.execute(
                f"""
                SELECT ts_epoch, timestamp
                FROM {view}
                WHERE substr(timestamp, 1, 19) {comparator} ?
                  AND ts_epoch IS NOT NULL
                ORDER BY substr(timestamp, 1, 19) {order_keyword}, ts_epoch {order_keyword}
                LIMIT 1
                """,
                [normalized_seconds],
            ).fetchone()
            _maybe_update(fallback)

        if best is not None:
            return best[1], best[0]

        if hint:
            parsed = self._parse_log_timestamp(hint)
            if parsed is not None:
                try:
                    return hint, int(parsed.timestamp())
                except (OverflowError, OSError, ValueError):
                    return hint, 0

        return None

    @staticmethod
    def _parse_log_timestamp(value: Any) -> Optional[datetime]:
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        if not isinstance(value, str):
            return None
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    def get_manifest_info(self) -> Dict[str, Any] | None:
        """Return the parsed dataset manifest if present."""

        manifest_path = self.dataset_root / "manifest.json"
        manifest = load_manifest(manifest_path)
        if manifest is None:
            return None
        return manifest

    def get_workload_summary(
        self,
        *,
        limit: int = 10,
        filters: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """Aggregate slow query metrics to approximate workload statistics."""

        if not self._available_views.get("slow_queries"):
            return {
                "top_bytes_read": [],
                "top_bytes_written": [],
                "top_cpu_usage": [],
                "top_io_time": [],
                "stats": {},
            }

        where_clause, params = self._build_slow_query_filters(filters)

        def run(query: str) -> List[Dict[str, Any]]:
            cursor = self._conn.execute(query, params + [limit])
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

        top_docs_examined = run(
            f"""
            SELECT
                COALESCE(NULLIF(database, ''), 'unknown') AS database,
                COALESCE(NULLIF(collection, ''), 'unknown') AS collection,
                COALESCE(NULLIF(plan_summary, ''), 'None') AS plan_summary,
                SUM(docs_examined) AS total_docs_examined,
                COUNT(*) AS executions,
                AVG(duration_ms) AS avg_duration_ms
            FROM slow_queries
            {where_clause}
            GROUP BY database, collection, plan_summary
            ORDER BY total_docs_examined DESC
            LIMIT ?
            """
        )

        top_docs_returned = run(
            f"""
            SELECT
                COALESCE(NULLIF(database, ''), 'unknown') AS database,
                COALESCE(NULLIF(collection, ''), 'unknown') AS collection,
                COALESCE(NULLIF(plan_summary, ''), 'None') AS plan_summary,
                SUM(docs_returned) AS total_docs_returned,
                COUNT(*) AS executions,
                AVG(duration_ms) AS avg_duration_ms
            FROM slow_queries
            {where_clause}
            GROUP BY database, collection, plan_summary
            ORDER BY total_docs_returned DESC
            LIMIT ?
            """
        )

        top_duration = run(
            f"""
            SELECT
                COALESCE(NULLIF(database, ''), 'unknown') AS database,
                COALESCE(NULLIF(collection, ''), 'unknown') AS collection,
                COALESCE(NULLIF(plan_summary, ''), 'None') AS plan_summary,
                SUM(duration_ms) AS total_duration_ms,
                AVG(duration_ms) AS avg_duration_ms,
                COUNT(*) AS executions
            FROM slow_queries
            {where_clause}
            GROUP BY database, collection, plan_summary
            ORDER BY total_duration_ms DESC
            LIMIT ?
            """
        )

        top_io_time = run(
            f"""
            SELECT
                COALESCE(NULLIF(database, ''), 'unknown') AS database,
                COALESCE(NULLIF(collection, ''), 'unknown') AS collection,
                COALESCE(NULLIF(plan_summary, ''), 'None') AS plan_summary,
                SUM(keys_examined) AS total_keys_examined,
                SUM(docs_examined) AS total_docs_examined,
                COUNT(*) AS executions,
                AVG(duration_ms) AS avg_duration_ms
            FROM slow_queries
            {where_clause}
            GROUP BY database, collection, plan_summary
            ORDER BY total_keys_examined DESC
            LIMIT ?
            """
        )

        stats_row = self._conn.execute(
            f"""
            SELECT
                COUNT(*) AS executions,
                SUM(duration_ms) AS total_duration_ms,
                SUM(docs_examined) AS total_docs_examined,
                SUM(docs_returned) AS total_docs_returned,
                SUM(keys_examined) AS total_keys_examined
            FROM slow_queries
            {where_clause}
            """,
            params,
        ).fetchone()

        stats = {
            "executions": stats_row[0] if stats_row else 0,
            "total_duration_ms": stats_row[1] if stats_row else 0,
            "total_docs_examined": stats_row[2] if stats_row else 0,
            "total_docs_returned": stats_row[3] if stats_row else 0,
            "total_keys_examined": stats_row[4] if stats_row else 0,
        }

        return {
            "top_docs_examined": top_docs_examined,
            "top_docs_returned": top_docs_returned,
            "top_duration": top_duration,
            "top_io_time": top_io_time,
            "stats": stats,
        }

    def list_ingests(self) -> List[Dict[str, Any]]:
        """Return manifest ingests (empty if manifest missing)."""

        manifest = self.get_manifest_info()
        if not manifest:
            return []
        return list(manifest.get("ingests", []))

    def search_slow_queries(
        self,
        *,
        text: str,
        limit: int = 200,
        filters: Dict[str, Any] | None = None,
    ) -> List[Dict[str, Any]]:
        """Perform case-insensitive search across slow query text/namespace."""

        if not text or not self._available_views.get("slow_queries"):
            return []

        where_clause, params = self._build_slow_query_filters(filters)
        # Append search condition
        pattern = f"%{text.lower()}%"
        search_clause = "lower(query_text) LIKE ? OR lower(namespace) LIKE ?"
        if where_clause:
            where_clause = f"{where_clause} AND ({search_clause})"
        else:
            where_clause = f"WHERE {search_clause}"

        query = f"""
            SELECT
                timestamp,
                namespace,
                duration_ms,
                query_hash,
                plan_summary,
                docs_examined,
                docs_returned,
                operation,
                connection_id,
                username,
                query_text
            FROM slow_queries
            {where_clause}
            ORDER BY ts_epoch DESC
            LIMIT ?
        """
        cursor = self._conn.execute(query, [*params, pattern, pattern, limit])
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_query_examples(
        self, query_hash: str, *, limit: int = 5
    ) -> List[Dict[str, Any]]:
        """Fetch representative raw log lines for a query hash."""

        if not self._available_views.get("query_offsets"):
            return []

        sql = """
            SELECT
                file_id,
                file_offset,
                line_length,
                line_number,
                timestamp,
                ts_epoch,
                database,
                collection
            FROM query_offsets
            WHERE query_hash = ?
            ORDER BY ts_epoch DESC
            LIMIT ?
        """
        rows = self._conn.execute(sql, [query_hash, limit]).fetchall()
        if not rows:
            return []

        columns = [desc[0] for desc in self._conn.description]
        result = []
        for row in rows:
            record = dict(zip(columns, row))
            raw = self._read_raw_log(
                record["file_id"], record["file_offset"], record["line_length"],
            )
            record["raw"] = raw
            result.append(record)
        return result

    # ------------------------------------------------------------------
    # Utility helpers

    def explain_query(self, sql: str) -> str:
        """Return DuckDB's EXPLAIN output for *sql* (diagnostics)."""

        plan = self._conn.execute(f"EXPLAIN {sql}").fetchall()
        return "\n".join(row[0] for row in plan)

    # ------------------------------------------------------------------
    # Filter construction helpers

    def _build_slow_query_filters(
        self, filters: Dict[str, Any] | None
    ) -> tuple[str, List[Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if not filters:
            return "", params

        db = filters.get("database") if isinstance(filters, dict) else None
        exclude_system = bool(filters.get("exclude_system")) if isinstance(filters, dict) else False
        if db:
            clauses.append("database = ?")
            params.append(db)
        elif exclude_system:
            placeholders = ", ".join(["?"] * len(_SYSTEM_DATABASES))
            clauses.append(f"database NOT IN ({placeholders})")
            params.extend(_SYSTEM_DATABASES)

        namespace = filters.get("namespace") if isinstance(filters, dict) else None
        if namespace:
            clauses.append("namespace = ?")
            params.append(namespace)

        start_ts = filters.get("start_ts") if isinstance(filters, dict) else None
        if start_ts is not None:
            clauses.append("ts_epoch >= ?")
            params.append(int(start_ts))

        end_ts = filters.get("end_ts") if isinstance(filters, dict) else None
        if end_ts is not None:
            clauses.append("ts_epoch <= ?")
            params.append(int(end_ts))

        if not clauses:
            return "", params
        return "WHERE " + " AND ".join(clauses), params

    def analyze_slow_query_patterns(
        self,
        *,
        grouping: str = "pattern_key",
        threshold_ms: Optional[int] = None,
        database: Optional[str] = None,
        namespace: Optional[str] = None,
        plan_summary: Optional[str] = None,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        exclude_system: bool = True,
        page: int = 1,
        per_page: int = 100,
        order_by: str = "total_duration_ms",
        order_dir: str = "desc",
    ) -> Dict[str, Any]:
        """Aggregate slow query execution patterns for analysis page."""

        if not self._available_views.get("slow_queries"):
            return {
                "items": [],
                "total_groups": 0,
                "total_executions": 0,
                "avg_duration_ms": 0.0,
                "high_priority_count": 0,
            }

        grouping_normalized = (grouping or "pattern_key").lower()
        group_expr, alias = self._resolve_grouping(grouping_normalized)

        where_clause, params = self._compose_slow_query_clause(
            threshold_ms=threshold_ms,
            database=database,
            namespace=namespace,
            plan_summary=plan_summary,
            start_ts=start_ts,
            end_ts=end_ts,
            exclude_system=exclude_system,
        )

        base_cte = f"""
            WITH base AS (
                SELECT
                    COALESCE(NULLIF(timestamp, ''), '') AS timestamp,
                    ts_epoch,
                    duration_ms,
                    docs_examined,
                    docs_returned,
                    keys_examined,
                    COALESCE(NULLIF(query_hash, ''), 'unknown') AS query_hash,
                    COALESCE(NULLIF(database, ''), 'unknown') AS database,
                    COALESCE(NULLIF(collection, ''), 'unknown') AS collection,
                    COALESCE(NULLIF(namespace, ''), 'unknown.unknown') AS namespace,
                    COALESCE(NULLIF(plan_summary, ''), 'None') AS plan_summary,
                    COALESCE(NULLIF(operation, ''), 'unknown') AS operation,
                    COALESCE(query_text, '') AS query_text
                FROM slow_queries
                {where_clause}
            )
        """

        if grouping_normalized == "namespace":
            group_fields = "namespace"
        elif grouping_normalized == "query_hash":
            group_fields = "query_hash"
        else:
            group_fields = "namespace, plan_summary, query_hash, database, collection"

        namespace_expr = (
            "CASE WHEN COUNT(DISTINCT namespace) = 1 "
            "THEN MAX(namespace) ELSE 'MIXED' END AS namespace"
        )
        database_expr = (
            "CASE WHEN COUNT(DISTINCT database) = 1 "
            "THEN MAX(database) ELSE 'MIXED' END AS database"
        )
        collection_expr = (
            "CASE WHEN COUNT(DISTINCT collection) = 1 "
            "THEN MAX(collection) ELSE 'MIXED' END AS collection"
        )
        plan_expr = (
            "CASE WHEN COUNT(DISTINCT plan_summary) = 1 "
            "THEN MAX(plan_summary) ELSE 'MIXED' END AS plan_summary"
        )
        hash_expr = (
            "CASE WHEN COUNT(DISTINCT query_hash) = 1 "
            "THEN MAX(query_hash) ELSE 'MIXED' END AS query_hash"
        )
        operation_expr = (
            "CASE WHEN COUNT(DISTINCT operation) = 1 "
            "THEN MAX(operation) ELSE 'mixed' END AS operation"
        )

        grouped_cte = f"""
            {base_cte},
            grouped AS (
                SELECT
                    {group_expr} AS {alias},
                    {namespace_expr},
                    {database_expr},
                    {collection_expr},
                    {plan_expr},
                    {hash_expr},
                    {operation_expr},
                    COUNT(*) AS execution_count,
                    AVG(duration_ms) AS avg_duration_ms,
                    MIN(duration_ms) AS min_duration_ms,
                    MAX(duration_ms) AS max_duration_ms,
                    MEDIAN(duration_ms) AS median_duration_ms,
                    SUM(duration_ms) AS total_duration_ms,
                    SUM(docs_examined) AS total_docs_examined,
                    SUM(docs_returned) AS total_docs_returned,
                    SUM(keys_examined) AS total_keys_examined,
                    AVG(docs_examined) AS avg_docs_examined,
                    MIN(timestamp) AS first_seen_raw,
                    MAX(timestamp) AS last_seen_raw,
                    ARG_MAX(timestamp, duration_ms) AS slowest_timestamp_raw,
                    ARG_MAX(query_text, duration_ms) AS slowest_query_full,
                    ARG_MAX(query_text, duration_ms) AS sample_query,
                    CASE
                        WHEN SUM(docs_examined) > 0 THEN (SUM(docs_returned) * 100.0) / SUM(docs_examined)
                        ELSE 0
                    END AS selectivity_pct,
                    CASE
                        WHEN SUM(docs_examined) > 0 THEN (SUM(keys_examined) * 100.0) / SUM(docs_examined)
                        ELSE 0
                    END AS index_efficiency_pct,
                    CASE
                        WHEN AVG(duration_ms) > 1000 OR (CASE WHEN SUM(docs_examined) > 0 THEN (SUM(docs_returned) * 100.0) / SUM(docs_examined) ELSE 0 END) < 10
                            THEN 'high'
                        WHEN AVG(duration_ms) > 100 OR (CASE WHEN SUM(docs_examined) > 0 THEN (SUM(docs_returned) * 100.0) / SUM(docs_examined) ELSE 0 END) < 50
                            THEN 'medium'
                        ELSE 'low'
                    END AS optimization_potential
                FROM base
                GROUP BY {group_fields}
            )
        """

        order_map = {
            "execution_count": "execution_count",
            "avg_duration_ms": "avg_duration_ms",
            "max_duration_ms": "max_duration_ms",
            "total_duration_ms": "total_duration_ms",
        }
        order_column = order_map.get(order_by, "total_duration_ms")
        order_direction = "ASC" if order_dir.lower() == "asc" else "DESC"
        order_clause = f"ORDER BY {order_column} {order_direction}, total_duration_ms DESC"

        page = max(int(page or 1), 1)
        per_page = max(int(per_page or 100), 1)
        offset = (page - 1) * per_page

        items_sql = f"""
            {grouped_cte}
            SELECT *
            FROM grouped
            {order_clause}
            LIMIT ? OFFSET ?
        """

        cursor = self._conn.execute(items_sql, [*params, per_page, offset])
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

        total_groups_sql = f"""
            {grouped_cte}
            SELECT COUNT(*) FROM grouped
        """
        total_groups = int(
            self._conn.execute(total_groups_sql, params).fetchone()[0]
        )

        stats_sql = f"""
            {base_cte}
            SELECT COUNT(*) AS total_exec, AVG(duration_ms) AS avg_duration
            FROM base
        """
        total_exec_row = self._conn.execute(stats_sql, params).fetchone()
        total_executions = int(total_exec_row[0]) if total_exec_row and total_exec_row[0] is not None else 0
        avg_duration_ms = (
            float(total_exec_row[1])
            if total_exec_row
            and total_exec_row[1] is not None
            else 0.0
        )

        high_priority_sql = f"""
            {grouped_cte}
            SELECT COUNT(*) FROM grouped WHERE optimization_potential = 'high'
        """
        high_priority_count = int(
            self._conn.execute(high_priority_sql, params).fetchone()[0]
        )

        parsed_items: List[Dict[str, Any]] = []
        for entry in rows:
            selectivity = float(entry.get("selectivity_pct", 0) or 0.0)
            avg_duration = float(entry.get("avg_duration_ms", 0) or 0.0)
            execution_count = int(entry.get("execution_count", 0) or 0)
            total_docs = entry.get("total_docs_examined")
            total_returned = entry.get("total_docs_returned")
            total_keys = entry.get("total_keys_examined")

            def _as_int(value: object) -> int:
                if value is None:
                    return 0
                if isinstance(value, Decimal):
                    return int(value)
                return int(value)

            total_docs_val = _as_int(total_docs)
            total_returned_val = _as_int(total_returned)
            total_keys_val = _as_int(total_keys)

            avg_docs_examined = float(entry.get("avg_docs_examined", 0) or 0.0)
            avg_index_efficiency = float(entry.get("index_efficiency_pct", 0) or 0.0)

            if isinstance(avg_docs_examined, Decimal):
                avg_docs_examined = float(avg_docs_examined)
            if isinstance(avg_index_efficiency, Decimal):
                avg_index_efficiency = float(avg_index_efficiency)

            min_duration = float(entry.get("min_duration_ms", 0) or 0.0)
            max_duration = float(entry.get("max_duration_ms", 0) or 0.0)

            complexity_score = min(
                100.0,
                (avg_duration / 10.0)
                + (100.0 - selectivity)
                + (execution_count / 10.0),
            )

            first_seen = self._parse_log_timestamp(entry.get("first_seen_raw"))
            last_seen = self._parse_log_timestamp(entry.get("last_seen_raw"))
            slowest_ts = self._parse_log_timestamp(entry.get("slowest_timestamp_raw"))

            query_hash_value = entry.get("query_hash") or "unknown"
            plan_summary_value = entry.get("plan_summary") or "None"
            namespace_value = entry.get("namespace") or "unknown.unknown"
            database_value = entry.get("database") or "unknown"
            collection_value = entry.get("collection") or "unknown"

            operation_value = entry.get("operation") or "unknown"
            query_type_value = str(operation_value).upper() if operation_value else "UNKNOWN"
            if query_type_value not in {"MIXED", "UNKNOWN"}:
                query_type_value = query_type_value.upper()

            parsed_items.append(
                {
                    alias: entry.get(alias),
                    "pattern_key": entry.get(alias),
                    "namespace": namespace_value,
                    "database": database_value,
                    "collection": collection_value,
                    "plan_summary": plan_summary_value,
                    "query_hash": query_hash_value,
                    "query_type": query_type_value or "unknown",
                    "total_count": execution_count,
                    "avg_duration": avg_duration,
                    "min_duration": min_duration,
                    "max_duration": max_duration,
                    "median_duration": float(entry.get("median_duration_ms", 0) or 0.0),
                    "total_duration": float(entry.get("total_duration_ms", 0) or 0.0),
                    "total_docs_examined": total_docs_val,
                    "total_returned": total_returned_val,
                    "total_keys_examined": total_keys_val,
                    "avg_docs_examined": avg_docs_examined,
                    "avg_selectivity": selectivity,
                    "avg_index_efficiency": avg_index_efficiency,
                    "optimization_potential": entry.get("optimization_potential", "low"),
                    "complexity_score": complexity_score,
                    "first_seen": first_seen,
                    "last_seen": last_seen,
                    "slowest_execution_timestamp": slowest_ts,
                    "slowest_query_full": entry.get("slowest_query_full", ""),
                    "sample_query": entry.get("sample_query", ""),
                    "is_estimated": False,
                }
            )

        return {
            "items": parsed_items,
            "total_groups": total_groups,
            "total_executions": total_executions,
            "avg_duration_ms": avg_duration_ms,
            "high_priority_count": high_priority_count,
        }

    def generate_index_suggestions(
        self,
        *,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        exclude_system: bool = True,
        database: Optional[str] = None,
        namespace: Optional[str] = None,
        limit_per_collection: int = 10,
    ) -> Dict[str, Any]:
        """Produce index suggestions using COLLSCAN/IXSCAN slow query data."""

        if not self._available_views.get("slow_queries"):
            return {
                "collections": {},
                "top_suggestions": [],
                "summary": {
                    "total_collscan_queries": 0,
                    "total_suggestions": 0,
                    "avg_docs_examined": 0,
                },
            }

        where_clause, params = self._compose_slow_query_clause(
            threshold_ms=None,
            database=database,
            namespace=namespace,
            plan_summary=None,
            start_ts=start_ts,
            end_ts=end_ts,
            exclude_system=exclude_system,
        )

        plan_filter = (
            "(UPPER(COALESCE(plan_summary, '')) LIKE 'COLLSCAN%' "
            "OR UPPER(COALESCE(plan_summary, '')) LIKE 'IXSCAN%')"
        )
        if where_clause:
            query_where = f"{where_clause} AND {plan_filter}"
        else:
            query_where = f"WHERE {plan_filter}"

        sql = f"""
            SELECT
                namespace,
                database,
                collection,
                COALESCE(NULLIF(plan_summary, ''), 'None') AS plan_summary,
                duration_ms,
                docs_examined,
                docs_returned,
                COALESCE(query_text, '') AS query_text,
                COALESCE(NULLIF(query_hash, ''), 'unknown') AS query_hash,
                timestamp,
                ts_epoch
            FROM slow_queries
            {query_where}
        """

        cursor = self._conn.execute(sql, params)
        columns = [desc[0] for desc in cursor.description]
        records: List[Dict[str, Any]] = []
        for row in cursor.fetchall():
            entry = dict(zip(columns, row))
            entry["duration_ms"] = float(entry.get("duration_ms") or 0.0)
            entry["docs_examined"] = int(entry.get("docs_examined") or 0)
            entry["docs_returned"] = int(entry.get("docs_returned") or 0)
            entry["timestamp"] = self._parse_log_timestamp(entry.get("timestamp"))
            records.append(entry)

        if not records:
            return {
                "collections": {},
                "top_suggestions": [],
                "summary": {
                    "total_collscan_queries": 0,
                    "total_suggestions": 0,
                    "avg_docs_examined": 0,
                },
            }

        pattern_analysis = self.analyze_slow_query_patterns(
            grouping="pattern_key",
            start_ts=start_ts,
            end_ts=end_ts,
            exclude_system=exclude_system,
            page=1,
            per_page=min(5000, max(1, len(records))),
            order_by="total_duration_ms",
            order_dir="desc",
        )
        total_groups = pattern_analysis.get("total_groups", 0)
        if total_groups > len(pattern_analysis.get("items", [])) and total_groups <= 20000:
            pattern_analysis = self.analyze_slow_query_patterns(
                grouping="pattern_key",
                start_ts=start_ts,
                end_ts=end_ts,
                exclude_system=exclude_system,
                page=1,
                per_page=total_groups,
                order_by="total_duration_ms",
                order_dir="desc",
            )

        pattern_totals = {
            item.get("pattern_key", ""): {
                "total_count": item.get("total_count", 0),
                "avg_duration": item.get("avg_duration", 0),
            }
            for item in pattern_analysis.get("items", [])
        }

        suggestions = generate_index_suggestions(
            records,
            pattern_totals=pattern_totals,
            limit_per_collection=limit_per_collection,
        )

        return suggestions

    def _compose_slow_query_clause(
        self,
        *,
        threshold_ms: Optional[int] = None,
        database: Optional[str] = None,
        namespace: Optional[str] = None,
        plan_summary: Optional[str] = None,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        exclude_system: bool = False,
    ) -> tuple[str, List[Any]]:
        clauses: List[str] = []
        params: List[Any] = []

        if threshold_ms is not None and threshold_ms > 0:
            clauses.append("duration_ms >= ?")
            params.append(int(threshold_ms))

        normalized_db = (database or "").strip()
        if normalized_db and normalized_db.lower() != "all":
            clauses.append("database = ?")
            params.append(normalized_db)
        elif exclude_system:
            placeholders = ", ".join(["?"] * len(_SYSTEM_DATABASES))
            clauses.append(f"database NOT IN ({placeholders})")
            params.extend(_SYSTEM_DATABASES)

        normalized_namespace = (namespace or "").strip()
        if normalized_namespace and normalized_namespace.lower() != "all":
            clauses.append("namespace = ?")
            params.append(normalized_namespace)

        normalized_plan = (plan_summary or "").strip()
        if normalized_plan and normalized_plan.lower() != "all":
            plan_upper = normalized_plan.upper()
            if plan_upper == "OTHER":
                clauses.append(
                    "(UPPER(COALESCE(plan_summary, '')) NOT IN ('COLLSCAN', 'IXSCAN') "
                    "OR COALESCE(plan_summary, '') = '')"
                )
            else:
                if plan_upper in {"COLLSCAN", "IXSCAN"}:
                    clauses.append("UPPER(COALESCE(plan_summary, '')) LIKE ?")
                    params.append(f"{plan_upper}%")
                else:
                    clauses.append("UPPER(COALESCE(plan_summary, '')) = ?")
                    params.append(plan_upper)

        if start_ts is not None:
            clauses.append("ts_epoch >= ?")
            params.append(int(start_ts))

        if end_ts is not None:
            clauses.append("ts_epoch <= ?")
            params.append(int(end_ts))

        clauses.append("ts_epoch IS NOT NULL")
        clauses.append("ts_epoch > 0")

        where_clause = " WHERE " + " AND ".join(clauses) if clauses else ""
        return where_clause, params

    def _build_auth_filters(
        self, filters: Dict[str, Any] | None
    ) -> tuple[str, List[Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if not filters:
            return "", params

        db = filters.get("database") if isinstance(filters, dict) else None
        if db:
            clauses.append("database = ?")
            params.append(db)

        user = filters.get("user") if isinstance(filters, dict) else None
        if user:
            clauses.append("user = ?")
            params.append(user)

        mechanism = filters.get("mechanism") if isinstance(filters, dict) else None
        if mechanism:
            clauses.append("mechanism = ?")
            params.append(mechanism)

        result = filters.get("result") if isinstance(filters, dict) else None
        if result:
            clauses.append("result = ?")
            params.append(result)

        remote = filters.get("remote_address") if isinstance(filters, dict) else None
        if remote:
            clauses.append("remote_address = ?")
            params.append(remote)

        start_ts = filters.get("start_ts") if isinstance(filters, dict) else None
        if start_ts is not None:
            clauses.append("ts_epoch >= ?")
            params.append(int(start_ts))

        end_ts = filters.get("end_ts") if isinstance(filters, dict) else None
        if end_ts is not None:
            clauses.append("ts_epoch <= ?")
            params.append(int(end_ts))

        if not clauses:
            return "", params
        return "WHERE " + " AND ".join(clauses), params

    def _build_connection_filters(
        self, filters: Dict[str, Any] | None
    ) -> tuple[str, List[Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if not filters:
            return "", params

        start_ts = filters.get("start_ts") if isinstance(filters, dict) else None
        if start_ts is not None:
            clauses.append("ts_epoch >= ?")
            params.append(int(start_ts))

        end_ts = filters.get("end_ts") if isinstance(filters, dict) else None
        if end_ts is not None:
            clauses.append("ts_epoch <= ?")
            params.append(int(end_ts))

        app_name = filters.get("app_name") if isinstance(filters, dict) else None
        if app_name:
            clauses.append("app_name = ?")
            params.append(app_name)

        if not clauses:
            return "", params
        return "WHERE " + " AND ".join(clauses), params

    def _load_file_map(self) -> Dict[int, str]:
        if self._file_map_cache is not None:
            return self._file_map_cache
        file_map_path = self.dataset_root / "index" / "file_map.json"
        mapping = load_file_map(file_map_path)
        self._file_map_cache = mapping
        return mapping

    def _read_raw_log(self, file_id: int, offset: int, length: int) -> str | None:
        mapping = self._load_file_map()
        file_path = mapping.get(file_id)
        if not file_path:
            LOGGER.warning("Missing source path for file_id %s", file_id)
            return None

        path = Path(file_path)
        if not path.exists():
            LOGGER.warning("Source log missing at %s", path)
            return None

        try:
            with path.open("r", encoding="utf-8", errors="ignore") as handle:
                handle.seek(int(offset))
                return handle.read(int(length)).rstrip("\n")
        except OSError as exc:
            LOGGER.warning("Failed to read raw log from %s: %s", path, exc)
            return None

    def _resolve_grouping(self, grouping: str) -> tuple[str, str]:
        grouping = (grouping or "namespace").lower()
        if grouping == "pattern_key":
            expr = "CONCAT_WS('::', namespace, plan_summary, query_hash)"
            return expr, "pattern_key"
        if grouping == "query_hash":
            return "query_hash", "query_hash"
        return "namespace", "namespace"


__all__ = ["DuckDBService"]
