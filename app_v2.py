"""Flask application entrypoint for the v2 Parquet/DuckDB backend."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal
from collections import OrderedDict
from types import SimpleNamespace
import math
import re
import json
from pathlib import Path
from typing import Mapping, List, Dict, Any

import shutil
import tempfile

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify

from log_analyzer_v2.analytics import DuckDBService
from log_analyzer_v2.config import settings
from log_analyzer_v2.web import slowq_blueprint
from log_analyzer_v2.ingest.uploader import process_uploads
from log_analyzer_v2.storage.manifest import load_manifest
from log_analyzer_v2.runtime.status import get_status as get_processing_status
from log_analyzer_v2.current_op.analyzer import (
    analyze_current_op as analyze_current_op_v2,
)

SLOWQ_ALLOWED_PAGE_SIZES = (100, 250, 500, 1000)
SLOWQ_ALL_PAGE_LIMIT = 5000
SLOWQ_EXPORT_DEFAULT_LIMIT = 10_000
SLOWQ_EXPORT_MAX_LIMIT = 50_000
SYSTEM_USERS = ("__system", "admin", "root", "mongodb", "system")


def create_app() -> Flask:
    app = Flask(__name__)
    app.config.setdefault("SLOWQ_DATASET_ROOT", str(settings.output_root))
    app.config.setdefault("ENABLE_V2_UI", True)
    secret = app.config.get("SECRET_KEY") or "dev-secret"
    app.config["SECRET_KEY"] = secret
    app.secret_key = secret

    def _extract_index_info(plan_summary: str | None) -> dict[str, object]:
        if not plan_summary or plan_summary == "None":
            return {
                "scan_type": "None",
                "index_pattern": None,
                "index_name": None,
                "display_text": "None",
            }

        if plan_summary == "COLLSCAN":
            return {
                "scan_type": "COLLSCAN",
                "index_pattern": None,
                "index_name": None,
                "display_text": "Collection Scan (No Index)",
            }

        if plan_summary.startswith("IXSCAN"):
            index_pattern: object | None = None
            display_text = "Index Scan"
            if "{" in plan_summary and "}" in plan_summary:
                start = plan_summary.find("{")
                end = plan_summary.rfind("}") + 1
                pattern_str = plan_summary[start:end]
                try:
                    parsed = json.loads(pattern_str)
                    index_pattern = parsed
                    if isinstance(parsed, dict):
                        parts = []
                        for field, direction in parsed.items():
                            if direction == 1:
                                parts.append(f"{field} (asc)")
                            elif direction == -1:
                                parts.append(f"{field} (desc)")
                            else:
                                parts.append(str(field))
                        display_text = "Index Scan: " + ", ".join(parts)
                    else:
                        display_text = f"Index Scan: {pattern_str}"
                except Exception:
                    index_pattern = pattern_str
                    display_text = f"Index Scan: {pattern_str}"
            return {
                "scan_type": "IXSCAN",
                "index_pattern": index_pattern,
                "index_name": None,
                "display_text": display_text,
            }

        scan_type = plan_summary.split(" {", 1)[0].split(" ", 1)[0]
        return {
            "scan_type": scan_type,
            "index_pattern": None,
            "index_name": None,
            "display_text": plan_summary,
        }

    app.add_template_filter(_extract_index_info, name="extract_index_info")

    def _format_plan_breakdown(plans: list[dict[str, object]] | None) -> str:
        if not plans:
            return ""
        parts: list[str] = []
        for entry in plans:
            name = entry.get("plan_summary") or "None"
            count = entry.get("count") or 0
            try:
                count_int = int(count)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                count_int = 0
            parts.append(f"{name} × {count_int}")
        return ", ".join(parts)

    app.add_template_filter(_format_plan_breakdown, name="format_plan_breakdown")

    def _parse_search_conditions(args) -> tuple[list[dict], str, str, str]:
        keyword = (args.get("keyword") or "").strip()
        field_name = (args.get("field_name") or "").strip()
        field_value = (args.get("field_value") or "").strip()

        conditions: list[dict] = []
        indices = set()
        for key in args.keys():
            if key.startswith("filters[") and key.endswith("][type]"):
                try:
                    idx = int(key.split("[")[1].split("]")[0])
                    indices.add(idx)
                except (ValueError, IndexError):
                    continue
        for idx in sorted(indices):
            prefix = f"filters[{idx}]"
            value = (args.get(f"{prefix}[value]") or "").strip()
            cond_type = (args.get(f"{prefix}[type]") or "").strip() or "keyword"
            if not value:
                continue
            conditions.append(
                {
                    "type": cond_type,
                    "name": (args.get(f"{prefix}[name]") or "").strip(),
                    "value": value,
                    "regex": args.get(f"{prefix}[regex]") in {"on", "true", "1"},
                    "case_sensitive": args.get(f"{prefix}[case]") in {"on", "true", "1"},
                    "negate": args.get(f"{prefix}[not]") in {"on", "true", "1"},
                }
            )

        base_conditions = []
        if keyword:
            base_conditions.append(
                {
                    "type": "keyword",
                    "value": keyword,
                    "regex": False,
                    "case_sensitive": False,
                    "negate": False,
                }
            )
        if field_name and field_value:
            base_conditions.append(
                {
                    "type": "field",
                    "name": field_name,
                    "value": field_value,
                    "regex": False,
                    "case_sensitive": False,
                    "negate": False,
                }
            )

        return base_conditions + conditions, keyword, field_name, field_value

    def _plan_badge_class(scan_type: str | None) -> str:
        if not scan_type:
            return "bg-secondary"
        normalized = scan_type.strip().upper()
        mapping = {
            "COLLSCAN": "bg-danger",
            "IXSCAN": "bg-success",
            "COUNT_SCAN": "bg-info",
            "COUNT": "bg-info",
            "IDHACK": "bg-warning",
            "SORT_MERGE": "bg-primary",
            "HOT_INDEX": "bg-primary",
        }
        return mapping.get(normalized, "bg-secondary")

    app.add_template_filter(_plan_badge_class, name="plan_badge_class")

    def _search_log_entries(
        dataset_root: Path,
        conditions: list[dict],
        start_ts: int | None,
        end_ts: int | None,
        limit: int,
    ) -> list[dict]:
        file_map_path = dataset_root / "index" / "file_map.json"
        if not file_map_path.exists():
            return []

        try:
            file_map = json.loads(file_map_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return []

        keyword_conditions = [c for c in conditions if c.get("type") != "field"]
        field_conditions = [c for c in conditions if c.get("type") == "field"]

        compiled_keywords = []
        for cond in keyword_conditions:
            value = cond.get("value", "")
            if not value:
                continue
            regex = cond.get("regex", False)
            case = cond.get("case_sensitive", False)
            negate = cond.get("negate", False)
            if regex:
                try:
                    compiled = re.compile(value, 0 if case else re.IGNORECASE)
                except re.error:
                    compiled = None
                compiled_keywords.append(("regex", compiled, negate))
            else:
                cmp_value = value if case else value.lower()
                compiled_keywords.append(("substr", cmp_value, negate, case))

        compiled_fields = []
        for cond in field_conditions:
            name = cond.get("name", "")
            value = cond.get("value", "")
            if not name or not value:
                compiled_fields.append(None)
                continue
            if cond.get("regex", False):
                try:
                    pattern = re.compile(
                        value,
                        0 if cond.get("case_sensitive", False) else re.IGNORECASE,
                    )
                except re.error:
                    pattern = None
                compiled_fields.append(pattern)
            else:
                compiled_fields.append(
                    value if cond.get("case_sensitive", False) else value.lower()
                )

        def keyword_match(raw_line: str) -> bool:
            for pattern in compiled_keywords:
                kind = pattern[0]
                if kind == "regex":
                    compiled, negate = pattern[1], pattern[2]
                    has_match = bool(compiled.search(raw_line)) if compiled else False
                else:
                    substr, negate, case = pattern[1], pattern[2], pattern[3]
                    haystack = raw_line if case else raw_line.lower()
                    has_match = substr in haystack
                if has_match == negate:
                    return False
            return True

        results: list[dict] = []
        count_limit = max(1, min(limit, 1000))

        for path_str in file_map.values():
            path = Path(path_str)
            if not path.exists():
                continue
            with path.open("r", encoding="utf-8", errors="ignore") as handle:
                for line_number, line in enumerate(handle, 1):
                    raw_line = line.strip()
                    if not raw_line or not raw_line.startswith("{"):
                        continue
                    if compiled_keywords and not keyword_match(raw_line):
                        continue
                    try:
                        entry = json.loads(raw_line)
                    except json.JSONDecodeError:
                        continue

                    ts_str = (
                        entry.get("t", {}).get("$date")
                        if isinstance(entry.get("t"), dict)
                        else None
                    )
                    ts_dt = _parse_iso_datetime(ts_str) if isinstance(ts_str, str) else None
                    ts_epoch = int(ts_dt.timestamp()) if ts_dt else None

                    if start_ts is not None and (ts_epoch is None or ts_epoch < start_ts):
                        continue
                    if end_ts is not None and (ts_epoch is None or ts_epoch > end_ts):
                        continue

                    field_ok = True
                    for cond, compiled_value in zip(field_conditions, compiled_fields):
                        field_path = cond.get("name", "")
                        if not field_path:
                            continue
                        target_value = _get_field(entry, field_path)
                        negate = cond.get("negate", False)
                        match = False
                        if target_value is not None:
                            if cond.get("regex", False):
                                pattern = compiled_value
                                match = bool(pattern.search(target_value)) if pattern else False
                            else:
                                case = cond.get("case_sensitive", False)
                                haystack = target_value if case else target_value.lower()
                                needle = compiled_value if case else compiled_value
                                match = needle in haystack if needle is not None else False
                        if match == negate:
                            field_ok = False
                            break
                    if not field_ok:
                        continue

                    results.append(
                        {
                            "timestamp": ts_dt,
                            "raw_line": raw_line,
                            "file_path": str(path),
                            "line_number": line_number,
                        }
                    )
                    if len(results) >= count_limit:
                        break
            if len(results) >= count_limit:
                break

        results.sort(key=lambda row: row.get("timestamp") or datetime.min)
        return results

    @app.teardown_appcontext
    def close_duckdb(exception: Exception | None) -> None:
        service = getattr(app, "slowq_duckdb_service", None)
        if service is not None:
            service.close()
            app.slowq_duckdb_service = None

    app.register_blueprint(slowq_blueprint)

    def _get_duckdb_service() -> DuckDBService:
        service = getattr(app, "slowq_duckdb_service", None)
        if service is not None:
            try:
                service.connection.execute("SELECT 1")
            except Exception:
                try:
                    service.close()
                except Exception:
                    pass
                service = None
                app.slowq_duckdb_service = None
        if service is None:
            dataset_root = Path(app.config["SLOWQ_DATASET_ROOT"])
            service = DuckDBService(dataset_root=dataset_root)
            app.slowq_duckdb_service = service
        return service

    def _parse_iso_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            text = value.replace("Z", "+00:00")
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    def _parse_datetime(value: str | None, *, is_end: bool = False) -> datetime | None:
        if not value:
            return None
        value = value.strip()
        if not value:
            return None
        try:
            if "T" in value:
                coerced = value.replace("Z", "+00:00")
                dt = datetime.fromisoformat(coerced)
            else:
                dt = datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            return None

        if is_end and "T" not in value:
            dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999)

        return dt

    def _format_datetime_for_input(dt: datetime | None) -> str | None:
        if dt is None:
            return None
        return dt.strftime("%Y-%m-%dT%H:%M")

    def _local_display_from_iso(iso_value: str | None) -> str | None:
        if not iso_value:
            return None
        match = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2})", iso_value)
        return match.group(1) if match else None

    def _paginate(data: list, page: int, per_page: int) -> dict:
        if per_page == "all":
            per_page = len(data) or 1
        try:
            per_page = int(per_page)
        except (TypeError, ValueError):
            per_page = 100
        try:
            page = max(int(page), 1)
        except (TypeError, ValueError):
            page = 1

        total = len(data)
        pages = max(1, (total + per_page - 1) // per_page)
        if page > pages:
            page = pages
        start = (page - 1) * per_page
        end = start + per_page

        return {
            "items": data[start:end],
            "page": page,
            "per_page": per_page,
            "total": total,
            "pages": pages,
            "has_prev": page > 1,
            "has_next": page < pages,
            "prev_num": page - 1 if page > 1 else None,
            "next_num": page + 1 if page < pages else None,
        }

    def _get_field(entry: dict, path: str) -> str | None:
        if not path:
            return None
        parts = path.split(".")
        current: object = entry
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        if isinstance(current, (str, int, float, bool)):
            return str(current)
        return None

    def _extract_offset(iso_value: str | None) -> str | None:
        if not iso_value:
            return None
        match = re.search(r"([+-]\d{2}:\d{2}|Z)$", iso_value)
        return match.group(1) if match else None

    def _timezone_from_offset(offset: str | None) -> timezone | None:
        if not offset:
            return None
        if offset == "Z":
            return timezone.utc
        try:
            sign = 1 if offset.startswith("+") else -1
            hours, minutes = offset[1:].split(":")
            delta = timedelta(hours=int(hours), minutes=int(minutes))
            return timezone(sign * delta)
        except (ValueError, TypeError):
            return None

    def _resolve_time_filters(
        params: Mapping[str, str],
        *,
        service: DuckDBService,
        date_offsets: dict[str, str],
    ) -> dict[str, object]:
        """Normalize incoming time filter parameters with dataset offsets."""

        start_date_str = (params.get("start_date") or "").strip() or None
        end_date_str = (params.get("end_date") or "").strip() or None
        start_iso_param = (params.get("start_iso") or "").strip() or None
        end_iso_param = (params.get("end_iso") or "").strip() or None
        start_ts_param = (params.get("start_ts") or "").strip() or None
        end_ts_param = (params.get("end_ts") or "").strip() or None

        def _normalize_local(value: str | None, *, is_end: bool) -> str | None:
            if not value:
                return None
            text = value.strip()
            if not text:
                return None
            if "T" in text:
                if len(text) == 16:
                    return text + ":00"
                return text
            suffix = "T23:59:59" if is_end else "T00:00:00"
            return text + suffix

        def _attach_timezone(value: datetime | None, iso_hint: str | None) -> datetime | None:
            if value is None:
                return None
            if value.tzinfo is not None:
                return value
            offset = _extract_offset(iso_hint)
            if offset is None:
                offset = date_offsets.get(value.strftime("%Y-%m-%d"))
            tzinfo_value = _timezone_from_offset(offset)
            if tzinfo_value is None:
                tzinfo_value = timezone.utc
            return value.replace(tzinfo=tzinfo_value)

        def _coerce(
            local_value: str | None,
            iso_hint: str | None,
            ts_hint: str | None,
            *,
            is_end: bool,
        ) -> tuple[str | None, datetime | None, int | None]:
            iso_value = iso_hint or None
            if not iso_value and ts_hint:
                try:
                    iso_value = datetime.fromtimestamp(int(ts_hint), tz=timezone.utc).isoformat()
                except (OSError, TypeError, ValueError):
                    iso_value = None

            normalized_local = _normalize_local(local_value, is_end=is_end)
            minute_value = normalized_local[:16] if normalized_local else None

            epoch_hint: int | None = None
            if minute_value:
                direction = "end" if is_end else "start"
                resolved = service.resolve_timestamp_for_local(
                    normalized_local if normalized_local else minute_value,
                    direction=direction,
                    hint=iso_hint,
                )
                if resolved and not iso_value:
                    iso_value, epoch_hint = resolved
                elif resolved:
                    _, epoch_hint = resolved

            if not iso_value and minute_value and normalized_local:
                day_key = minute_value[:10]
                offset = date_offsets.get(day_key)
                if offset:
                    suffix = "Z" if offset == "Z" else offset
                    iso_value = normalized_local + suffix

            dt = _parse_iso_datetime(iso_value)
            if dt is None and normalized_local:
                fallback = _parse_datetime(normalized_local, is_end=is_end)
                dt = _attach_timezone(fallback, iso_value)

            if epoch_hint is None and isinstance(dt, datetime):
                try:
                    epoch_hint = int(dt.timestamp())
                except (OverflowError, OSError, ValueError):
                    epoch_hint = None

            return iso_value, dt, epoch_hint

        start_iso_value, start_dt, start_epoch = _coerce(
            start_date_str, start_iso_param, start_ts_param, is_end=False
        )
        end_iso_value, end_dt, end_epoch = _coerce(
            end_date_str, end_iso_param, end_ts_param, is_end=True
        )

        if start_dt and end_dt and start_dt > end_dt:
            start_dt, end_dt = end_dt, start_dt
            start_iso_value, end_iso_value = end_iso_value, start_iso_value
            start_epoch, end_epoch = end_epoch, start_epoch

        return {
            "start_dt": start_dt,
            "end_dt": end_dt,
            "start_iso": start_iso_value,
            "end_iso": end_iso_value,
            "start_local": start_date_str,
            "end_local": end_date_str,
            "start_epoch": start_epoch,
            "end_epoch": end_epoch,
        }

    @app.route("/slow-query-analysis", endpoint="slow_query_analysis")
    @app.route("/slow-query-analysis/v2", endpoint="slow_query_analysis_v2")
    def slow_query_analysis():
        service = _get_duckdb_service()

        start_ts = end_ts = None
        date_offsets = service.get_date_offset_map()
        time_filters = _resolve_time_filters(request.args, service=service, date_offsets=date_offsets)
        start_dt = time_filters["start_dt"]
        end_dt = time_filters["end_dt"]
        start_iso_value = time_filters["start_iso"]
        end_iso_value = time_filters["end_iso"]
        start_ts = time_filters.get("start_epoch")
        end_ts = time_filters.get("end_epoch")

        start_date_display = request.args.get("start_date") or time_filters.get("start_local") or ""
        end_date_display = request.args.get("end_date") or time_filters.get("end_local") or ""

        analysis = service.analyze_slow_query_patterns(
            grouping="pattern_key",
            start_ts=start_ts,
            end_ts=end_ts,
            exclude_system=True,
            page=1,
            per_page=SLOWQ_ALL_PAGE_LIMIT,
            order_by="total_duration_ms",
            order_dir="desc",
        )

        items = analysis.get("items", [])
        patterns_ordered: "OrderedDict[str, dict[str, object]]" = OrderedDict()

        for row in items:
            entry = dict(row)
            pattern_key = entry.get("pattern_key") or entry.get("namespace") or row.get("query_hash")
            avg_duration = float(entry.get("avg_duration_ms") or 0.0)
            min_duration = float(entry.get("min_duration_ms") or 0.0)
            max_duration = float(entry.get("max_duration_ms") or 0.0)
            selectivity = float(entry.get("selectivity_pct") or 0.0)
            index_eff = float(entry.get("index_efficiency_pct") or 0.0)

            first_seen = entry.get("first_seen_raw")
            last_seen = entry.get("last_seen_raw")

            entry.update(
                {
                    "pattern_key": pattern_key,
                    "database": entry.get("database") or "unknown",
                    "collection": entry.get("collection") or "unknown",
                    "query_hash": entry.get("query_hash") or "unknown",
                    "query_type": entry.get("operation") or "unknown",
                    "total_count": int(entry.get("execution_count") or entry.get("total_count") or 0),
                    "avg_duration": int(round(avg_duration)),
                    "min_duration": int(round(min_duration)),
                    "max_duration": int(round(max_duration)),
                    "avg_selectivity": selectivity,
                    "avg_index_efficiency": index_eff,
                    "total_docs_examined": int(entry.get("total_docs_examined") or 0),
                    "total_returned": int(entry.get("total_docs_returned") or 0),
                    "first_seen": _parse_iso_datetime(first_seen) if isinstance(first_seen, str) else None,
                    "last_seen": _parse_iso_datetime(last_seen) if isinstance(last_seen, str) else None,
                    "slowest_query_full": entry.get("slowest_query_full"),
                    "sample_query": entry.get("sample_query") or entry.get("slowest_query_full"),
                }
            )

            if isinstance(pattern_key, str):
                patterns_ordered[pattern_key] = entry

        min_date, max_date = service.get_available_date_range()

        context = {
            "patterns": patterns_ordered,
            "total_executions": int(analysis.get("total_executions") or 0),
            "avg_duration": float(analysis.get("avg_duration_ms") or 0.0),
            "high_priority_count": int(analysis.get("high_priority_count") or 0),
            "start_date": start_date_display,
            "end_date": end_date_display,
            "start_iso": start_iso_value,
            "end_iso": end_iso_value,
            "min_date": _format_datetime_for_input(min_date),
            "max_date": _format_datetime_for_input(max_date),
            "pagination": None,
        }

        return render_template("slow_query_analysis.html", **context)

    @app.route("/")
    def index():
        return redirect(url_for("dashboard"))

    @app.route("/dashboard")
    def dashboard():
        service = _get_duckdb_service()

        stats = service.get_dashboard_summary(limit=10)

        # Coerce timestamps for template formatting
        def _coerce_user(entry: dict) -> None:
            raw = entry.get("last_seen")
            if isinstance(raw, str):
                parsed = _parse_iso_datetime(raw)
                if parsed is not None:
                    entry["last_seen"] = parsed

        for user in stats.get("top_users", []):
            _coerce_user(user)

        return render_template("dashboard_results.html", stats=stats)

    @app.route("/dashboard/v2")
    def dashboard_v2():
        return redirect(url_for("dashboard"))

    def _format_duration_pair(value_ms: float | int | None) -> tuple[str, str]:
        if value_ms is None:
            return ("0 ms", "0 ms")
        try:
            ms_float = float(value_ms)
        except (TypeError, ValueError):
            ms_float = 0.0

        raw = f"{ms_float:,.0f} ms"
        if ms_float >= 60_000:
            minutes = ms_float / 60_000
            return (f"{minutes:,.1f} min", raw)
        if ms_float >= 1_000:
            seconds = ms_float / 1_000
            return (f"{seconds:,.1f} s", raw)
        return (f"{ms_float:,.0f} ms", raw)

    def _format_total_duration(value_ms: float | int | None) -> str:
        if value_ms is None:
            return "0 ms"
        try:
            total = float(value_ms)
        except (TypeError, ValueError):
            total = 0.0
        if total >= 3_600_000:
            hours = total / 3_600_000
            return f"{hours:,.1f} h"
        if total >= 60_000:
            minutes = total / 60_000
            return f"{minutes:,.1f} min"
        if total >= 1_000:
            seconds = total / 1_000
            return f"{seconds:,.1f} s"
        return f"{total:,.0f} ms"

    @app.route("/query-trend", endpoint="query_trend")
    @app.route("/query-trend/v2", endpoint="query_trend_v2")
    def query_trend():
        service = _get_duckdb_service()

        grouping_type = (request.args.get("groupingType") or "pattern_key").strip().lower()
        if grouping_type not in {"pattern_key", "namespace", "query_hash"}:
            grouping_type = "pattern_key"

        selected_db = (request.args.get("database") or "all").strip()
        if not selected_db:
            selected_db = "all"

        selected_namespace = (request.args.get("namespace") or "all").strip()
        if not selected_namespace:
            selected_namespace = "all"

        exclude_system = selected_db.lower() == "all"

        filters: dict[str, object] = {
            "exclude_system": exclude_system,
        }
        if selected_db.lower() != "all":
            filters["database"] = selected_db
        if selected_namespace.lower() != "all":
            filters["namespace"] = selected_namespace

        dataset = service.get_query_trend_dataset(
            grouping=grouping_type,
            filters=filters,
            summary_limit=100,
            execution_limit=2000,
        )

        patterns = dataset.get("patterns", []) if isinstance(dataset, dict) else []
        executions = dataset.get("executions", []) if isinstance(dataset, dict) else []

        pattern_map: dict[str, dict[str, object]] = {}
        top_queries: list[dict[str, object]] = []
        total_execs = 0
        total_duration = 0.0
        max_duration = 0.0
        namespaces_seen: set[str] = set()
        operations_seen: set[str] = set()

        for record in patterns:
            if not isinstance(record, dict):
                continue
            group_key = record.get("pattern_key") or record.get("namespace") or record.get("query_hash")
            if group_key is None:
                continue

            execution_count = int(record.get("execution_count") or 0)
            avg_duration = float(record.get("avg_duration_ms") or 0.0)
            max_duration = max(max_duration, float(record.get("max_duration_ms") or 0.0))
            total_execs += execution_count
            total_duration += float(record.get("total_duration_ms") or 0.0)

            namespace_value = record.get("namespace") or "unknown.unknown"
            namespaces_seen.add(str(namespace_value))
            operation_value = record.get("operation") or "unknown"
            operations_seen.add(str(operation_value))

            mean_fmt, mean_raw = _format_duration_pair(avg_duration)
            max_fmt, max_raw = _format_duration_pair(record.get("max_duration_ms"))
            sum_fmt = _format_total_duration(record.get("total_duration_ms"))

            top_queries.append(
                {
                    "namespace": namespace_value,
                    "operation": operation_value,
                    "execution_count": execution_count,
                    "mean_duration": avg_duration,
                    "mean_duration_formatted": mean_fmt,
                    "mean_duration_raw": mean_raw,
                    "max_duration": float(record.get("max_duration_ms") or 0.0),
                    "max_duration_formatted": max_fmt,
                    "max_duration_raw": max_raw,
                    "sum_duration": float(record.get("total_duration_ms") or 0.0),
                    "sum_duration_formatted": sum_fmt,
                    "query_hash": group_key,
                }
            )

            pattern_map[str(group_key)] = {
                "namespace": namespace_value,
                "database": record.get("database") or "unknown",
                "collection": record.get("collection") or "unknown",
                "operation": operation_value,
                "plan_summary": record.get("plan_summary") or "None",
                "avg_duration": avg_duration,
                "max_duration": float(record.get("max_duration_ms") or 0.0),
                "execution_count": execution_count,
                "total_duration": float(record.get("total_duration_ms") or 0.0),
                "docs_examined": int(record.get("docs_examined") or 0),
                "docs_returned": int(record.get("docs_returned") or 0),
                "sample_query": record.get("sample_query"),
                "executions": [],
            }

        top_queries.sort(key=lambda row: row.get("sum_duration", 0), reverse=True)
        top_queries = top_queries[:50]

        for entry in executions:
            if not isinstance(entry, dict):
                continue
            group_key = entry.get("pattern_key") or entry.get("namespace") or entry.get("query_hash")
            if group_key is None:
                continue
            key = str(group_key)
            bucket = pattern_map.get(key)
            if not bucket:
                continue
            duration_ms = int(float(entry.get("duration_ms") or 0.0))
            timestamp_value = entry.get("timestamp")
            bucket["executions"].append(
                {
                    "timestamp": timestamp_value,
                    "duration": duration_ms,
                }
            )

        for bucket in pattern_map.values():
            executions_list = bucket.get("executions")
            if isinstance(executions_list, list):
                bucket["executions"] = executions_list[-20:]

        trend_data = []
        for entry in executions:
            if not isinstance(entry, dict):
                continue
            group_key = entry.get("pattern_key") or entry.get("namespace") or entry.get("query_hash")
            if group_key is None:
                continue
            key = str(group_key)
            bucket = pattern_map.get(key)
            avg_duration = bucket.get("avg_duration") if bucket else 0.0
            trend_data.append(
                {
                    "timestamp": entry.get("timestamp"),
                    "duration": int(float(entry.get("duration_ms") or 0.0)),
                    "operation": (entry.get("operation") or "unknown").lower(),
                    "namespace": entry.get("namespace") or "unknown.unknown",
                    "query_hash": key,
                    "avg_duration": float(avg_duration or 0.0),
                }
            )

        query_details = {}
        for key, bucket in pattern_map.items():
            query_details[key] = {
                "namespace": bucket.get("namespace"),
                "operation_type": bucket.get("operation"),
                "execution_count": bucket.get("execution_count", 0),
                "avg_duration": bucket.get("avg_duration", 0.0),
                "max_duration": bucket.get("max_duration", 0.0),
                "total_duration": bucket.get("total_duration", 0.0),
                "sample_query": bucket.get("sample_query"),
                "executions": bucket.get("executions", []),
            }

        avg_duration = total_duration / total_execs if total_execs else 0.0

        stats = SimpleNamespace(
            total_executions=total_execs,
            avg_duration=avg_duration,
            max_duration=max_duration,
            unique_patterns=len(pattern_map),
            unique_namespaces=len(namespaces_seen),
            unique_operations=len(operations_seen),
        )

        databases = service.list_slow_query_databases(exclude_system=True)
        namespaces = service.list_slow_query_namespaces(
            database=None if selected_db.lower() == "all" else selected_db,
            exclude_system=exclude_system,
        )

        context = {
            "grouping_type": grouping_type,
            "selected_db": selected_db,
            "selected_namespace": selected_namespace,
            "databases": databases,
            "namespaces": namespaces,
            "trend_data": trend_data,
            "query_details": query_details,
            "top_queries": top_queries,
            "stats": stats if pattern_map else None,
        }

        return render_template("query_trend.html", **context)
    @app.route("/slow-queries", endpoint="slow_queries")
    @app.route("/slow-queries/v2", endpoint="slow_queries_v2")
    def slow_queries():
        service = _get_duckdb_service()

        view_mode = (request.args.get("view_mode") or "all_executions").strip()
        if view_mode not in {"all_executions", "unique_queries"}:
            view_mode = "all_executions"

        grouping_type = (request.args.get("grouping") or "pattern_key").strip() or "pattern_key"
        if grouping_type not in {"pattern_key", "namespace", "query_hash"}:
            grouping_type = "pattern_key"

        selected_db = (request.args.get("database") or "all").strip() or "all"
        selected_plan = (request.args.get("plan_summary") or "all").strip() or "all"

        threshold_param = (request.args.get("threshold") or "100").strip()
        try:
            threshold = max(int(threshold_param), 0)
        except (ValueError, TypeError):
            threshold = 100

        page = request.args.get("page", default=1, type=int)
        if page is None or page < 1:
            page = 1

        per_page_param = (request.args.get("per_page") or str(SLOWQ_ALLOWED_PAGE_SIZES[0])).strip()
        if per_page_param == "all":
            per_page = SLOWQ_ALL_PAGE_LIMIT
            per_page_all = True
        else:
            per_page_all = False
            try:
                per_page = int(per_page_param)
            except (ValueError, TypeError):
                per_page = SLOWQ_ALLOWED_PAGE_SIZES[0]
            if per_page not in SLOWQ_ALLOWED_PAGE_SIZES:
                per_page = SLOWQ_ALLOWED_PAGE_SIZES[0]

        date_offsets = service.get_date_offset_map()
        time_filters = _resolve_time_filters(request.args, service=service, date_offsets=date_offsets)
        start_dt = time_filters["start_dt"]
        end_dt = time_filters["end_dt"]
        start_iso_value = time_filters["start_iso"]
        end_iso_value = time_filters["end_iso"]
        start_epoch = time_filters.get("start_epoch")
        end_epoch = time_filters.get("end_epoch")

        start_date_display = request.args.get("start_date") or time_filters.get("start_local") or ""
        end_date_display = request.args.get("end_date") or time_filters.get("end_local") or ""

        # Fetch dropdown data
        databases = ["all"] + service.list_slow_query_databases(exclude_system=True)

        # Base filter kwargs for service calls
        db_filter = None if selected_db.lower() == "all" else selected_db
        plan_filter = None if selected_plan.lower() == "all" else selected_plan

        # Adjust per-page when "all" requested
        fetch_limit = per_page if not per_page_all else SLOWQ_ALL_PAGE_LIMIT

        queries: list[dict[str, object]] = []
        total_queries = 0

        if view_mode == "unique_queries":
            offset = (page - 1) * fetch_limit if not per_page_all else 0
            analysis = service.analyze_slow_query_patterns(
                grouping=grouping_type,
                threshold_ms=threshold,
                database=db_filter,
                plan_summary=plan_filter,
                start_ts=start_epoch,
                end_ts=end_epoch,
                exclude_system=(db_filter is None),
                page=page if not per_page_all else 1,
                per_page=fetch_limit,
                order_by="total_duration_ms",
                order_dir=(request.args.get("direction") or "desc"),
            )

            items = analysis.get("items", [])
            total_queries = int(analysis.get("total_groups") or 0)

            queries = []
            for item in items:
                entry = dict(item)
                avg_duration = float(entry.get("avg_duration_ms") or 0.0)
                min_duration = float(entry.get("min_duration_ms") or 0.0)
                max_duration = float(entry.get("max_duration_ms") or 0.0)
                last_seen_raw = entry.get("last_seen_raw")
                last_seen_dt = _parse_iso_datetime(last_seen_raw) if isinstance(last_seen_raw, str) else None

                entry.update(
                    {
                        "avg_duration": int(round(avg_duration)),
                        "min_duration": int(round(min_duration)) if min_duration else None,
                        "max_duration": int(round(max_duration)) if max_duration else None,
                        "last_seen": last_seen_dt,
                        "execution_count": int(entry.get("execution_count") or 0),
                        "duration": int(round(max_duration)) if max_duration else int(round(avg_duration)),
                        "sample_query": entry.get("sample_query") or entry.get("slowest_query_full"),
                        "query_text": entry.get("slowest_query_full"),
                        "docs_examined": int(entry.get("total_docs_examined") or 0),
                    }
                )
                queries.append(entry)

        else:
            # all executions view
            result = service.fetch_slow_query_executions(
                threshold_ms=threshold,
                database=db_filter,
                plan_summary=plan_filter,
                start_ts=start_epoch,
                end_ts=end_epoch,
                page=page if not per_page_all else 1,
                per_page=fetch_limit,
            )

            items = result.get("items", [])
            total_queries = int(result.get("total") or 0)

            queries = []
            for item in items:
                entry = dict(item)
                duration_ms = int(entry.get("duration_ms") or entry.get("duration") or 0)
                entry["duration"] = duration_ms
                timestamp_value = entry.get("timestamp")
                if isinstance(timestamp_value, str):
                    parsed = _parse_iso_datetime(timestamp_value)
                    if parsed is not None:
                        entry["timestamp"] = parsed
                queries.append(entry)

        # Pagination metadata
        if per_page_all:
            per_page_effective = total_queries if 0 < total_queries <= SLOWQ_ALL_PAGE_LIMIT else min(SLOWQ_ALL_PAGE_LIMIT, max(total_queries, 1))
            pages = 1 if per_page_effective == 0 else 1
            page = 1
            has_prev = False
            has_next = False
            prev_num = None
            next_num = None
        else:
            per_page_effective = fetch_limit
            pages = max(1, math.ceil(total_queries / per_page_effective) if per_page_effective else 1)
            if page > pages:
                page = pages
            has_prev = page > 1
            has_next = page < pages
            prev_num = page - 1 if has_prev else None
            next_num = page + 1 if has_next else None

        pagination = {
            "items": queries,
            "page": page,
            "per_page": per_page_effective,
            "total": total_queries,
            "pages": pages,
            "has_prev": has_prev,
            "has_next": has_next,
            "prev_num": prev_num,
            "next_num": next_num,
        }

        min_date, max_date = service.get_available_date_range()
        min_date_display = _format_datetime_for_input(min_date)
        max_date_display = _format_datetime_for_input(max_date)

        context = {
            "queries": queries,
            "pagination": pagination,
            "view_mode": view_mode,
            "grouping_type": grouping_type,
            "databases": databases,
            "selected_db": selected_db,
            "selected_plan": selected_plan,
            "threshold": threshold,
            "total_queries": total_queries,
            "start_date": start_date_display,
            "end_date": end_date_display,
            "min_date": min_date_display,
            "max_date": max_date_display,
            "start_iso": start_iso_value,
            "end_iso": end_iso_value,
            "per_page": per_page_param,
            "page": page,
        }

        return render_template("slow_queries.html", **context)

    @app.route("/export-slow-queries")
    def export_slow_queries():
        service = _get_duckdb_service()

        date_offsets = service.get_date_offset_map()
        time_filters = _resolve_time_filters(
            request.args, service=service, date_offsets=date_offsets
        )
        start_dt = time_filters["start_dt"]
        end_dt = time_filters["end_dt"]
        start_iso_value = time_filters["start_iso"]
        end_iso_value = time_filters["end_iso"]
        start_epoch_hint = time_filters.get("start_epoch")
        end_epoch_hint = time_filters.get("end_epoch")

        start_ts = start_epoch_hint if isinstance(start_epoch_hint, int) else (int(start_dt.timestamp()) if start_dt else None)
        end_ts = end_epoch_hint if isinstance(end_epoch_hint, int) else (int(end_dt.timestamp()) if end_dt else None)

        selected_db = (request.args.get("database") or "all").strip() or "all"
        selected_plan = (request.args.get("plan_summary") or "all").strip() or "all"
        view_mode = request.args.get("view_mode", "all_executions")
        grouping_type = (request.args.get("grouping") or "pattern_key").strip()
        if grouping_type not in {"pattern_key", "namespace", "query_hash"}:
            grouping_type = "pattern_key"

        threshold_param = request.args.get("threshold", "100")
        try:
            threshold = max(int(threshold_param), 0)
        except (TypeError, ValueError):
            threshold = 100

        limit_param = request.args.get("limit")
        if limit_param == "all":
            requested_limit = SLOWQ_EXPORT_MAX_LIMIT
        elif limit_param is None or limit_param == "":
            requested_limit = SLOWQ_EXPORT_DEFAULT_LIMIT
        else:
            try:
                requested_limit = int(limit_param)
            except (TypeError, ValueError):
                requested_limit = SLOWQ_EXPORT_DEFAULT_LIMIT
        export_limit = max(1, min(requested_limit, SLOWQ_EXPORT_MAX_LIMIT))

        def _coerce(value: object) -> object:
            if isinstance(value, datetime):
                return value.isoformat()
            if isinstance(value, Decimal):
                return float(value)
            return value

        truncated = False
        if view_mode == "unique_queries":
            result = service.fetch_slow_query_patterns(
                grouping=grouping_type,
                threshold_ms=threshold,
                database=selected_db,
                plan_summary=selected_plan,
                start_ts=start_ts,
                end_ts=end_ts,
                limit=export_limit,
                offset=0,
            )
            total = result["total"]
            items = result.get("items", [])
            truncated = total > export_limit
            payload_items = [
                {key: _coerce(value) for key, value in entry.items()}
                for entry in items
            ]
            export_payload = {
                "mode": "unique_queries",
                "grouping": grouping_type,
                "limit": export_limit,
                "returned": len(payload_items),
                "total_matched": total,
                "truncated": truncated,
                "filters": {
                    "database": selected_db,
                    "plan_summary": selected_plan,
                    "threshold_ms": threshold,
                    "start_iso": start_iso_value,
                    "end_iso": end_iso_value,
                    "limit": export_limit,
                },
                "items": payload_items,
            }
            suffix = "patterns"
        else:
            result = service.fetch_slow_query_executions(
                threshold_ms=threshold,
                database=selected_db,
                plan_summary=selected_plan,
                start_ts=start_ts,
                end_ts=end_ts,
                page=1,
                per_page=export_limit,
            )
            total = result["total"]
            items = result.get("items", [])
            truncated = total > export_limit
            payload_items = []
            for entry in items:
                converted = {}
                for key, value in entry.items():
                    converted[key] = _coerce(value)
                payload_items.append(converted)
            export_payload = {
                "mode": "all_executions",
                "limit": export_limit,
                "returned": len(payload_items),
                "total_matched": total,
                "truncated": truncated,
                "filters": {
                    "database": selected_db,
                    "plan_summary": selected_plan,
                    "threshold_ms": threshold,
                    "start_iso": start_iso_value,
                    "end_iso": end_iso_value,
                    "limit": export_limit,
                },
                "items": payload_items,
            }
            suffix = "executions"

        export_payload["exported_at"] = datetime.utcnow().isoformat() + "Z"

        file_parts = ["slow_queries", suffix]
        if selected_db and selected_db != "all":
            file_parts.append(selected_db.replace(".", "_"))
        timestamp_label = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{'_'.join(file_parts)}_{timestamp_label}.json"

        data = json.dumps(export_payload, indent=2)
        response = app.response_class(data, mimetype="application/json")
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        return response

    @app.route("/export-query-analysis")
    def export_query_analysis():
        service = _get_duckdb_service()

        date_offsets = service.get_date_offset_map()
        time_filters = _resolve_time_filters(
            request.args, service=service, date_offsets=date_offsets
        )
        start_dt = time_filters["start_dt"]
        end_dt = time_filters["end_dt"]
        start_iso_value = time_filters["start_iso"]
        end_iso_value = time_filters["end_iso"]
        start_epoch_hint = time_filters.get("start_epoch")
        end_epoch_hint = time_filters.get("end_epoch")

        start_ts = (
            start_epoch_hint
            if isinstance(start_epoch_hint, int)
            else (int(start_dt.timestamp()) if start_dt else None)
        )
        end_ts = (
            end_epoch_hint
            if isinstance(end_epoch_hint, int)
            else (int(end_dt.timestamp()) if end_dt else None)
        )

        grouping_type = (request.args.get("grouping") or "pattern_key").strip()
        if grouping_type not in {"pattern_key", "namespace", "query_hash"}:
            grouping_type = "pattern_key"

        exclude_values = request.args.getlist("exclude_system_db")
        if not exclude_values:
            exclude_system_db = True
        else:
            exclude_system_db = "1" in exclude_values

        limit_param = request.args.get("limit")
        if limit_param == "all":
            requested_limit = SLOWQ_EXPORT_MAX_LIMIT
        elif limit_param is None or limit_param == "":
            requested_limit = SLOWQ_EXPORT_DEFAULT_LIMIT
        else:
            try:
                requested_limit = int(limit_param)
            except (TypeError, ValueError):
                requested_limit = SLOWQ_EXPORT_DEFAULT_LIMIT
        export_limit = max(1, min(requested_limit, SLOWQ_EXPORT_MAX_LIMIT))

        analysis = service.analyze_slow_query_patterns(
            grouping=grouping_type,
            start_ts=start_ts,
            end_ts=end_ts,
            exclude_system=exclude_system_db,
            page=1,
            per_page=export_limit,
            order_by="total_duration_ms",
            order_dir="desc",
        )

        patterns = analysis["items"]

        def _dt(value: datetime | None) -> str | None:
            if value is None:
                return None
            return value.isoformat()

        export_payload = {
            "generated_on": datetime.utcnow().isoformat() + "Z",
            "grouping": grouping_type,
            "total_patterns": analysis["total_groups"],
            "summary": {
                "total_executions": analysis["total_executions"],
                "high_priority_issues": analysis["high_priority_count"],
                "avg_duration_ms": analysis["avg_duration_ms"],
            },
            "filters": {
                "start_iso": start_iso_value,
                "end_iso": end_iso_value,
                "exclude_system_db": exclude_system_db,
                "limit": export_limit,
            },
            "patterns": [],
        }

        for pattern in patterns:
            avg_duration = float(pattern.get("avg_duration", 0) or 0.0)
            min_duration = float(pattern.get("min_duration", 0) or 0.0)
            max_duration = float(pattern.get("max_duration", 0) or 0.0)
            median_duration = float(pattern.get("median_duration", 0) or 0.0)
            export_payload["patterns"].append(
                {
                    "pattern_key": pattern.get("pattern_key"),
                    "database": pattern.get("database"),
                    "collection": pattern.get("collection"),
                    "query_hash": pattern.get("query_hash"),
                    "query_type": pattern.get("query_type"),
                    "plan_summary": pattern.get("plan_summary"),
                    "complexity_score": pattern.get("complexity_score"),
                    "optimization_potential": pattern.get(
                        "optimization_potential", "low"
                    ),
                    "performance_stats": {
                        "total_executions": pattern.get("total_count", 0),
                        "avg_duration_ms": round(avg_duration),
                        "min_duration_ms": round(min_duration),
                        "max_duration_ms": round(max_duration),
                        "median_duration_ms": round(median_duration),
                    },
                    "efficiency_metrics": {
                        "total_docs_examined": pattern.get(
                            "total_docs_examined", 0
                        ),
                        "total_keys_examined": pattern.get(
                            "total_keys_examined", 0
                        ),
                        "total_returned": pattern.get("total_returned", 0),
                        "avg_selectivity_percent": round(
                            float(pattern.get("avg_selectivity", 0) or 0.0), 2
                        ),
                        "avg_index_efficiency_percent": round(
                            float(pattern.get("avg_index_efficiency", 0) or 0.0),
                            2,
                        ),
                    },
                    "sample_query": pattern.get("sample_query"),
                    "slowest_query": pattern.get("slowest_query_full"),
                    "first_seen": _dt(pattern.get("first_seen")),
                    "last_seen": _dt(pattern.get("last_seen")),
                    "slowest_execution_timestamp": _dt(
                        pattern.get("slowest_execution_timestamp")
                    ),
                }
            )

        timestamp_label = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"slow_query_analysis_{timestamp_label}.json"
        data = json.dumps(export_payload, indent=2)
        response = app.response_class(data, mimetype="application/json")
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        return response

    @app.route("/workload-summary", endpoint="workload_summary")
    @app.route("/workload-summary/v2", endpoint="workload_summary_v2")
    def workload_summary():
        service = _get_duckdb_service()

        summary = service.get_workload_summary(limit=10)
        stats = summary.get("stats", {})

        def map_entries(entries: List[Dict[str, Any]], *, read_key: str | None = None) -> List[Dict[str, Any]]:
            mapped: List[Dict[str, Any]] = []
            for entry in entries:
                database = entry.get("database") or "unknown"
                collection = entry.get("collection") or "unknown"
                plan_summary = entry.get("plan_summary") or "None"
                executions = int(entry.get("executions") or entry.get("query_count") or 0)
                avg_duration = entry.get("avg_duration_ms")
                if avg_duration is None and executions:
                    total_ms = entry.get("total_duration_ms")
                    if total_ms is not None:
                        avg_duration = float(total_ms) / max(executions, 1)
                avg_duration_val = int(round(float(avg_duration))) if avg_duration is not None else 0

                mapped.append(
                    {
                        "database": database,
                        "collection": collection,
                        "plan_summary": plan_summary,
                        "docs_examined": int(entry.get("total_docs_examined") or 0),
                        "docs_returned": int(entry.get("total_docs_returned") or 0),
                        "keys_examined": int(entry.get("total_keys_examined") or 0),
                        "total_duration_ms": int(entry.get("total_duration_ms") or 0),
                        "avg_duration": avg_duration_val,
                        "query_count": executions,
                        "total_bytes_read": int(entry.get(read_key) or entry.get("total_docs_examined") or 0) if read_key else int(entry.get("total_docs_examined") or 0),
                    }
                )
            return mapped

        top_docs = map_entries(summary.get("top_docs_examined", []))
        top_keys = map_entries(summary.get("top_io_time", []))

        top_duration_entries = summary.get("top_duration", [])
        top_cpu = []
        for entry in map_entries(top_duration_entries):
            total_duration_ms = entry.get("total_duration_ms", 0)
            entry["total_cpu_nanos"] = int(total_duration_ms) * 1_000_000
            top_cpu.append(entry)

        top_bytes = []
        for entry in map_entries(summary.get("top_docs_examined", [])):
            entry["total_bytes_read"] = entry.pop("total_bytes_read", entry.get("docs_examined", 0))
            top_bytes.append(entry)

        top_mem = []
        for entry in map_entries(summary.get("top_docs_returned", [])):
            entry["total_bytes_written"] = entry.get("docs_returned", 0)
            top_mem.append(entry)

        context = {
            "top_docs": top_docs,
            "top_cpu": top_cpu,
            "top_keys": top_keys,
            "top_bytes": top_bytes,
            "top_mem": top_mem,
            "workload_stats": stats,
        }

        return render_template("workload_summary.html", **context)
    @app.route("/export-index-suggestions")
    def export_index_suggestions():
        service = _get_duckdb_service()

        date_offsets = service.get_date_offset_map()
        time_filters = _resolve_time_filters(
            request.args, service=service, date_offsets=date_offsets
        )
        start_dt = time_filters["start_dt"]
        end_dt = time_filters["end_dt"]
        start_iso_value = time_filters["start_iso"]
        end_iso_value = time_filters["end_iso"]
        start_epoch_hint = time_filters.get("start_epoch")
        end_epoch_hint = time_filters.get("end_epoch")

        start_ts = (
            start_epoch_hint
            if isinstance(start_epoch_hint, int)
            else (int(start_dt.timestamp()) if start_dt else None)
        )
        end_ts = (
            end_epoch_hint
            if isinstance(end_epoch_hint, int)
            else (int(end_dt.timestamp()) if end_dt else None)
        )

        exclude_values = request.args.getlist("exclude_system_db")
        if not exclude_values:
            exclude_system_db = True
        else:
            exclude_system_db = "1" in exclude_values

        suggestions_data = service.generate_index_suggestions(
            start_ts=start_ts,
            end_ts=end_ts,
            exclude_system=exclude_system_db,
        )

        collections = suggestions_data.get("collections", {})

        export_payload = {
            "generated_on": datetime.utcnow().isoformat() + "Z",
            "total_collections": len(collections),
            "filters": {
                "start_iso": start_iso_value,
                "end_iso": end_iso_value,
                "exclude_system_db": exclude_system_db,
            },
            "index_commands": [],
        }

        for collection_name, data in collections.items():
            commands = []
            for suggestion in data.get("suggestions", []):
                commands.append(
                    {
                        "priority": suggestion.get("priority", "high"),
                        "reason": suggestion.get("reason", "Index suggestion"),
                        "justification": suggestion.get("justification", ""),
                        "command": suggestion.get("command", ""),
                        "index": suggestion.get("index", ""),
                        "fields": suggestion.get("fields", []),
                        "confidence": suggestion.get("confidence", "high"),
                        "impact_score": suggestion.get("impact_score", 0),
                        "occurrences": suggestion.get("occurrences", 0),
                        "avg_duration_ms": suggestion.get("avg_duration_ms", 0),
                        "inefficiency_ratio": suggestion.get("inefficiency_ratio"),
                        "how_to_validate": "Create as hidden index, run explain() to confirm usage, then unhide if beneficial.",
                    }
                )

            reviews = data.get("reviews", [])
            if commands or reviews:
                export_payload["index_commands"].append(
                    {
                        "collection": collection_name,
                        "collscan_queries": data.get("collscan_queries", 0),
                        "ixscan_ineff_queries": data.get("ixscan_ineff_queries", 0),
                        "avg_duration_ms": int(data.get("avg_duration", 0)),
                        "avg_docs_per_query": int(data.get("avg_docs_per_query", 0)),
                        "commands": commands,
                        "reviews": reviews,
                    }
                )

        timestamp_label = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"mongodb_index_suggestions_{timestamp_label}.json"
        data = json.dumps(export_payload, indent=2)
        response = app.response_class(data, mimetype="application/json")
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        return response
    @app.route("/current-op", methods=["GET", "POST"], endpoint="current_op")
    @app.route("/current-op-analyzer", methods=["GET", "POST"], endpoint="current_op_analyzer")
    def current_op():
        analysis: dict[str, object] | None = None
        original_data = ""

        if request.method == "POST":
            data_text = request.form.get("current_op_data", "")
            upload = request.files.get("currentop_file")
            if not data_text and upload and upload.filename:
                try:
                    data_bytes = upload.read()
                    data_text = data_bytes.decode("utf-8", errors="ignore")
                except Exception:
                    data_text = ""

            data_text = data_text.strip()
            original_data = data_text

            threshold_value = request.form.get("threshold")
            try:
                threshold_int = max(int(threshold_value), 1)
            except (TypeError, ValueError):
                threshold_int = 30

            filter_state = {
                "threshold": threshold_int,
                "only_active": request.form.get("only_active") is not None,
                "only_waiting": request.form.get("only_waiting") is not None,
                "only_collscan": request.form.get("only_collscan") is not None,
            }

            if data_text:
                try:
                    analysis = analyze_current_op_v2(
                        data_text,
                        threshold=threshold_int,
                        filters=filter_state,
                    )
                except json.JSONDecodeError as exc:
                    analysis = {"error": f"Failed to parse JSON: {exc}"}
                except Exception as exc:
                    analysis = {"error": str(exc)}
            else:
                analysis = {"error": "Please provide db.currentOp() output to analyze."}

            if analysis is not None:
                existing_filters = analysis.get("filters") if isinstance(analysis, dict) else None
                if not isinstance(existing_filters, dict):
                    analysis["filters"] = filter_state
                else:
                    existing_filters.update(filter_state)

        return render_template(
            "current_op_analyzer.html",
            analysis=analysis,
            original_data=original_data,
        )

    @app.route("/current-op/v2", endpoint="current_op_v2")
    def current_op_v2_redirect():
        return redirect(url_for("current_op"))

    @app.route("/search-logs", endpoint="search_logs")
    @app.route("/search-logs/v2", endpoint="search_logs_v2")
    def search_logs():
        service = _get_duckdb_service()

        limit_param = (request.args.get("limit") or "100").strip()
        try:
            limit = max(1, min(int(limit_param), 1000))
        except (ValueError, TypeError):
            limit = 100

        conditions, keyword, field_name, field_value = _parse_search_conditions(request.args)

        start_date_str = request.args.get("start_date")
        end_date_str = request.args.get("end_date")

        start_dt = _parse_datetime(start_date_str, is_end=False)
        end_dt = _parse_datetime(end_date_str, is_end=True)
        start_ts = int(start_dt.timestamp()) if start_dt else None
        end_ts = int(end_dt.timestamp()) if end_dt else None

        min_date, max_date = service.get_available_date_range()

        search_performed = bool(
            keyword
            or field_name
            or field_value
            or start_date_str
            or end_date_str
            or conditions
        )

        dataset_root = Path(app.config["SLOWQ_DATASET_ROOT"])
        results: List[Dict[str, Any]] = []
        heavy_search_warning = False
        total_found_exact = True
        error_message = None

        if search_performed:
            try:
                results = _search_log_entries(dataset_root, conditions, start_ts, end_ts, limit)
                if len(results) >= limit:
                    heavy_search_warning = True
                    total_found_exact = False
            except re.error as exc:
                error_message = f"Invalid search pattern: {exc}"
                results = []
        
        result_count = len(results)

        context = {
            "results": results,
            "result_count": result_count,
            "limit": limit,
            "search_performed": search_performed,
            "heavy_search_warning": heavy_search_warning,
            "total_found_exact": total_found_exact,
            "error_message": error_message,
            "start_date": start_date_str or "",
            "end_date": end_date_str or "",
            "min_date": _format_datetime_for_input(min_date),
            "max_date": _format_datetime_for_input(max_date),
            "pagination": None,
        }

        return render_template("search_logs.html", **context)
    @app.route("/export/search-results")
    def export_search_results():
        all_conditions, keyword, field_name, field_value = _parse_search_conditions(request.args)

        start_date_str = request.args.get("start_date")
        end_date_str = request.args.get("end_date")
        start_dt = _parse_datetime(start_date_str, is_end=False)
        end_dt = _parse_datetime(end_date_str, is_end=True)
        start_ts = int(start_dt.timestamp()) if start_dt else None
        end_ts = int(end_dt.timestamp()) if end_dt else None

        dataset_root = Path(app.config["SLOWQ_DATASET_ROOT"])
        results = _search_log_entries(dataset_root, all_conditions, start_ts, end_ts, 5000)

        payload = {
            "exported_at": datetime.utcnow().isoformat() + "Z",
            "count": len(results),
            "items": [
                {
                    "timestamp": item["timestamp"].isoformat() if item.get("timestamp") else None,
                    "raw": item.get("raw_line"),
                }
                for item in results
            ],
        }

        data = json.dumps(payload, indent=2)
        response = app.response_class(data, mimetype="application/json")
        response.headers["Content-Disposition"] = "attachment; filename=search_results.json"
        return response

    @app.route("/search-user-access", endpoint="search_user_access")
    @app.route("/search-user-access/v2", endpoint="search_user_access_v2")
    def search_user_access():
        service = _get_duckdb_service()
        conn = service.connection

        try:
            auth_cols = conn.execute("PRAGMA table_info('authentications')").fetchall()
            has_remote_port = any(row[1] == "remote_port" for row in auth_cols)
        except Exception:
            has_remote_port = False

        start_date_str = request.args.get("start_date") or ""
        end_date_str = request.args.get("end_date") or ""
        filter_type = request.args.get("filter_type") or ""
        filter_value_raw = (request.args.get("filter_value") or "").strip()
        filter_value = filter_value_raw
        use_regex = request.args.get("use_regex") in {"on", "true", "1"}

        start_dt = _parse_datetime(start_date_str, is_end=False)
        end_dt = _parse_datetime(end_date_str, is_end=True)
        start_ts = int(start_dt.timestamp()) if start_dt else None
        end_ts = int(end_dt.timestamp()) if end_dt else None

        exclude_param = request.args.get("exclude_system")
        if not request.args:
            exclude_system = True
        elif exclude_param is None:
            # Form was submitted with the checkbox unchecked
            exclude_system = False
        else:
            exclude_system = exclude_param.lower() not in {"0", "false", "off"}

        filters_dict = {
            "start_date": start_date_str,
            "end_date": end_date_str,
            "filter_type": filter_type,
            "filter_value": filter_value_raw,
            "use_regex": "on" if use_regex else None,
            "exclude_system": exclude_system,
        }

        if not getattr(service, "_available_views", {}).get("authentications"):
            user_access_data = {
                "items": [],
                "page": 1,
                "per_page": 50,
                "total": 0,
                "pages": 1,
                "has_prev": False,
                "has_next": False,
                "prev_num": None,
                "next_num": None,
            }

            context = {
                "user_access_data": user_access_data,
                "filters": filters_dict,
                "available_mechanisms": [],
                "available_ips": [],
                "available_users": [],
                "available_auth_statuses": [],
                "has_remote_port": False,
            }

            return render_template("search_user_access.html", **context)

        clauses: List[str] = []
        params: List[Any] = []

        if start_ts is not None:
            clauses.append("ts_epoch >= ?")
            params.append(start_ts)
        if end_ts is not None:
            clauses.append("ts_epoch <= ?")
            params.append(end_ts)
        if exclude_system:
            placeholders = ", ".join(["?"] * len(SYSTEM_USERS))
            clauses.append(f"COALESCE(user, '') NOT IN ({placeholders})")
            params.extend(SYSTEM_USERS)

        regex_pattern = None
        filter_column = None

        if filter_type == "ip_address" and filter_value:
            filter_column = "remote_address"
        elif filter_type == "username" and filter_value:
            filter_column = "user"
        elif filter_type == "mechanism" and filter_value:
            filter_column = "mechanism"
        elif filter_type == "auth_status" and filter_value:
            filter_column = "result"
            normalized = filter_value.lower()
            if not use_regex:
                status_map = {
                    "success": "auth_success",
                    "auth_success": "auth_success",
                    "succeeded": "auth_success",
                    "passed": "auth_success",
                    "fail": "auth_failed",
                    "failed": "auth_failed",
                    "failure": "auth_failed",
                    "auth_failed": "auth_failed",
                    "denied": "auth_failed",
                }
                filter_value = status_map.get(normalized, filter_value)

        if filter_column and filter_value and not use_regex:
            clauses.append(f"LOWER(COALESCE({filter_column}, '')) LIKE ?")
            params.append(f"%{filter_value.lower()}%")

        where_clause = " WHERE " + " AND ".join(clauses) if clauses else ""

        try:
            regex_pattern = re.compile(filter_value) if use_regex and filter_column and filter_value else None
        except re.error as exc:
            flash(f"Invalid regular expression: {exc}", "warning")
            regex_pattern = None

        page = request.args.get("page", default=1, type=int)
        if page is None or page < 1:
            page = 1
        per_page = 50

        total_matches = 0
        user_access_rows: List[Dict[str, Any]] = []

        select_fields = (
            "timestamp, ts_epoch, user, database, mechanism, result, connection_id, remote_address"
        )
        if has_remote_port:
            select_fields += ", remote_port"
        select_fields += ", app_name, error"

        if regex_pattern and filter_column:
            raw_query = (
                f"SELECT {select_fields} "
                "FROM authentications {where_clause} ORDER BY ts_epoch DESC"
            ).format(where_clause=where_clause)
            rows = conn.execute(raw_query, params).fetchall()
            columns = [desc[0] for desc in conn.description]
            filtered = []
            for row in rows:
                record = dict(zip(columns, row))
                value = record.get(filter_column) or ""
                if regex_pattern.search(str(value)):
                    filtered.append(record)
            total_matches = len(filtered)
            start_index = (page - 1) * per_page
            end_index = start_index + per_page
            user_access_rows = filtered[start_index:end_index]
        else:
            count_query = "SELECT COUNT(*) FROM authentications {where_clause}".format(where_clause=where_clause)
            total_row = conn.execute(count_query, params).fetchone()
            total_matches = int(total_row[0]) if total_row and total_row[0] is not None else 0

            offset = (page - 1) * per_page
            data_query = (
                f"SELECT {select_fields} "
                "FROM authentications {where_clause} ORDER BY ts_epoch DESC LIMIT ? OFFSET ?"
            ).format(where_clause=where_clause)
            data_params = params + [per_page, offset]
            rows = conn.execute(data_query, data_params).fetchall()
            columns = [desc[0] for desc in conn.description]
            user_access_rows = [dict(zip(columns, row)) for row in rows]

        for entry in user_access_rows:
            timestamp_value = entry.get("timestamp")
            if isinstance(timestamp_value, str):
                parsed = _parse_iso_datetime(timestamp_value)
                entry["last_seen"] = parsed if parsed is not None else timestamp_value
            else:
                entry["last_seen"] = timestamp_value

            entry["username"] = entry.get("user") or "unknown"
            entry["auth_mechanism"] = entry.get("mechanism") or "unknown"
            address_raw = entry.get("remote_address") or ""
            entry["ip_address"] = address_raw if address_raw else "unknown"
            port_value = entry.get("remote_port") if has_remote_port else None
            try:
                entry["remote_port"] = int(port_value) if port_value is not None else None
            except (TypeError, ValueError):
                entry["remote_port"] = None

        total_pages = max(1, math.ceil(total_matches / per_page) if per_page else 1)
        if page > total_pages:
            page = total_pages

        user_access_data = {
            "items": user_access_rows,
            "page": page,
            "per_page": per_page,
            "total": total_matches,
            "pages": total_pages,
            "has_prev": page > 1,
            "has_next": page < total_pages,
            "prev_num": page - 1 if page > 1 else None,
            "next_num": page + 1 if page < total_pages else None,
        }

        def _distinct_values(column: str, *, coalesce: str = 'unknown') -> List[str]:
            base_query = f"SELECT DISTINCT COALESCE({column}, '{coalesce}') FROM authentications"
            if where_clause:
                base_query = f"{base_query} {where_clause}"
            base_query += " ORDER BY 1"
            return [row[0] for row in conn.execute(base_query, params).fetchall()]

        available_mechanisms = _distinct_values("mechanism")
        available_ips = _distinct_values("remote_address")
        available_users = _distinct_values("user")
        available_auth_statuses = _distinct_values("result")

        context = {
            "user_access_data": user_access_data,
            "filters": filters_dict,
            "available_mechanisms": available_mechanisms,
            "available_ips": available_ips,
            "available_users": available_users,
            "available_auth_statuses": available_auth_statuses,
            "has_remote_port": has_remote_port,
        }

        return render_template("search_user_access.html", **context)

    @app.route("/index-suggestions", endpoint="index_suggestions")
    @app.route("/index-suggestions/v2", endpoint="index_suggestions_v2")
    def index_suggestions():
        service = _get_duckdb_service()

        date_offsets = service.get_date_offset_map()
        time_filters = _resolve_time_filters(
            request.args, service=service, date_offsets=date_offsets
        )
        start_dt = time_filters["start_dt"]
        end_dt = time_filters["end_dt"]
        start_iso_value = time_filters.get("start_iso")
        end_iso_value = time_filters.get("end_iso")
        start_epoch_hint = time_filters.get("start_epoch")
        end_epoch_hint = time_filters.get("end_epoch")

        start_ts = (
            start_epoch_hint
            if isinstance(start_epoch_hint, int)
            else (int(start_dt.timestamp()) if start_dt else None)
        )
        end_ts = (
            end_epoch_hint
            if isinstance(end_epoch_hint, int)
            else (int(end_dt.timestamp()) if end_dt else None)
        )

        selected_db = (request.args.get("database") or "all").strip() or "all"
        selected_namespace = (request.args.get("namespace") or "").strip()

        exclude_values = request.args.getlist("exclude_system_db")
        if not exclude_values:
            exclude_system_db = True
        else:
            exclude_system_db = "1" in exclude_values

        suggestions_data = service.generate_index_suggestions(
            start_ts=start_ts,
            end_ts=end_ts,
            exclude_system=exclude_system_db,
            database=None if selected_db.lower() == "all" else selected_db,
            namespace=selected_namespace or None,
        )

        collections = suggestions_data.get("collections") or {}
        top_suggestions = suggestions_data.get("top_suggestions") or []
        summary = suggestions_data.get("summary") or {}

        for collection_data in collections.values():
            samples = collection_data.get("sample_queries") or []
            if isinstance(samples, list):
                for sample in samples:
                    if not isinstance(sample, dict):
                        continue
                    timestamp_value = sample.get("timestamp")
                    if isinstance(timestamp_value, str):
                        parsed = _parse_iso_datetime(timestamp_value)
                        sample["timestamp"] = parsed if parsed is not None else timestamp_value

        context = {
            "suggestions": collections,
            "top_suggestions": top_suggestions,
            "total_collscan_queries": int(summary.get("total_collscan_queries") or 0),
            "total_suggestions": int(summary.get("total_suggestions") or 0),
            "avg_docs_examined": float(summary.get("avg_docs_examined") or 0.0),
            "filters": {
                "start_iso": start_iso_value,
                "end_iso": end_iso_value,
                "database": selected_db,
                "namespace": selected_namespace,
                "exclude_system_db": exclude_system_db,
            },
        }

        return render_template("index_suggestions.html", **context)
    @app.route("/upload/v2", methods=["GET", "POST"])
    def upload_v2():
        dataset_root = Path(app.config["SLOWQ_DATASET_ROOT"])
        dataset_root.mkdir(parents=True, exist_ok=True)

        if request.method == "POST":
            files = request.files.getlist("files")
            if not files or all(not f.filename for f in files):
                flash("Please select at least one file to upload.", "warning")
                return redirect(url_for("upload_v2"))

            temp_dir = Path(tempfile.mkdtemp(prefix="slowq_upload_"))
            try:
                ingested = process_uploads(files, dataset_root=dataset_root, temp_dir=temp_dir)
                if not ingested:
                    flash("No valid log files found in upload.", "warning")
                else:
                    for entry in ingested:
                        source_path = entry.get("input_path") or "uploaded file"
                        try:
                            source_display = Path(source_path).name
                        except Exception:
                            source_display = str(source_path)
                        segments = []
                        for key, label in (
                            ("slow_queries", "slow"),
                            ("authentications", "auth"),
                            ("connections", "conn"),
                        ):
                            info = entry.get(key) or {}
                            rows = info.get("rows_written", 0)
                            if rows:
                                path = info.get("path")
                                if path:
                                    segments.append(
                                        f"{label}={rows} rows → {Path(path).name}"
                                    )
                                else:
                                    segments.append(f"{label}={rows} rows")
                        if not segments:
                            segments.append("no rows persisted")
                        message = f"Ingested {source_display}: " + ", ".join(segments)
                        flash(
                            message,
                            "success",
                        )
                        row_counts = entry.get("row_counts") or {}
                        timings = entry.get("timings") or {}
                        counts_message = (
                            " • Slow queries={slow} auth={auth} conn={conn}"
                        ).format(
                            slow=row_counts.get("slow_queries", 0),
                            auth=row_counts.get("authentications", 0),
                            conn=row_counts.get("connections", 0),
                        )
                        duration = timings.get("parse_seconds", 0) + timings.get("write_seconds", 0) + timings.get("finalize_seconds", 0)
                        flash(
                            f"Processed {source_display}{counts_message} in {duration:.2f}s",
                            "info",
                        )
                    total_counts = {
                        "slow_queries": sum((entry.get("row_counts", {}).get("slow_queries", 0) for entry in ingested)),
                        "authentications": sum((entry.get("row_counts", {}).get("authentications", 0) for entry in ingested)),
                        "connections": sum((entry.get("row_counts", {}).get("connections", 0) for entry in ingested)),
                    }
                    flash(
                        "Total rows written → slow={slow_queries} auth={authentications} conn={connections}".format(**total_counts),
                        "success",
                    )
                    existing_service = getattr(app, "slowq_duckdb_service", None)
                    if existing_service is not None:
                        try:
                            existing_service.refresh()
                        except Exception as refresh_error:
                            flash(f"Dataset refresh failed: {refresh_error}", "error")
            except ValueError as exc:
                flash(str(exc), "error")
            except Exception as exc:
                flash(f"Upload failed: {exc}", "error")
            finally:
                shutil.rmtree(temp_dir, ignore_errors=True)

            return redirect(url_for("dashboard"))

        manifest = load_manifest(dataset_root / "manifest.json") or {}
        return render_template("upload.html", manifest=manifest)

    app.add_url_rule("/upload", "upload", upload_v2, methods=["GET", "POST"])

    @app.route("/processing-status")
    def processing_status():
        status = get_processing_status()
        try:
            service = _get_duckdb_service()
        except Exception:  # pragma: no cover - ensure status endpoint survives failures
            return jsonify(status)

        dataset_info: dict[str, object] = {
            "dataset_root": str(service.dataset_root),
            "available_views": dict(service._available_views),
            "manifest": service.get_manifest_info(),
        }

        try:
            dataset_info["recent_ingests"] = service.list_ingests()[:5]
        except Exception:
            dataset_info["recent_ingests"] = []

        current_ingest = status.get("current_ingest") or {}
        status["ingest_phase"] = current_ingest.get("phase", "idle")
        status["ingest_file"] = current_ingest.get("file")
        status["ingest_started_at"] = current_ingest.get("started_at")
        status["ingest_updated_at"] = current_ingest.get("updated_at")
        status["dataset"] = dataset_info
        status["ready"] = status.get("current_ingest") is None and service._available_views.get("slow_queries", False)

        return jsonify(status)

    return app


app = create_app()


if __name__ == "__main__":
    dataset_root = Path(app.config["SLOWQ_DATASET_ROOT"]).resolve()
    print(f"Serving v2 analyzer with dataset root: {dataset_root}")
    app.run(debug=True)
