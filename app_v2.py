"""Flask application entrypoint for the v2 Parquet/DuckDB backend."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal
from collections import OrderedDict
import math
import re
import json
from pathlib import Path
from typing import Mapping

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
        return render_template("slow_query_analysis_v2.html")

    @app.route("/")
    def index():
        return redirect(url_for("dashboard"))

    @app.route("/dashboard", endpoint="dashboard")
    @app.route("/dashboard/v2", endpoint="dashboard_v2")
    def dashboard_v2():
        return render_template("dashboard_v2.html")
    @app.route("/query-trend", endpoint="query_trend")
    @app.route("/query-trend/v2", endpoint="query_trend_v2")
    def query_trend():
        return render_template("query_trend_v2.html")
    @app.route("/slow-queries", endpoint="slow_queries")
    @app.route("/slow-queries/v2", endpoint="slow_queries_v2")
    def slow_queries():
        return render_template("slow_queries_v2.html")

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
        return render_template("workload_summary_v2.html")
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
    @app.route("/current-op", endpoint="current_op")
    @app.route("/current-op/v2", endpoint="current_op_v2")
    def current_op():
        return render_template("current_op_v2.html")

    @app.route("/search-logs", endpoint="search_logs")
    @app.route("/search-logs/v2", endpoint="search_logs_v2")
    def search_logs():
        return render_template("search_logs_v2.html")
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
        return render_template("search_user_access_v2.html")

    @app.route("/index-suggestions", endpoint="index_suggestions")
    @app.route("/index-suggestions/v2", endpoint="index_suggestions_v2")
    def index_suggestions():
        return render_template("index_suggestions_v2.html")
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
        return render_template("upload_v2.html", manifest=manifest)

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
def _search_log_entries(dataset_root: Path, conditions: list[dict], start_ts: int | None, end_ts: int | None, limit: int) -> list[dict]:
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
            for line in handle:
                raw_line = line.strip()
                if not raw_line or not raw_line.startswith("{"):
                    continue
                if compiled_keywords and not keyword_match(raw_line):
                    continue
                try:
                    entry = json.loads(raw_line)
                except json.JSONDecodeError:
                    continue

                ts_str = entry.get("t", {}).get("$date") if isinstance(entry.get("t"), dict) else None
                ts_dt = _parse_iso_datetime(ts_str) if isinstance(ts_str, str) else None
                ts_epoch = int(ts_dt.timestamp()) if ts_dt else None

                if start_ts is not None and (ts_epoch is None or ts_epoch < start_ts):
                    continue
                if end_ts is not None and (ts_epoch is None or ts_epoch > end_ts):
                    continue

                field_ok = True
                for cond in field_conditions:
                    field_path = cond.get("name", "")
                    value = cond.get("value", "")
                    if not field_path or not value:
                        continue
                    target_value = _get_field(entry, field_path)
                    regex = cond.get("regex", False)
                    case = cond.get("case_sensitive", False)
                    negate = cond.get("negate", False)
                    match = False
                    if target_value is not None:
                        if regex:
                            try:
                                pattern = re.compile(value, 0 if case else re.IGNORECASE)
                                match = bool(pattern.search(target_value))
                            except re.error:
                                match = False
                        else:
                            haystack = target_value if case else target_value.lower()
                            needle = value if case else value.lower()
                            match = needle in haystack
                    if match == negate:
                        field_ok = False
                        break
                if not field_ok:
                    continue

                results.append({"timestamp": ts_dt, "raw_line": raw_line})
                if len(results) >= count_limit:
                    return results

    return results


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
