"""Flask blueprint exposing v2 analytics endpoints."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from flask import Blueprint, current_app, jsonify, request, Response

from ..analytics import DuckDBService
from ..config import settings
from ..utils.logging_utils import get_logger
from ..current_op import analyze_current_op
from ..search import search_logs

LOGGER = get_logger("web.routes")


def _get_service() -> DuckDBService:
    service: DuckDBService | None = getattr(current_app, "slowq_duckdb_service", None)
    if service is not None:
        try:
            service.connection.execute("SELECT 1")
        except Exception:
            try:
                service.close()
            except Exception:
                LOGGER.debug("Failed to close stale DuckDB connection", exc_info=True)
            service = None
            current_app.slowq_duckdb_service = None
    if service is None:
        dataset_root = Path(current_app.config.get("SLOWQ_DATASET_ROOT", settings.output_root))
        LOGGER.info("Instantiating DuckDBService for dataset %s", dataset_root)
        service = DuckDBService(dataset_root=dataset_root)
        current_app.slowq_duckdb_service = service
    return service


bp = Blueprint("slowq_v2", __name__, url_prefix="/v2")


@bp.route("/slow-query/summary")
def slow_query_summary() -> Any:
    service = _get_service()
    limit = int(request.args.get("limit", 50))
    filters = _extract_filters(request)
    summary = service.get_namespace_summary(limit=limit, filters=filters)
    plan_mix = service.get_plan_summary(limit=limit, filters=filters)
    operation_mix = service.get_operation_mix(filters=filters)
    recent = service.get_recent_slow_queries(limit=20, filters=filters)
    return jsonify(
        {
            "namespaces": summary,
            "plan_mix": plan_mix,
            "operations": operation_mix,
            "recent": recent,
        }
    )


@bp.route("/slow-query/search")
def slow_query_search() -> Any:
    service = _get_service()
    text = request.args.get("q") or ""
    limit = int(request.args.get("limit", 200))
    filters = _extract_filters(request)
    results = service.search_slow_queries(text=text, limit=limit, filters=filters)
    return jsonify({"results": results})


@bp.route("/slow-query/list")
def slow_query_list() -> Any:
    service = _get_service()
    limit = int(request.args.get("limit", 200))
    filters = _extract_filters(request)
    rows = service.get_recent_slow_queries(limit=limit, filters=filters)
    return jsonify({"results": rows})


@bp.route("/slow-query/details/<query_hash>")
def slow_query_details(query_hash: str) -> Any:
    service = _get_service()
    limit = int(request.args.get("limit", 5))
    examples = service.get_query_examples(query_hash, limit=limit)
    return jsonify({"query_hash": query_hash, "examples": examples})


@bp.route("/auth/summary")
def auth_summary() -> Any:
    service = _get_service()
    filters = _extract_filters(request)
    summary = service.get_authentication_summary(filters=filters)
    return jsonify({"authentications": summary})


@bp.route("/connections/activity")
def connection_activity() -> Any:
    service = _get_service()
    filters = _extract_filters(request)
    activity = service.get_connection_activity(filters=filters)
    return jsonify({"connections": activity})


@bp.route("/manifest/ingests")
def manifest_ingests() -> Any:
    service = _get_service()
    return jsonify({"ingests": service.list_ingests()})


@bp.route("/workload/summary")
def workload_summary() -> Any:
    service = _get_service()
    filters = _extract_filters(request)
    limit = int(request.args.get("limit", 10))
    summary = service.get_workload_summary(limit=limit, filters=filters)
    return jsonify(summary)


@bp.route("/current-op/analyze", methods=["POST"])
def current_op_analyze() -> Any:
    payload = request.get_json(silent=True) or {}
    data = payload.get("data", "")
    if not data:
        return jsonify({"error": "No currentOp data provided"}), 400

    threshold = int(payload.get("threshold", 30) or 30)
    filters = payload.get("filters") or {}
    result = analyze_current_op(data, threshold=threshold, filters=filters)
    return jsonify(result)


@bp.route("/query-trend/summary")
def query_trend_summary() -> Any:
    service = _get_service()
    grouping = request.args.get("grouping", "namespace")
    limit = int(request.args.get("limit", 50))
    filters = _extract_filters(request)
    summary = service.get_trend_summary(grouping=grouping, filters=filters, limit=limit)
    series = service.get_trend_series(grouping=grouping, filters=filters, limit=500)
    return jsonify({"summary": summary, "series": series})


@bp.route("/logs/search")
def logs_search() -> Any:
    dataset_root = Path(current_app.config.get("SLOWQ_DATASET_ROOT", settings.output_root))
    text = request.args.get("text", "")
    limit = int(request.args.get("limit", 100))
    start_ts = request.args.get("start_ts")
    end_ts = request.args.get("end_ts")
    regex = request.args.get("regex")
    case_sensitive = request.args.get("case_sensitive") in {"1", "true", "on"}

    try:
        start_ts_val = int(start_ts) if start_ts else None
    except (TypeError, ValueError):
        start_ts_val = None
    try:
        end_ts_val = int(end_ts) if end_ts else None
    except (TypeError, ValueError):
        end_ts_val = None

    results = search_logs(
        dataset_root=dataset_root,
        text=text,
        limit=limit,
        start_ts=start_ts_val,
        end_ts=end_ts_val,
        regex=regex,
        case_sensitive=case_sensitive,
    )
    return jsonify({"results": results})


@bp.route("/auth/logs")
def auth_logs() -> Any:
    service = _get_service()
    filters = _extract_filters(request)
    limit = int(request.args.get("limit", 200))
    records = service.get_authentication_records(filters=filters, limit=limit)
    return jsonify({"results": records})


@bp.route("/index-suggestions")
def index_suggestions() -> Any:
    service = _get_service()
    filters = _extract_filters(request)
    limit = int(request.args.get("limit", 50))
    suggestions = service.get_index_suggestions(limit=limit, filters=filters)
    return jsonify({"suggestions": suggestions})


@bp.route("/export/slow-queries")
def export_slow_queries() -> Response:
    service = _get_service()
    view = request.args.get("view", "executions")
    limit = int(request.args.get("limit", 5000))
    filters = _extract_filters(request)
    payload = service.export_slow_queries(view=view, limit=limit, filters=filters)
    filename = f"slow_queries_{view}.json"
    return _attachment_response(payload, filename)


@bp.route("/export/query-analysis")
def export_query_analysis() -> Response:
    service = _get_service()
    grouping = request.args.get("grouping", "namespace")
    limit = int(request.args.get("limit", 500))
    filters = _extract_filters(request)
    payload = service.export_query_analysis(grouping=grouping, limit=limit, filters=filters)
    filename = f"query_analysis_{grouping}.json"
    return _attachment_response(payload, filename)


@bp.route("/export/search-logs")
def export_search_logs() -> Response:
    dataset_root = Path(current_app.config.get("SLOWQ_DATASET_ROOT", settings.output_root))
    text = request.args.get("text", "")
    regex = request.args.get("regex")
    case_sensitive = request.args.get("case_sensitive") in {"1", "true", "on"}
    limit = int(request.args.get("limit", 1000))
    start_ts = request.args.get("start_ts")
    end_ts = request.args.get("end_ts")
    try:
        start_ts_val = int(start_ts) if start_ts else None
    except (TypeError, ValueError):
        start_ts_val = None
    try:
        end_ts_val = int(end_ts) if end_ts else None
    except (TypeError, ValueError):
        end_ts_val = None
    results = search_logs(
        dataset_root=dataset_root,
        text=text,
        regex=regex,
        case_sensitive=case_sensitive,
        limit=limit,
        start_ts=start_ts_val,
        end_ts=end_ts_val,
    )
    payload = {"results": results, "text": text, "regex": regex}
    return _attachment_response(payload, "search_logs.json")


@bp.route("/export/index-suggestions")
def export_index_suggestions() -> Response:
    service = _get_service()
    filters = _extract_filters(request)
    limit = int(request.args.get("limit", 100))
    suggestions = service.get_index_suggestions(limit=limit, filters=filters)
    payload = {"suggestions": suggestions}
    return _attachment_response(payload, "index_suggestions.json")


@bp.route("/dashboard/summary")
def dashboard_summary() -> Any:
    service = _get_service()
    limit = int(request.args.get("limit", 10))
    filters = _extract_filters(request)
    ip_filter = request.args.get("ip_filter")
    user_filter = request.args.get("user_filter")
    summary = service.get_dashboard_summary(
        limit=limit,
        filters=filters,
        ip_filter=ip_filter,
        user_filter=user_filter,
    )
    return jsonify(summary)


@bp.route("/dashboard/resolve-timestamp")
def dashboard_resolve_timestamp() -> Any:
    service = _get_service()
    local_value = request.args.get("local")
    direction = request.args.get("direction", "start").lower()
    if direction not in {"start", "end"}:
        direction = "start"
    hint = request.args.get("hint")
    if not local_value:
        return jsonify({"iso": None, "offset": None, "epoch": None}), 400

    normalized = local_value.strip()
    if len(normalized) == 16:
        normalized += ":00"
    resolved = service.resolve_timestamp_for_local(normalized, direction=direction, hint=hint)

    if resolved is None:
        offset_map = service.get_date_offset_map()
        offset = offset_map.get(normalized[:10], "Z") if normalized else "Z"
        suffix = "Z" if offset == "Z" else offset
        fallback_iso = normalized + suffix
        parsed = DuckDBService._parse_log_timestamp(fallback_iso)
        epoch = int(parsed.timestamp()) if parsed else None
        return jsonify({"iso": fallback_iso, "offset": offset, "epoch": epoch}), 200

    iso_value, epoch_value = resolved
    offset = iso_value[-6:] if len(iso_value) >= 6 else "Z"
    if offset.upper() == "Z" and not iso_value.endswith("Z"):
        offset = "Z"
    return jsonify({"iso": iso_value, "offset": offset, "epoch": epoch_value}), 200


def _extract_filters(req) -> Dict[str, Any]:
    filters: Dict[str, Any] = {}
    if "database" in req.args:
        filters["database"] = req.args["database"]
    if "namespace" in req.args:
        filters["namespace"] = req.args["namespace"]
    if "start_ts" in req.args:
        start_ts = _safe_int(req.args.get("start_ts"))
        if start_ts is not None:
            filters["start_ts"] = start_ts
    if "end_ts" in req.args:
        end_ts = _safe_int(req.args.get("end_ts"))
        if end_ts is not None:
            filters["end_ts"] = end_ts
    if "app_name" in req.args:
        filters["app_name"] = req.args["app_name"]
    if "user" in req.args:
        filters["user"] = req.args["user"]
    if "mechanism" in req.args:
        filters["mechanism"] = req.args["mechanism"]
    if "result" in req.args:
        filters["result"] = req.args["result"]
    if "remote_address" in req.args:
        filters["remote_address"] = req.args["remote_address"]
    return filters


def _safe_int(raw: Any) -> Optional[int]:
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _attachment_response(payload: Dict[str, Any], filename: str) -> Response:
    data = jsonify(payload).get_data(as_text=False)
    response = Response(data, mimetype="application/json")
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return response
