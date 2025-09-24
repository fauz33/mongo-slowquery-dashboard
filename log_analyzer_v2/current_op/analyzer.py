"""Utility to analyze `db.currentOp()` output for the v2 stack."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from typing import Any, Dict


def analyze_current_op(
    current_op_data: str,
    *,
    threshold: int = 30,
    filters: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Analyze MongoDB `db.currentOp()` output and return structured insights."""

    filters = filters or {}

    try:
        if current_op_data.startswith("db.currentOp()"):
            current_op_data = current_op_data.replace("db.currentOp()", "").strip()

        if not current_op_data.startswith("{"):
            current_op_data = current_op_data.strip()
            if current_op_data.startswith("inprog"):
                start_idx = current_op_data.find("[")
                end_idx = current_op_data.rfind("]")
                if start_idx != -1 and end_idx != -1:
                    current_op_data = '{"inprog": ' + current_op_data[start_idx : end_idx + 1] + "}"

        data = json.loads(current_op_data)
        operations = data.get("inprog", []) if isinstance(data, dict) else data

        if not operations:
            return {"error": "No operations found in the provided data", "total_operations": 0}

        analysis: Dict[str, Any] = {
            "total_operations": len(operations),
            "operation_types": Counter(),
            "operation_states": Counter(),
            "long_running_ops": [],
            "resource_intensive_ops": [],
            "lock_analysis": {
                "read_locks": [],
                "write_locks": [],
                "waiting_operations": [],
            },
            "collection_hotspots": Counter(),
            "database_hotspots": Counter(),
            "client_connections": Counter(),
            "query_analysis": {
                "collscans": [],
                "index_scans": [],
                "duplicate_queries": [],
            },
            "recommendations": [],
            "performance_metrics": {
                "avg_duration": 0,
                "max_duration": 0,
                "min_duration": float("inf"),
                "total_duration": 0,
            },
            "ops_brief": [],
            "filters": {
                "threshold": threshold,
                "only_active": bool(filters.get("only_active")),
                "only_waiting": bool(filters.get("only_waiting")),
                "only_collscan": bool(filters.get("only_collscan")),
            },
        }

        durations = []
        query_patterns: Dict[str, list] = defaultdict(list)

        for op in operations:
            if not isinstance(op, dict):
                continue

            op_type = op.get("op", "unknown")
            analysis["operation_types"][op_type] += 1

            if "active" in op:
                state = "active" if op["active"] else "inactive"
            elif "waitingForLock" in op:
                state = "waiting_for_lock" if op["waitingForLock"] else "active"
            else:
                state = "unknown"
            analysis["operation_states"][state] += 1

            duration = 0.0
            if isinstance(op.get("microsecs_running"), (int, float)):
                duration = float(op["microsecs_running"]) / 1_000_000
            elif isinstance(op.get("secs_running"), (int, float)):
                duration = float(op["secs_running"])

            if duration > 0:
                durations.append(duration)
                metrics = analysis["performance_metrics"]
                metrics["total_duration"] += duration
                metrics["max_duration"] = max(metrics["max_duration"], duration)
                metrics["min_duration"] = min(metrics["min_duration"], duration)

                if duration > (threshold or 30):
                    analysis["long_running_ops"].append(
                        {
                            "opid": op.get("opid"),
                            "op": op_type,
                            "duration": duration,
                            "ns": op.get("ns", "unknown"),
                            "desc": op.get("desc", ""),
                            "client": op.get("client", "unknown"),
                        }
                    )

            locks = op.get("locks", {})
            waiting_for_lock = bool(op.get("waitingForLock"))
            if waiting_for_lock:
                analysis["lock_analysis"]["waiting_operations"].append(
                    {
                        "opid": op.get("opid"),
                        "op": op_type,
                        "ns": op.get("ns", "unknown"),
                        "duration": duration,
                    }
                )

            for lock_type, lock_info in (locks or {}).items():
                if not isinstance(lock_info, dict):
                    continue
                lock_mode = lock_info.get("acquireCount") or lock_info.get("mode")
                if isinstance(lock_mode, dict):
                    lock_mode = next(iter(lock_mode.keys()), None)
                if lock_mode in ["R", "r"]:
                    analysis["lock_analysis"]["read_locks"].append(
                        {"opid": op.get("opid"), "type": lock_type, "ns": op.get("ns", "unknown")}
                    )
                elif lock_mode in ["W", "w", "X"]:
                    analysis["lock_analysis"]["write_locks"].append(
                        {"opid": op.get("opid"), "type": lock_type, "ns": op.get("ns", "unknown")}
                    )

            try:
                cpu_s = 0.0
                if isinstance(op.get("microsecs_running"), (int, float)):
                    cpu_s = float(op["microsecs_running"]) / 1_000_000
                elif isinstance(op.get("secs_running"), (int, float)):
                    cpu_s = float(op["secs_running"])

                bytes_read = op.get("bytesRead") or op.get("bytes_read")
                if bytes_read is None and isinstance(op.get("network"), dict):
                    bytes_read = op["network"].get("bytesRead")
                bytes_written = op.get("bytesWritten") or op.get("bytes_written")
                if bytes_written is None and isinstance(op.get("network"), dict):
                    bytes_written = op["network"].get("bytesWritten")

                mem_mb = None
                mem = op.get("memory") or {}
                if isinstance(mem, dict):
                    mem_mb = (
                        mem.get("residentMb")
                        or mem.get("residentMB")
                        or mem.get("workingSetMb")
                    )
                if mem_mb is None:
                    ws = op.get("workingSet") or op.get("resident")
                    if isinstance(ws, (int, float)):
                        mem_mb = round(float(ws) / (1024 * 1024), 2)

                analysis["ops_brief"].append(
                    {
                        "opid": op.get("opid"),
                        "ns": op.get("ns"),
                        "op": op.get("op"),
                        "client": op.get("client") or op.get("conn"),
                        "active": bool(op.get("active")),
                        "waitingForLock": bool(op.get("waitingForLock")),
                        "planSummary": op.get("planSummary") or "None",
                        "cpuTime_s": round(cpu_s, 3),
                        "bytesRead": bytes_read if isinstance(bytes_read, (int, float)) else None,
                        "bytesWritten": bytes_written if isinstance(bytes_written, (int, float)) else None,
                        "memoryMb": mem_mb,
                    }
                )
            except Exception:
                pass

            ns = op.get("ns", "")
            if ns and "." in ns:
                db_name, _collection = ns.split(".", 1)
                analysis["database_hotspots"][db_name] += 1
                analysis["collection_hotspots"][ns] += 1

            client = op.get("client", "unknown")
            if client != "unknown":
                analysis["client_connections"][client] += 1

            command = op.get("command", {})
            plan_summary = op.get("planSummary", "")

            if plan_summary == "COLLSCAN":
                analysis["query_analysis"]["collscans"].append(
                    {
                        "opid": op.get("opid"),
                        "ns": ns,
                        "duration": duration,
                        "command": _truncate(str(command)),
                    }
                )
            elif "IXSCAN" in plan_summary:
                analysis["query_analysis"]["index_scans"].append(
                    {
                        "opid": op.get("opid"),
                        "ns": ns,
                        "plan": plan_summary,
                    }
                )

            if command:
                query_key = _normalize_query_for_grouping(command)
                query_patterns[query_key].append(
                    {"opid": op.get("opid"), "ns": ns, "duration": duration}
                )

        if durations:
            metrics = analysis["performance_metrics"]
            metrics["avg_duration"] = sum(durations) / len(durations)
        else:
            analysis["performance_metrics"]["min_duration"] = 0

        for query_key, ops in query_patterns.items():
            if len(ops) > 1:
                analysis["query_analysis"]["duplicate_queries"].append(
                    {
                        "query_pattern": _truncate(query_key, 100),
                        "count": len(ops),
                        "operations": ops,
                    }
                )

        try:
            analysis["ops_brief"].sort(
                key=lambda x: (x.get("cpuTime_s") or 0.0), reverse=True
            )
            analysis["ops_brief"] = analysis["ops_brief"][:50]
        except Exception:
            pass

        analysis["recommendations"] = _generate_current_op_recommendations(analysis)
        return analysis

    except json.JSONDecodeError as exc:
        return {"error": f"Invalid JSON format: {exc}", "total_operations": 0}
    except Exception as exc:
        return {"error": f"Error analyzing data: {exc}", "total_operations": 0}


def _truncate(text: str, limit: int = 200) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + "..."


def _normalize_query_for_grouping(command: Any) -> str:
    if not isinstance(command, dict):
        return str(command)

    normalized: Dict[str, Any] = {}
    for key, value in command.items():
        if key in {"find", "aggregate", "update", "insert", "delete"}:
            normalized[key] = value
        elif key in {"filter", "query", "pipeline"}:
            normalized[key] = _normalize_query_structure(value)
        else:
            normalized[key] = type(value).__name__
    return json.dumps(normalized, sort_keys=True)


def _normalize_query_structure(query: Any) -> Any:
    if isinstance(query, dict):
        normalized: Dict[str, Any] = {}
        for key, value in query.items():
            if isinstance(value, (str, int, float, bool)):
                normalized[key] = type(value).__name__
            elif isinstance(value, (list, dict)):
                normalized[key] = _normalize_query_structure(value)
            else:
                normalized[key] = "mixed"
        return normalized
    if isinstance(query, list):
        return [_normalize_query_structure(query[0])] if query else []
    return type(query).__name__


def _generate_current_op_recommendations(analysis: Dict[str, Any]) -> list:
    recommendations = []

    if analysis["long_running_ops"]:
        recommendations.append(
            {
                "type": "warning",
                "title": "Long-Running Operations Detected",
                "description": f"Found {len(analysis['long_running_ops'])} operations running longer than 30 seconds.",
                "action": "Review and consider killing operations that may be stuck or inefficient.",
                "priority": "high",
            }
        )

    if analysis["query_analysis"]["collscans"]:
        recommendations.append(
            {
                "type": "error",
                "title": "Collection Scans Detected",
                "description": f"Found {len(analysis['query_analysis']['collscans'])} operations performing full collection scans.",
                "action": "Consider adding appropriate indexes to improve query performance.",
                "priority": "high",
            }
        )

    if analysis["query_analysis"]["duplicate_queries"]:
        recommendations.append(
            {
                "type": "info",
                "title": "Duplicate Operations Found",
                "description": f"Found {len(analysis['query_analysis']['duplicate_queries'])} patterns with multiple concurrent executions.",
                "action": "Review if these operations can be optimized or cached.",
                "priority": "medium",
            }
        )

    if analysis["lock_analysis"]["waiting_operations"]:
        recommendations.append(
            {
                "type": "warning",
                "title": "Lock Contention Detected",
                "description": f"Found {len(analysis['lock_analysis']['waiting_operations'])} operations waiting for locks.",
                "action": "Review operations causing lock contention and consider optimization.",
                "priority": "high",
            }
        )

    for client, count in analysis["client_connections"].most_common(3):
        if count > 10:
            recommendations.append(
                {
                    "type": "info",
                    "title": "High Connection Count",
                    "description": f"Client {client} has {count} active operations.",
                    "action": "Review connection pooling and operation efficiency for this client.",
                    "priority": "medium",
                }
            )

    for ns, count in analysis["collection_hotspots"].most_common(3):
        if count > 5:
            recommendations.append(
                {
                    "type": "info",
                    "title": "Collection Hotspot",
                    "description": f"Collection {ns} has {count} concurrent operations.",
                    "action": "Monitor for potential bottlenecks and consider sharding if needed.",
                    "priority": "low",
                }
            )

    return recommendations
