"""Utilities for deriving index suggestions from slow query logs."""

from __future__ import annotations

import json
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Tuple


def _parse_query(query_text: str) -> Dict[str, Any] | None:
    if not query_text or not query_text.strip():
        return None
    text = query_text.strip()
    if not (text.startswith("{") and text.endswith("}")):
        return None
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None


def _single_field_suggestion(collection: str, field: str, reason: str) -> Dict[str, Any]:
    index_spec = f"{{{field}: 1}}"
    return {
        "type": "single_field",
        "index": index_spec,
        "reason": reason,
        "priority": "high",
        "confidence": "high",
        "command": f"db.{collection}.createIndex({index_spec})",
    }


def _find_suggestions(query: Dict[str, Any], collection: str) -> List[Dict[str, Any]]:
    suggestions: List[Dict[str, Any]] = []

    filter_obj = query.get("filter") or {}
    sort_obj = query.get("sort") or {}

    for field, value in filter_obj.items():
        if field in {"$and", "$or", "$nor"}:
            continue
        suggestions.append(
            _single_field_suggestion(
                collection,
                field,
                f"Filter on {field}",
            )
        )

    if sort_obj:
        sort_fields = [(field, direction) for field, direction in sort_obj.items()]
        if len(sort_fields) == 1:
            field, direction = sort_fields[0]
            index_spec = f"{{{field}: {direction}}}"
            suggestions.append(
                {
                    "type": "sort",
                    "index": index_spec,
                    "reason": f"Sort by {field}",
                    "priority": "high",
                    "confidence": "high",
                    "command": f"db.{collection}.createIndex({index_spec})",
                }
            )
        elif 1 < len(sort_fields) <= 3:
            index_fields = ", ".join(f"{field}: {direction}" for field, direction in sort_fields)
            index_spec = f"{{{index_fields}}}"
            suggestions.append(
                {
                    "type": "compound_sort",
                    "index": index_spec,
                    "reason": "Compound sort on "
                    + ", ".join(field for field, _ in sort_fields),
                    "priority": "medium",
                    "confidence": "high",
                    "command": f"db.{collection}.createIndex({index_spec})",
                }
            )

    if filter_obj and sort_obj and len(filter_obj) == 1 and len(sort_obj) == 1:
        filter_field = next(iter(filter_obj.keys()))
        sort_field, sort_dir = next(iter(sort_obj.items()))
        if filter_field != sort_field:
            index_spec = f"{{{filter_field}: 1, {sort_field}: {sort_dir}}}"
            suggestions.append(
                {
                    "type": "compound_filter_sort",
                    "index": index_spec,
                    "reason": f"Filter on {filter_field} and sort by {sort_field}",
                    "priority": "high",
                    "confidence": "high",
                    "command": f"db.{collection}.createIndex({index_spec})",
                }
            )

    return suggestions


def _aggregate_suggestions(query: Dict[str, Any], collection: str) -> List[Dict[str, Any]]:
    suggestions: List[Dict[str, Any]] = []
    pipeline = query.get("pipeline") or []

    for stage in pipeline:
        if not isinstance(stage, dict):
            continue

        if "$match" in stage:
            match_obj = stage["$match"]
            if isinstance(match_obj, dict) and match_obj:
                for field, value in match_obj.items():
                    if field in {"$and", "$or", "$nor", "$expr"}:
                        continue
                    suggestions.append(
                        _single_field_suggestion(
                            collection,
                            field,
                            f"$match stage filter on {field}",
                        )
                    )
        elif "$sort" in stage:
            sort_obj = stage["$sort"]
            if isinstance(sort_obj, dict) and len(sort_obj) == 1:
                field, direction = next(iter(sort_obj.items()))
                index_spec = f"{{{field}: {direction}}}"
                suggestions.append(
                    {
                        "type": "aggregate_sort",
                        "index": index_spec,
                        "reason": f"$sort stage on {field}",
                        "priority": "high",
                        "confidence": "high",
                        "command": f"db.{collection}.createIndex({index_spec})",
                    }
                )

    return suggestions


def _spec_from_index(index_spec: str) -> Tuple[Tuple[str, int], ...]:
    inside = index_spec.strip().strip("{}").strip()
    if not inside:
        return tuple()
    result: List[Tuple[str, int]] = []
    parts = [segment.strip() for segment in inside.split(",") if segment.strip()]
    for part in parts:
        if ":" not in part:
            continue
        field, direction = part.split(":", 1)
        field = field.strip()
        try:
            dir_int = int(direction.strip())
        except ValueError:
            continue
        result.append((field, dir_int))
    return tuple(result)


def _is_prefix(candidate: Tuple[Tuple[str, int], ...], other: Tuple[Tuple[str, int], ...]) -> bool:
    if len(candidate) > len(other):
        return False
    return candidate == other[: len(candidate)]


def _normalize_timestamp(value: Any) -> Any:
    return value


def generate_index_suggestions(
    records: Iterable[Dict[str, Any]],
    pattern_totals: Dict[str, Dict[str, Any]] | None = None,
    *,
    limit_per_collection: int = 10,
    min_occurrences: int = 3,
    min_avg_duration_ms: float = 250.0,
) -> Dict[str, Any]:
    pattern_totals = pattern_totals or {}

    collections: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "collection_name": "",
            "collscan_queries": 0,
            "ixscan_ineff_queries": 0,
            "total_docs_examined": 0,
            "total_returned": 0,
            "total_duration": 0,
            "avg_duration": 0.0,
            "avg_docs_per_query": 0.0,
            "sample_queries": [],
            "suggestions": [],
            "reviews": [],
        }
    )

    spec_index: Dict[Tuple[str, Tuple[Tuple[str, int], ...]], Dict[str, Any]] = defaultdict(
        lambda: {
            "collection": "",
            "spec": tuple(),
            "occurrences": 0,
            "total_duration": 0,
            "docs_examined": 0,
            "returned": 0,
            "reason": "",
            "confidence": "high",
            "priority": "high",
        }
    )

    total_collscan = 0
    priority_rank = {"high": 3, "medium": 2, "low": 1}

    for record in records:
        database = record.get("database") or "unknown"
        collection = record.get("collection") or "unknown"
        namespace = f"{database}.{collection}"
        plan_summary = (record.get("plan_summary") or "").upper()
        is_collscan = plan_summary.startswith("COLLSCAN")
        is_ixscan = "IXSCAN" in plan_summary and not is_collscan

        if not (is_collscan or is_ixscan):
            continue

        coll_entry = collections[namespace]
        coll_entry["collection_name"] = namespace
        if is_collscan:
            coll_entry["collscan_queries"] += 1
            total_collscan += 1
        if is_ixscan:
            coll_entry["ixscan_ineff_queries"] += 1

        docs = int(record.get("docs_examined") or 0)
        returned = int(record.get("docs_returned") or 0)
        duration = float(record.get("duration_ms") or 0.0)

        coll_entry["total_docs_examined"] += docs
        coll_entry["total_returned"] += returned
        coll_entry["total_duration"] += duration

        if is_collscan and len(coll_entry["sample_queries"]) < 3:
            coll_entry["sample_queries"].append(
                {
                    "query": record.get("query_text", ""),
                    "duration": int(duration),
                    "timestamp": _normalize_timestamp(record.get("timestamp")),
                }
            )

        query_obj = _parse_query(record.get("query_text", ""))
        if not query_obj:
            if is_collscan:
                coll_entry["reviews"].append(
                    {
                        "plan_summary": plan_summary or "COLLSCAN",
                        "duration_ms": int(duration),
                        "docs_examined": docs,
                        "docs_returned": returned,
                        "reason": "COLLSCAN query has complex/unsupported filter; review manually.",
                        "query_text": record.get("query_text", ""),
                    }
                )
            elif is_ixscan:
                coll_entry["reviews"].append(
                    {
                        "plan_summary": plan_summary or "IXSCAN",
                        "duration_ms": int(duration),
                        "docs_examined": docs,
                        "docs_returned": returned,
                        "reason": "Query already uses index plan; evaluate query structure before adding new index.",
                        "query_text": record.get("query_text", ""),
                    }
                )
            continue

        collection_name = namespace.split(".", 1)[-1]
        raw_suggestions: List[Dict[str, Any]] = []
        if "find" in query_obj:
            raw_suggestions.extend(_find_suggestions(query_obj, collection_name))
        elif "aggregate" in query_obj:
            raw_suggestions.extend(_aggregate_suggestions(query_obj, collection_name))

        if not raw_suggestions:
            if is_collscan:
                coll_entry["reviews"].append(
                    {
                        "plan_summary": plan_summary or "COLLSCAN",
                        "duration_ms": int(duration),
                        "docs_examined": docs,
                        "docs_returned": returned,
                        "reason": "COLLSCAN query with no straightforward filter/sort; review manually.",
                        "query_text": record.get("query_text", ""),
                    }
                )
            elif is_ixscan:
                coll_entry["reviews"].append(
                    {
                        "plan_summary": plan_summary or "IXSCAN",
                        "duration_ms": int(duration),
                        "docs_examined": docs,
                        "docs_returned": returned,
                        "reason": "Query already uses an index (IXSCAN); consider tuning existing index or query.",
                        "query_text": record.get("query_text", ""),
                    }
                )
            continue

        if not is_collscan:
            coll_entry["reviews"].append(
                {
                    "plan_summary": plan_summary or "IXSCAN",
                    "duration_ms": int(duration),
                    "docs_examined": docs,
                    "docs_returned": returned,
                    "reason": "Index already present; review before adding additional indexes.",
                    "query_text": record.get("query_text", ""),
                }
            )
            continue

        pattern_key = None
        query_hash = record.get("query_hash") or ""
        if query_hash:
            pattern_key = f"{namespace}::{plan_summary}::{query_hash}"
        pattern_info = pattern_totals.get(pattern_key or "")

        for suggestion in raw_suggestions:
            spec = _spec_from_index(suggestion.get("index", ""))
            if not spec:
                continue
            key = (namespace, spec)
            entry = spec_index[key]
            entry["collection"] = namespace
            entry["spec"] = spec
            entry["occurrences"] += 1
            entry["total_duration"] += duration
            entry["docs_examined"] += docs
            entry["returned"] += returned
            entry["reason"] = suggestion.get("reason", "Index suggestion")
            entry["confidence"] = suggestion.get("confidence", "high")
            priority_value = suggestion.get("priority", "high")
            current_rank = priority_rank.get(priority_value, 1)
            stored_rank = priority_rank.get(entry.get("priority", "high"), 1)
            if current_rank > stored_rank:
                entry["priority"] = priority_value

            if pattern_info:
                total_count = int(pattern_info.get("total_count", 0) or 0)
                avg_duration = float(pattern_info.get("avg_duration", 0) or 0.0)
                if total_count > 1:
                    additional = max(0, total_count - 1)
                    entry["occurrences"] += additional
                    entry["total_duration"] += avg_duration * additional

    coll_to_specs: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for entry in spec_index.values():
        coll_to_specs[entry["collection"]].append(entry)

    final_collections: Dict[str, Dict[str, Any]] = {}
    top_suggestions: List[Dict[str, Any]] = []

    global_candidates: List[Dict[str, Any]] = []

    for namespace, data in collections.items():
        items = coll_to_specs.get(namespace, [])
        ranked: List[Dict[str, Any]] = []
        for entry in items:
            docs_examined = entry["docs_examined"]
            returned = entry["returned"]
            ineff_ratio = None
            if docs_examined:
                ineff_ratio = round(docs_examined / max(1, returned or 1), 2)
            impact = entry["total_duration"] * (ineff_ratio if ineff_ratio else 1.0)
            entry["impact_score"] = int(impact)
            entry["inefficiency_ratio"] = ineff_ratio
            entry["avg_duration_ms"] = int(
                entry["total_duration"] / max(1, entry["occurrences"])
            )
            selectivity_pct = None
            if docs_examined:
                selectivity_pct = round((returned / docs_examined) * 100, 2)
            entry["selectivity_pct"] = selectivity_pct
            ranked.append(entry)

        ranked.sort(key=lambda r: (-r["impact_score"], -len(r["spec"])))

        filtered: List[Dict[str, Any]] = []
        for candidate in ranked:
            if candidate["occurrences"] < min_occurrences:
                continue
            if candidate["avg_duration_ms"] < min_avg_duration_ms and candidate["impact_score"] < min_avg_duration_ms * min_occurrences:
                continue
            filtered.append(candidate)

        filtered.sort(key=lambda r: (-r["impact_score"], -len(r["spec"])))

        deduped: List[Dict[str, Any]] = []
        for candidate in filtered:
            spec = candidate["spec"]
            if any(_is_prefix(spec, existing["spec"]) for existing in deduped):
                continue
            deduped.append(candidate)
        deduped = deduped[:limit_per_collection]

        formatted: List[Dict[str, Any]] = []
        for entry in deduped:
            fields_formatted = ", ".join(f"{f}: {d}" for f, d in entry["spec"])
            index_spec = f"{{{fields_formatted}}}" if fields_formatted else "{}"
            collection_name = namespace.split(".", 1)[-1]
            priority_value = entry.get("priority", "high")
            if priority_value.lower() != "high":
                continue
            avg_docs = int(entry["docs_examined"] / max(1, entry["occurrences"]))
            justification = (
                f"{entry['occurrences']} COLLSCAN executions scanned ~{avg_docs} docs in "
                f"{entry['avg_duration_ms']} ms without an index covering {index_spec}."
            )
            formatted_entry = {
                "type": "compound" if len(entry["spec"]) > 1 else "single_field",
                "index": index_spec,
                "reason": entry.get("reason", "Index suggestion"),
                "priority": priority_value,
                "confidence": entry.get("confidence", "high"),
                "impact_score": entry.get("impact_score", 0),
                "occurrences": entry.get("occurrences", 0),
                "avg_duration_ms": entry.get("avg_duration_ms", 0),
                "inefficiency_ratio": entry.get("inefficiency_ratio"),
                "selectivity_pct": entry.get("selectivity_pct"),
                "command": f"db.{collection_name}.createIndex({index_spec})",
                "fields": list(entry["spec"]),
                "justification": justification,
                "collection": namespace,
            }
            formatted.append(formatted_entry)
            top_entry = formatted_entry.copy()
            top_entry["collection"] = namespace
            top_suggestions.append(top_entry)
            global_candidates.append(formatted_entry)

        total_queries = (
            data["collscan_queries"] + data["ixscan_ineff_queries"]
        )
        if total_queries:
            data["avg_duration"] = data["total_duration"] / total_queries
            data["avg_docs_per_query"] = data["total_docs_examined"] / total_queries
        data["suggestions"] = formatted
        data["sample_queries"] = data["sample_queries"][:3]
        final_collections[namespace] = data

    global_candidates.sort(key=lambda s: s.get("impact_score", 0), reverse=True)
    top_suggestions = global_candidates[:10]

    total_suggestions = sum(
        len(collection_data["suggestions"])
        for collection_data in final_collections.values()
    )
    avg_docs_examined = (
        sum(data["total_docs_examined"] for data in final_collections.values())
        / total_collscan
        if total_collscan
        else 0
    )

    # Ensure reviews key exists even if no entries were added
    for data in final_collections.values():
        data["reviews"] = data.get("reviews", [])

    return {
        "collections": final_collections,
        "top_suggestions": top_suggestions,
        "summary": {
            "total_collscan_queries": total_collscan,
            "total_suggestions": total_suggestions,
            "avg_docs_examined": avg_docs_examined,
        },
    }


__all__ = ["generate_index_suggestions"]
