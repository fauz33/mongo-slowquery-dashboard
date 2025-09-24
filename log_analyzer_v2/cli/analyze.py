"""Command-line helpers for the v2 analyzer prototypes."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Dict, Any

from ..ingest.pipeline import ingest_log_file
from ..analytics.duckdb_service import DuckDBService
from ..storage.manifest import load_manifest
from ..config import settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Mongo slow query analyzer v2 CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest_parser = sub.add_parser("ingest-log", help="Parse log file into Parquet datasets")
    ingest_parser.add_argument("log_file", type=Path, help="Path to MongoDB log file")
    ingest_parser.add_argument(
        "--out", type=Path, default=None, help="Output root directory (defaults to config)"
    )
    ingest_parser.add_argument(
        "--file-id", type=int, default=None, help="Logical identifier for the source file"
    )
    ingest_parser.add_argument(
        "--compression",
        type=str,
        default=None,
        help="Parquet compression codec (default: config value)",
    )

    # Backwards-compatible alias
    ingest_alias = sub.add_parser(
        "ingest-slow", help="Alias for ingest-log (kept for early prototypes)"
    )
    ingest_alias.add_argument("log_file", type=Path, help="Path to MongoDB log file")
    ingest_alias.add_argument(
        "--out", type=Path, default=None, help="Output root directory (defaults to config)"
    )
    ingest_alias.add_argument(
        "--file-id", type=int, default=None, help="Logical identifier for the source file"
    )
    ingest_alias.add_argument(
        "--compression",
        type=str,
        default=None,
        help="Parquet compression codec (default: config value)",
    )

    status_parser = sub.add_parser("status", help="Show dataset manifest summary")
    status_parser.add_argument("--out", type=Path, default=None, help="Dataset root path")

    summaries_parser = sub.add_parser("summaries", help="Display quick workload summaries")
    summaries_parser.add_argument("--out", type=Path, default=None, help="Dataset root path")
    summaries_parser.add_argument("--limit", type=int, default=10, help="Rows per section")

    clean_parser = sub.add_parser("clean", help="Remove generated dataset artifacts")
    clean_parser.add_argument("--out", type=Path, default=None, help="Dataset root path")
    clean_parser.add_argument(
        "--force", action="store_true", help="Skip confirmation prompt and delete immediately"
    )

    return parser


def _print_summary(telemetry: Dict[str, Any]) -> None:
    print(f"Input file: {telemetry['input_path']}")
    if "file_id" in telemetry:
        print(f"  file_id: {telemetry['file_id']}")
    for key in ("slow_queries", "authentications", "connections", "query_offsets"):
        info = telemetry.get(key, {})
        print(
            f"  {key}: {info.get('rows_written', 0)} rows -> {info.get('path', '<n/a>')}"
        )
    manifest = telemetry.get("manifest", {})
    manifest_path = manifest.get("path", "<n/a>")
    ingest_id = manifest.get("ingest_id")
    suffix = f" (ingest #{ingest_id})" if ingest_id is not None else ""
    print(f"  manifest: {manifest_path}{suffix}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command in {"ingest-log", "ingest-slow"}:
        telemetry = ingest_log_file(
            args.log_file,
            output_root=args.out,
            file_id=args.file_id,
            compression=args.compression,
        )
        _print_summary(telemetry)
        return 0

    if args.command == "status":
        out_root = _resolve_out(args.out)
        manifest = load_manifest(out_root / "manifest.json")
        if not manifest:
            print(f"No manifest found under {out_root}")
            return 1
        print(f"Dataset version: {manifest.get('dataset_version')}")
        print(f"Created at: {manifest.get('created_at')}")
        print(f"Updated at: {manifest.get('updated_at')}")
        ingests = manifest.get("ingests", [])
        print(f"Ingest count: {len(ingests)}")
        for ingest in ingests[-5:]:
            row_counts = ingest.get("row_counts", {})
            print(
                f"  #{ingest.get('ingest_id')} {ingest.get('source_file')} "
                f"slow_queries={row_counts.get('slow_queries', 0)}"
            )
        return 0

    if args.command == "summaries":
        out_root = _resolve_out(args.out)
        try:
            service = DuckDBService(dataset_root=out_root)
        except RuntimeError as exc:
            print(f"DuckDB unavailable: {exc}")
            return 1
        limit = max(1, min(args.limit, 100))
        namespaces = service.get_namespace_summary(limit=limit)
        plans = service.get_plan_summary(limit=limit)
        workload = service.get_workload_summary(limit=limit)
        print("Top namespaces:")
        for item in namespaces:
            print(
                f"  {item['namespace']}: executions={item['executions']} avg_ms={item['avg_duration_ms']:.1f}"
            )
        print("Top plans:")
        for item in plans:
            print(f"  {item['plan_summary']}: executions={item['executions']}")
        print("Workload stats:")
        stats = workload.get("stats", {})
        print(f"  executions={stats.get('executions')} total_docs_examined={stats.get('total_docs_examined')}")
        service.close()
        return 0

    if args.command == "clean":
        out_root = _resolve_out(args.out)
        if not out_root.exists():
            print(f"Nothing to clean under {out_root}")
            return 0
        if not args.force:
            response = input(f"Delete dataset at {out_root}? [y/N] ").strip().lower()
            if response not in {"y", "yes"}:
                print("Aborted")
                return 1
        shutil.rmtree(out_root)
        print(f"Removed dataset directory {out_root}")
        return 0

    parser.error("Unknown command")
    return 1


def _resolve_out(out: Path | None) -> Path:
    return Path(out) if out is not None else Path(settings.output_root)


if __name__ == "__main__":
    raise SystemExit(main())
