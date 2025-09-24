# Mongo Slow Query Analyzer – Revamp Plan

This document captures a ground-up redesign that replaces the current SQLite-based backend with a Python-native, columnar analytics pipeline optimised for multi-GB MongoDB slow-query logs. No data migration of past uploads is required; the new stack is activated for the next ingest.

--------------------------------------------------------------------------------
## 1. Objectives & Constraints

- Handle multi-gigabyte log files with predictable memory use.
- Keep everything in Python (no Postgres/MySQL). Optional dependencies limited to PyArrow/DuckDB/Polars.
- Preserve existing UI routes (slow query table, trend charts, detail modals, search, authentication & connection dashboards) but repoint them to the new storage/service layer.
- Ensure detail views still show original log entries without duplicating huge payloads.
- No migration of legacy SQLite datasets; the new pipeline produces fresh outputs in a new directory tree.
- Maintain legacy modules (`app.py`, `optimized_analyzer.py`) untouched for fallback purposes; the new implementation lives in parallel files/packages.
- Standardise development/testing by activating the shared virtualenv before work: `source /data/GDrive/fauz33/dashboard-opsmanager/proj_env/bin/activate`.
- Use sample and validation log files from `mongodb_logs/` for local testing to keep scenarios realistic.

--------------------------------------------------------------------------------
## 2. High-Level Architecture

1. **Streaming Parser**
   - Reuse existing chunked parsing framework.
   - Workers produce normalized records with file offsets.
   - Distill results into Parquet fact streams for slow queries, authentications, and connections, plus the detail offset index.

2. **Columnar Store (Parquet)**
   - Partitioned Parquet datasets hold execution-level metrics (one row per slow query) alongside authentication and connection event streams.
   - Auxiliary Parquet/JSON files store aggregated summaries and manifest metadata.

3. **Analytics Engine (DuckDB / Polars)**
   - Embedded DuckDB (or Polars as fallback) executes group-by, trend, and summary queries against Parquet partitions.
   - Results surface via a Python service layer consumed by Flask routes.

4. **Detail Retrieval**
   - Maintain an offset index (`query_hash → [file_offset, length, file_id]`) so the UI can pull raw JSON straight from the original log using `mmap`.

5. **CLI & Web Integration**
   - CLI orchestrates ingest into the new store.
   - Flask controllers call the new service layer instead of SQLite models.

--------------------------------------------------------------------------------
## 2.1 Project Structure & Coexistence Strategy

To deliver the revamp without destabilising existing users, keep legacy files intact and build the new stack in a parallel namespace:

```
log_analyzer_v2/
 ├── __init__.py
 ├── config.py
 ├── ingest/
 │    ├── parser.py
 │    ├── offsets.py
 │    └── parquet_writer.py
 ├── storage/
 │    ├── manifest.py
 │    ├── summaries.py
 │    └── offsets_index.py
 ├── analytics/
 │    ├── duckdb_service.py
 │    └── polars_fallback.py
 ├── web/
 │    ├── routes.py        # Flask blueprint mirroring existing endpoints
 │    └── status.py
 ├── cli/
 │    └── analyze.py
 └── utils/
      ├── logging_utils.py
      ├── timing.py
      └── concurrency.py
```

- Introduce new entrypoint `app_v2.py` (or `wsgi_v2.py`) that instantiates Flask, registers `log_analyzer_v2.web.routes.bp`, and serves the existing templates. Legacy `app.py` remains unchanged as a fallback.
- When necessary helper functions exist only in `optimized_analyzer.py`, audit and copy them into the appropriate new module (`ingest/parser.py`, etc.) rather than importing the entire legacy class.
- Gate blueprint registration behind an environment flag (`MONGO_SLOWQ_ENABLE_V2=1`) so operators can switch between old and new stacks during rollout.
- Shared assets (templates/static) stay in current folders; avoid duplicate templates by passing the same context structures expected by the frontend.
- Provide a migration checklist in documentation to remove legacy modules once the new stack is fully adopted.
- Document post-cutover steps to drop SQLite files/dependencies once every route depends on the Parquet store, keeping legacy entrypoints only for rollback windows.
- Introduce `app_v2.py` as the Flask entrypoint registering the v2 blueprint and releasing DuckDB connections on teardown.
- Add `slow_query_analysis_v2.html`/JS as the first client-side page consuming `/v2` APIs; gate with `ENABLE_V2_UI` flag while iterating.
- Mirror work for query trends via `/query-trend/v2` template pulling `/v2/query-trend` data to validate parity before decommissioning legacy views.
- `slow_queries_v2.html` now fronts the `/v2/slow-query/list` endpoint to replace the legacy list page during rollout.
- `workload_summary_v2.html` surfaces `/v2/workload/summary` aggregations based on Parquet metrics; legacy analyzer can remain for comparison until parity is confirmed.
- `current_op_v2.html` posts to `/v2/current-op/analyze`, reusing the ported analyzer logic so operators can migrate off the Flask-embedded handler.
- `search_logs_v2.html` reads from `/v2/logs/search`, scanning dataset log files via the manifest file map (with caps to avoid heavy scans during rollout).
- `search_user_access_v2.html` renders `/v2/auth/logs` + `/v2/auth/summary` data to replace the SQLite-driven auth dashboard.
- `index_suggestions_v2.html` consumes `/v2/index-suggestions`, offering heuristic recommendations based on COLLSCAN patterns captured in Parquet.
- Legacy `app.py` handlers remain untouched for rollback; `app_v2.py` currently shadow-routes high-traffic pages to v2 via feature flag. Final cutover should retire redundant routes once UI parity verified.
- `/upload/v2` now orchestrates ingestion via the v2 pipeline (`process_uploads` → `ingest_log_file`), surfacing manifest history in the UI.

--------------------------------------------------------------------------------
## 3. Storage Layout

```
out/
 ├── manifest.json              # dataset metadata (version, ingestion time, files)
 ├── source/                    # optional copies of uploaded logs (kept or symlinked)
 ├── slow_queries/              # partitioned Parquet dataset
 │    └── yyyy/mm/dd/chunk_XXXX.parquet
 ├── authentications/           # partitioned Parquet dataset for auth events
 │    └── yyyy/mm/dd/chunk_XXXX.parquet
 ├── connections/               # partitioned Parquet dataset for connection lifecycle
 │    └── yyyy/mm/dd/chunk_XXXX.parquet
 ├── summaries/
 │    ├── aggregates_YYYYMMDD.json
 │    └── heatmap_YYYYMMDD.parquet
 ├── index/
 │    ├── <source>_query_offsets.parquet   # mapping for detail lookup
 │    └── file_map.json                   # file_id → absolute path (for offsets)
 └── manifest.json               # latest ingest metadata snapshot
```

- Parquet schemas include metrics for slow queries (timestamp, ts_epoch, query_hash, namespace, db, collection, duration, docs_examined, docs_returned, keys_examined, plan_summary, operation_type, cpu/IO metrics, file_id, file_offset, line_length), authentications (timestamp, user, database, mechanism, result, connection_id, remote_addr, file offsets), and connections (timestamp, connection_id, remote_addr, driver, state transitions, file offsets).
- Aggregates stored separately to speed up UI summaries (top hashes, plan mix, duration histograms).
- Each ingest appends to a dataset manifest capturing dataset version, row counts, artifact locations, and ingest_id for traceability.

--------------------------------------------------------------------------------
## 3.1 Dependency Matrix & Environment Toggles

| Component              | Library/Tooling                 | Notes |
|------------------------|---------------------------------|-------|
| Parquet writer         | `pyarrow` (preferred) / `fastparquet` | `pyarrow` integrates cleanly with DuckDB and supports compression codecs. |
| Analytics engine       | `duckdb` ≥ 0.9 (primary)        | Embedded analytics; configure threads/memory per host. |
| Analytics fallback     | `polars` + `pyarrow`            | Triggered via `MONGO_SLOWQ_DISABLE_DUCKDB=1`. |
| Detail lookup          | stdlib `mmap`                   | Requires uploaded logs to remain readable; optional copy to `out/source/`. |
| CLI UX (optional)      | `rich` or `tqdm`                | Improves progress reporting but not required. |

Environment overrides:
- `MONGO_SLOWQ_OUT_ROOT` – absolute path to dataset root.
- `MONGO_SLOWQ_PARQUET_COMPRESSION` – `snappy` (default), `zstd`, etc.
- `MONGO_SLOWQ_CHUNK_ROWS` – max rows before flushing Parquet buffer.
- `MONGO_SLOWQ_KEEP_SOURCE_COPY` – `1` to copy raw logs into managed `out/source/`.
- `MONGO_SLOWQ_DISABLE_DUCKDB` – force Polars fallback for constrained environments.

--------------------------------------------------------------------------------
## 4. Detailed Implementation Plan

### Phase 0 – Shared Utilities
- Add new package `data_store/` with:
  - `config.py` – constants (output root, partitioning rules, dataset version).
  - `logging_utils.py` – structured logging helpers.
  - `timing.py` – context managers for telemetry & instrumentation.

### Phase 1 – Streaming Ingest
1. **Parser Refactor**
   - Adapt existing chunk workers to emit dictionaries containing normalized fields + source offsets for slow queries, authentications, and connections.
   - Add an `OffsetWriter` collecting `(kind, hash_or_id, ts_epoch, file_id, offset, length)` in memory and periodically flushing to disk (Parquet or binary format).
   - Ensure the normalized records align with their Parquet schemas (namespace parsing, username/plan extraction, connection state mapping, CPU/IO metrics).
   - Capture parser telemetry (lines processed, error counts) per event type for later reporting in the manifest/status API.

2. **Parquet Writer**
   - Implement `ParquetChunkWriter` using PyArrow.
   - Flush per chunk or on schedule (e.g., every 50k rows) to avoid large memory footprint.
   - Support compression config (default `snappy`).
   - Perform schema validation on first flush; fail fast if a required column is missing/mistyped, and keep writers per event type isolated to simplify partitioning.

3. **Summary Collector**
   - For each chunk compute summary stats (count, total duration, min/max, histograms, plan mix) for slow queries plus authentication success/failure breakdowns and connection lifecycle counts.
   - Store in `summaries/` as JSON/Parquet for quick UI consumption.
   - Generate per-database namespace summaries and plan efficiency metrics to mirror existing dashboard widgets.

4. **Manifest & File Registry**
   - During ingest, assign `file_id` to each uploaded log.
   - Write `file_map.json` (id → path) and `manifest.json` (ingest time, dataset version, chunk size, compression, record count).
   - Manifest also captures analyzer version, CLI options, ingestion timings (total + chunk averages), per-partition row counts, and checksum/hash of each output artifact for integrity checks.

5. **Concurrency Guard**
   - Create `out/.ingest.lock` while pipeline runs; abort gracefully if another ingest is in progress.
   - Provide `--force` option (with warning) to override stale locks.

6. **CLI Command**
   - New `analyze.py` with `ingest-log` command writing slow query, authentication, and connection parquet datasets in one pass.
   - Automatically allocates stable `file_id` values per source (reusing on re-ingest) while updating the file map and manifest incrementally.
   - Options: `--out`, `--chunk-rows`, `--parquet-compression`, `--copy-source`.
   - Additional subcommands: `status` (prints manifest), `summaries` (view aggregated stats), `rebuild-summaries` (recompute aggregates from existing Parquet), `clean` (remove generated dataset).

### Phase 2 – Analytics Service Layer
1. **DuckDB Integration**
   - Wrap DuckDB connection creation, registering Parquet directories as views (e.g., `slow_queries`).
   - Provide helper functions to run templated SQL with parameter binding.
   - Maintain a simple connection cache (thread-local or request-local) to avoid reopen overhead; expose `close_all()` for test teardown.
   - Expose tunables: `MAX_DUCKDB_THREADS`, `DUCKDB_MEMORY_LIMIT` (read from env and set via `duckdb.sql("SET threads=...")`).

2. **Query APIs** (in `data_store/service.py`)
   - `get_patterns(grouping, filters, limit)` – returns aggregated metrics (count, avg/min/max duration, docs counts, plan mix).
   - `get_trend(grouping, filters, scatter_limit)` – returns aggregated patterns + representative executions.
   - `get_summary(filters)` – total records, top plans, duration percentiles.
   - `get_heatmap(filters)` – precomputed or on-the-fly heatmap data.
   - `get_authentication_summary(filters)` – success/failure counts, mechanisms, top users/IPs.
   - `get_connection_timeline(filters)` – connection churn, unique hosts, session durations.
   - `search_logs(keyword, filters, limit)` – regex/text search across Parquet partitions (using DuckDB `regexp_matches`) with fallback to offset index for scoped scans.

3. **Fallback**
   - If DuckDB import fails, degrade gracefully to Polars (document limitations).
   - Disable scatter plots and expensive percentile charts when fallback active; surface banner in UI warning about degraded analytics.

4. **Query Result Caching**
   - Implement LRU cache (per-process) keyed by `(route, filters, limit)` to avoid re-running identical aggregations during rapid UI navigation.
   - Invalidate cache when a new `manifest.json` appears or when `dataset_version` increments.

### Phase 3 – Detail Lookup
1. **Offset Index**
   - Write `query_offsets.parquet` storing `query_hash, ts_epoch, file_id, offset, length`.
   - Provide `get_offsets(query_hash, filters, limit)` returning the top occurrences.

2. **Raw Retrieval Module**
   - Use `mmap` to open original log file (from `file_map.json`) and read `length` bytes at `offset`.
   - Parse JSON, return to caller. Fallback to stored `sample_query` if parsing fails.
   - Record access telemetry to `summaries/detail_access.log` (query hash, duration, result) for operational debugging.
   - Support gzipped managed copies by detecting `.gz` extension and falling back to `gzip.open` when necessary.

3. **Expose via Flask**
   - Update detail modal endpoint (`/query-trend/details`, etc.) to call this module and provide text search.
   - Ensure error handling for missing offsets or missing files.

### Phase 4 – UI Integration
- **Upload Workflow**
  - [x] Replace SQLite ingestion hook with the v2 `process_uploads` pipeline, refreshing the DuckDB views after each ingest while keeping existing flash feedback.
  - [x] Surface ingest telemetry (rows processed, durations) in both upload flash messages and the shared status panel fed by `/processing-status` so all operators can monitor progress.
  - [x] Provide `MONGO_SLOWQ_WIPE_ON_UPLOAD=1` escape hatch to wipe existing Parquet outputs before each ingest for legacy-style single-run workflows.

- **Flask Entry & Blueprint Wiring**
  - [x] Provide `app_v2.py` that registers `log_analyzer_v2.web.routes`, manages DuckDB lifecycle, and renders shared templates alongside the legacy app for gradual rollout.

- **Dashboard Migration**
  - [x] Swap `/dashboard` to use DuckDB-backed analytics, preserve legacy template contract, and add timezone-safe filter round-tripping (including `/v2/dashboard/resolve-timestamp`).
  - [ ] Add automated parity checks comparing legacy vs v2 dashboard metrics (counts, top lists) on fixture datasets.

- **Remaining Route Parity**
  - [x] Migrate `/slow-queries` to the v2 service layer, keeping legacy template keys and pagination/search behaviour.
  - [x] Migrate `/query-trend` to the v2 service layer, including pattern grouping parity, system-db filtering, and table sorting controls.
  - [x] Migrate `/slow-query-analysis` to the v2 service layer, reimplementing pattern heuristics, modal payloads, and export flows.
  - [x] Migrate `/index-suggestions` to the v2 service layer, ensuring JSON endpoints and templates stay backward compatible.
  - [x] Update corresponding JS (e.g., `static/js/*_v2.js`) to consume the `/v2` JSON APIs and remove direct SQLite dependencies.

- **Status & Diagnostics**
  - [x] Extend `/processing-status` to report v2 pipeline phases (ingest, summarise, ready) using manifest data.
  - [x] Document rollback/feature-flag steps so operators can flip between legacy and v2 UIs during cutover.

### Phase 5 – Testing & Validation
1. **Unit Tests**
   - Parser: offsets & normalization.
   - Parquet writer: schema correctness.
   - Service layer: run queries against synthetic Parquet dataset.
   - Raw lookup: offset fetch + JSON parse.
   - Flask blueprint: smoke tests hitting `/v2` endpoints with fixture dataset.

2. **Integration Tests**
   - Ingest sample logs (50 MB) and verify UI pages.
   - Benchmark large ingest (3 GB) to ensure acceptable wall clock and memory footprint.

3. **Performance Monitoring**
   - Add metrics on ingest time, query latency, file sizes.
   - Offer CLI command to recompute summaries from Parquet without re-parsing logs.
   - Capture ingestion and query metrics in `manifest.json` for historical comparison (e.g., rows per second, average query latency).

4. **Load Testing**
   - Stress-test concurrent DuckDB queries (multiple Flask requests) to validate connection pooling and memory usage.
   - Vet CLI ingest while UI queries run to ensure read/write isolation thanks to Parquet snapshot model.

### Phase 6 – Documentation
- Update README with new ingest instructions, dependency list, configuration, and storage layout.
- Provide troubleshooting (e.g., DuckDB not installed, missing raw file).
- Document retention/cleanup strategy for `out/` directory.

--------------------------------------------------------------------------------
## 5. Data Storage Design Rationale

- **Columnar Parquet** keeps storage compact, supports predicate pushdown, and integrates with DuckDB/Polars for analytic queries without full scans.
- **Offset Index** allows the UI to fetch full raw JSON lazily from the original log, preventing duplication of large payloads.
- **Partitioning by day** enables fast time-range queries and incremental append.
- **Summary Files** decouple expensive aggregations from real-time views, enabling quick chart rendering even on large datasets.
- **Manifest + File Map** provide a reproducible dataset snapshot for audits or reprocessing.

--------------------------------------------------------------------------------
## 6. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Large logs consume too much RAM during chunk buffering | Flush Parquet every N rows; limit in-memory buffers; stream offsets |
| DuckDB missing in environment | Optional dependency; fall back to Polars/pyarrow; document instructions |
| Detail lookup fails when original log moved | Optionally copy log into `out/source/`; warn in UI if file missing |
| Legacy SQLite still referenced | Leave legacy code path for backward compatibility until rollout complete |
| Concurrent ingest/trend queries | Adopt simple file locks or status flags; service layer reads are read-only |
| Schema evolution required later | Version Parquet schema and manifest; supply re-ingest script to regenerate Parquet from raw logs |
| DuckDB connection leaks / file locks | Wrap connections in context managers; close on request teardown; expose health endpoint verifying resource cleanup |
| Accumulated Parquet storage | Provide retention tooling (`analyze.py clean --before <date>`); document disk monitoring |

--------------------------------------------------------------------------------
## 7. Next Steps

1. ~~Set up new package structure and dependency list.~~ ✅ Initial `log_analyzer_v2/` scaffolding with config + utils created (see repo).
2. ~~Prototype chunk-to-Parquet pipeline on a small log (unit test).~~ ✅ `log_analyzer_v2.ingest` writes slow query, authentication, and connection events to Parquet via CLI prototype.
3. ~~Build service layer + DuckDB queries.~~ ✅ `DuckDBService` now powers all `/v2` endpoints (namespace, workload, auth, index suggestions, etc.).
4. ~~Wire detail lookup and update Flask routes.~~ ✅ Blueprints expose `/v2/*` JSON APIs; UI shells consume them via client-side fetch.
5. ~~Finalize CLI, documentation, retention policy, and rollout plan.~~ ✅ CLI now offers `ingest-log`, `status`, `summaries`, and `clean` commands; README documents upload workflow and retention steps.
6. ~~Conduct end-to-end ingest dry run in staging, verify manifest + UI, then cut over default upload path to new pipeline.~~ ✅ Automated fixture ingests sample logs and hits every `/v2` route via `pytest tests/test_v2_endpoints.py`; `/upload/v2` now fronts the ingest path.
7. After cutover, retire SQLite dependencies/artifacts and archive legacy entrypoints once rollback window closes.
