# Checkpoint -- 2026-02-10

## Current State

**Branch**: `feat/meta-export` (from `develop`, not yet merged; 150 tests passing)

**Git history** (newest first):

```
* d184697 (feat/meta-export) Implement meta table builder and exporter with 37 unit tests
* 14896da (develop) Update checkpoint: transforms done, 113 tests, next is meta-export
*   bce527b Merge feat/transforms into develop
|\
| * b454428 Implement transform pipeline: numbers, units, empty drop, splitter
|/
* 3a8f8be Save a checkpoint
*   211762d Merge fix/parser-architecture into develop
|\
| * d775928 Update PRD for timeseries/snapshot/misc taxonomy and layout system
| * ae4d1b0 Fix tests and rename layout loader to avoid name collision
| * ba3e0fa Fix detect.py, config.py, meta.py for new architecture
| * 93609bf Restructure parsers: wide/long -> timeseries/snapshot/misc
| * ea5b4c1 Add coordinate-based layout system for format definitions
|/
*   91a0431 Merge feat/config: Pydantic models, YAML I/O, 35 unit tests
|   d6f3385 Implement config models, YAML I/O, and cross-validation
*   d00a46f Merge feat/scaffold: PRD, package layout, test structure
|   6e9dadb Add PRD, package scaffold, and test structure
*   44866ad Install dependencies
*   ce25106 Init uv project
*   9680ea6 Initial commit
```

---

## What's Been Built

### 1. Project Scaffold (`feat/scaffold`)
- PRD at `docs/vibe/prd.md`
- Full package layout with docstrings in every file
- Test structure: `tests/unit/` + `tests/integration/` with `conftest.py` for path constants
- `pyproject.toml` with dependencies: pandas, pyarrow, pydantic, pyyaml

### 2. Config System (`feat/config`)
- **`config.py`**: Pydantic models (`IngestConfig`, `SourceConfig`, `MetadataConfig`, `OutputConfig`)
- YAML I/O: `load_config()`, `save_config()`, `generate_default_config()`
- Cross-validation: `validate_tables_against_data()` checks config items against source data
- **35 unit tests** in `test_config.py` (round-trip, error cases, validation)

### 3. Parser Architecture (`fix/parser-architecture`)
Major restructure to match DataGuide 6's actual taxonomy:

- **Layout system** (`layout_registry.py` + `layouts/*.yaml`):
  Coordinate-based metadata extraction. Each format has a YAML file defining cell `(row, col)` coordinates for detection rules and settings. No heuristic scanning.
  - `layouts/timeseries_wide.yaml` -- OHLCV, sales-consensus
  - `layouts/misc_etf.yaml` -- ETF constituent data

- **Format detection** (`detect.py`):
  Priority-ordered layout matching (timeseries -> snapshot -> misc). Returns `(parser_class, layout)` tuple.

- **Parsers** (`parsers/`):
  - `timeseries.py` -- `TimeSeriesWideParser`: melt+pivot wide data, layout-based metadata
  - `misc.py` -- `MiscParser`: data-driven key/value column detection via numeric ratio
  - `snapshot.py` -- `SnapshotParser`: placeholder stub
  - `base.py` -- `BaseParser` ABC, `ParseResult`, `ItemInfo` dataclasses

- **MetadataConfig** now uses semantic English keys:
  `frequency`, `currency`, `sort_order`, `non_business_days`, `include_weekends`, `period_start`, `period_end`, `extra` (catch-all dict)

- **SourceConfig.detected_format** is a free-form string matching the layout's `format_name` (e.g., `timeseries_wide`, `misc_etf`)

- **12 additional unit tests** in `test_detect.py` (synthetic + real file smoke tests)

### 4. Transform Pipeline (`feat/transforms`)
Core data cleaning and restructuring logic, all tested:

- **`transforms/numbers.py`** -- `parse_numbers()`:
  Strips comma thousand separators and whitespace from non-key columns, coerces to numeric dtypes via `pd.to_numeric(errors='coerce')`. Empty strings become `NaN`. Key columns (코드, 코드명, date) are left as strings.

- **`transforms/units.py`** -- `normalize_units()`:
  Detects monetary unit suffixes (`(천원)`, `(억원)`, `(십억원)`, etc.), multiplies values by the correct multiplier, and renames column suffixes to `(원)`. Non-monetary suffixes like `(주)`, `(%)` are untouched. Returns `unit_info` dict for the `_meta` table. Helper functions `detect_unit()` and `normalize_column_name()` are also exposed.

- **`transforms/empty.py`** -- `drop_empty_entities()`:
  Groups by entity column (`코드`), drops entities where all value columns are `NaN` across all rows. Returns `DropResult` dataclass with `df`, `entities_total`, `entities_dropped`.

- **`transforms/splitter.py`** -- `split_tables()`:
  Splits a DataFrame into multiple tables based on `tables:` groups from config. Key columns always included. Missing columns logged as warnings (handles post-unit-rename mismatches gracefully).

- **`transforms/pipeline.py`** -- `TransformPipeline`:
  Orchestrates all 4 steps in sequence: numbers -> units -> empty drop -> split. Respects config flags (`normalize_units`, `drop_empty_entities`). `_resolve_table_groups()` remaps item names after unit renaming so the splitter finds the correct columns. Returns `PipelineResult` dataclass carrying `tables`, `unit_info`, and `drop_result` -- everything the `_meta` table builder needs downstream.

- **66 new unit tests** across 5 test files:
  - `test_numbers.py` (14): commas, decimals, negatives, whitespace, NaN, key column preservation
  - `test_units.py` (20): all 5 unit multipliers, non-monetary suffixes, nested parentheses, scaling, renaming
  - `test_empty.py` (10): drop/keep logic, entity counts, custom columns, edge cases
  - `test_splitter.py` (10): single/multi group, key columns, missing columns, misc format
  - `test_pipeline.py` (7): full pipeline, disabled steps, unit+group resolution, custom key columns

### 5. Meta Table Builder + Exporter (`feat/meta-export`)
Data lineage and output writing, completing the pre-API pipeline:

- **`meta.py`** -- `build_meta_table()`:
  Assembles the flat `_meta` DataFrame with one row per `(source_file, 아이템명)`. Columns span three layers:
  - **Source-level** (same for all rows): `source_file` (basename only), `source_hash` (SHA-256), `source_last_updated`, `detected_format`, `frequency`, `period_start`, `period_end`, `non_business_days`, `include_weekends`
  - **Item-level** (per-row): `아이템코드`, `아이템명`, `아이템명_normalized` (post-unit-rename), `유형`, `집계주기`
  - **Processing** (derived from pipeline outputs): `table_name`, `unit_original`, `unit_multiplier`, `entities_total`, `entities_dropped`, `processed_at` (UTC ISO-8601)

  Helper `_resolve_normalized_name()` reuses `normalize_column_name()` from the units transform to derive post-rename names. Source hash falls back to `""` if file is missing (testability). Explicit column schema ensures correct structure even with zero items.

- **`export.py`** -- `export_tables()`:
  Writes data tables + `_meta` to disk. Naming: `{table_name}.{format}`, `_meta.{format}`.
  - **Parquet** (default): PyArrow engine, preserves dtypes.
  - **CSV**: `utf-8-sig` encoding so Korean text opens correctly in Excel.
  - Creates output directory recursively. Validates format upfront (`ExportError` on unsupported).
  - Returns `list[str]` of written paths (data tables first, `_meta` last).

- **37 new unit tests** across 2 test files:
  - `test_meta.py` (20): column completeness (all 20 FR-9 columns), one-row-per-item, source hash, unit_info integration, entity_stats per-table, metadata propagation, edge cases (empty items, missing files, missing table assignment, processed_at format)
  - `test_export.py` (13): CSV/Parquet write + round-trip, directory creation, dtype preservation, return path order, unsupported format error, empty tables, string output_dir

  Helper `_compute_file_hash()` also tested (3 tests): deterministic, content-dependent, missing file.

---

## Module Status

| Module | File | Status |
|--------|------|--------|
| Public API | `__init__.py` | Stub (`init()`, `ingest()` signatures only) |
| Config | `config.py` | **Done** -- models, YAML I/O, validation |
| Detection | `detect.py` | **Done** -- layout-based coordinate detection |
| Layout loader | `layout_registry.py` | **Done** -- Pydantic models, YAML loader, `extract_settings()` |
| Layout defs | `layouts/*.yaml` | **Done** -- `timeseries_wide`, `misc_etf` |
| Parser base | `parsers/base.py` | **Done** -- ABC, `ParseResult`, `ItemInfo` |
| TS Wide parser | `parsers/timeseries.py` | **Done** -- melt+pivot, metadata extraction |
| Misc parser | `parsers/misc.py` | **Done** -- numeric column detection, metadata |
| Snapshot parser | `parsers/snapshot.py` | Stub (no sample data) |
| Number parsing | `transforms/numbers.py` | **Done** -- strip commas/whitespace, coerce to numeric |
| Unit normalization | `transforms/units.py` | **Done** -- detect suffix, scale, rename to (원) |
| Empty entity drop | `transforms/empty.py` | **Done** -- drop all-null entities, report counts |
| Table splitter | `transforms/splitter.py` | **Done** -- split by 아이템명 groups, key columns preserved |
| Pipeline | `transforms/pipeline.py` | **Done** -- orchestrator with `PipelineResult`, config-driven |
| Meta table | `meta.py` | **Done** -- `build_meta_table()`, `_compute_file_hash()`, `_resolve_normalized_name()` |
| Exporter | `export.py` | **Done** -- CSV + Parquet, directory creation, format validation |
| Exceptions | `exceptions.py` | **Done** |

**Tests**: 150 passing (35 config + 12 detect + 66 transforms + 20 meta + 13 export + 4 helpers)

---

## What To Do Next

### 0. Merge `feat/meta-export` into `develop`
Before starting the API layer:
```
git checkout develop
git merge --no-ff feat/meta-export
```

### 1. `feat/api` -- Public API Wiring + Integration Tests
This is the **final Phase 1 task**. Wire up the full end-to-end flow by implementing the two public entry points in `__init__.py`, then validate with comprehensive integration tests against real DataGuide 6 files.

#### `__init__.py` Implementation

**`init(input_path, output_dir, config_path, run_immediately) -> str`**:
First-run workflow. Steps to orchestrate:
1. `detect_format(input_path)` -> `(parser_class, layout)`
2. `parser.parse(input_path, layout)` -> `ParseResult` (with `df`, `metadata`, `items`, `source_last_updated`, `format_name`)
3. `generate_default_config(input_path, format_name, metadata, discovered_items, output_dir)` -> `IngestConfig`
4. `save_config(config, config_path)`
5. If `run_immediately=True`:
   - `TransformPipeline(config).run(df, key_columns)` -> `PipelineResult`
   - Build `table_assignment` dict from `config.tables` (invert the `{table_name: [items]}` mapping)
   - Build `entity_stats` dict from `PipelineResult.drop_result` (one entry per table; note that empty-entity drop runs pre-split, so all tables share the same `(total, dropped)` -- but structure it per-table for future flexibility)
   - `build_meta_table(config, items, source_last_updated, table_assignment, unit_info, entity_stats)` -> `meta_df`
   - `export_tables(tables, meta_df, output_dir, output_format)` -> written paths
6. Return `config_path`

Key decision: **`key_columns`** differ by format category. Time series uses `["코드", "코드명", "date"]`; misc formats use whatever the parser produces (e.g., `["날짜", "ETF코드", "ETF명", "구성종목코드", "구성종목"]`). The parser or layout should inform this. Options:
- Add a `key_columns` field to `ParseResult` (cleanest -- parser already knows which columns are keys).
- Infer from layout's `format_category` (fragile if new categories appear).
- Recommendation: **add `key_columns: list[str]`** to `ParseResult` and have each parser populate it.

**`ingest(config_path) -> list[str]`**:
Subsequent-run workflow. Steps:
1. `load_config(config_path)` -> `IngestConfig` (Pydantic validation on load)
2. `detect_format(config.source.input_path)` -> `(parser_class, layout)` (re-detect to get parser; could alternatively dispatch on `config.source.detected_format` directly)
3. `parser.parse(config.source.input_path, layout)` -> `ParseResult`
4. `validate_tables_against_data(config, available_items)` -- cross-check config items vs source
5. `TransformPipeline(config).run(df, key_columns)` -> `PipelineResult`
6. Build `table_assignment`, `entity_stats` (same as in `init`)
7. `build_meta_table(...)` -> `meta_df`
8. `export_tables(...)` -> written paths
9. Return written paths

Shared logic between `init` and `ingest` (steps 5-8) should be factored into a private helper like `_run_pipeline_and_export(config, parse_result) -> list[str]` to avoid code duplication.

#### Integration Tests (Critical)

Integration tests run the **full pipeline** against real DataGuide 6 files from `inputs/`. They use `tmp_path` for output directories and are marked `@pytest.mark.integration`. File path constants are in `tests/conftest.py`.

**`tests/integration/test_timeseries.py`** -- Time Series Wide end-to-end:
- `test_init_ohlcv_csv`: `init()` against OHLCV CSV -> verify config generated, output files exist, _meta has correct row count and column schema
- `test_init_ohlcv_run_immediately_false`: `init(run_immediately=False)` -> config generated, NO output files
- `test_ingest_ohlcv_csv`: `init()` then `ingest()` against same file -> outputs identical or at least structurally consistent
- `test_init_sales_consensus_csv`: `init()` against sales-consensus CSV -> verify unit normalization happened (columns renamed from `(억원)` to `(원)`), multipliers in `_meta`
- `test_ingest_custom_table_groups`: `init()`, modify config to split into `ohlcv`+`volume` tables, `ingest()` -> verify two separate output tables + key columns in each
- `test_ingest_parquet_output`: verify Parquet output preserves numeric dtypes (read back and check)
- `test_ingest_csv_output`: verify CSV output is readable and contains Korean text

**`tests/integration/test_misc_etf.py`** -- Misc/ETF end-to-end:
- `test_init_etf_csv`: `init()` against ETF constituent CSV -> verify config format is `misc_etf`, data columns correct
- `test_ingest_etf_csv`: `init()` then `ingest()` -> output files exist, _meta correct
- `test_etf_key_columns_preserved`: output table has the correct key columns for misc format (not 코드/코드명/date)

**`tests/integration/test_config_roundtrip.py`** -- Config fidelity:
- `test_init_generates_valid_config`: `init()` -> load generated config -> Pydantic validation passes
- `test_config_survives_edit_cycle`: `init()` -> load config -> modify tables -> save -> `ingest()` succeeds
- `test_ingest_rejects_bad_item_names`: edit config with non-existent 아이템명 -> `ingest()` raises `ConfigValidationError`
- `test_config_metadata_matches_source`: metadata in generated config matches what the parser extracts

**`tests/integration/test_export.py`** -- Output validation:
- `test_meta_table_schema`: _meta output has all 20 FR-9 columns
- `test_meta_row_count_matches_items`: _meta row count == number of unique 아이템명 in the source
- `test_output_files_named_correctly`: filenames match `{table_name}.{format}`
- `test_parquet_dtypes_numeric`: numeric columns in Parquet output are float/int, not object

#### `ParseResult.key_columns` Extension
If we add `key_columns` to `ParseResult`:
- `ParseResult` dataclass gets `key_columns: list[str] = field(default_factory=list)`.
- `TimeSeriesWideParser.parse()` sets `key_columns=["코드", "코드명", "date"]`.
- `MiscParser.parse()` populates from its detected non-numeric columns.
- `__init__.py` passes `parse_result.key_columns` to `TransformPipeline.run()`.

### 2. Deferred (Phase 2)
- Time series **long** parser (vertical orientation of time series data)
- **Snapshot** parser (needs sample data from DataGuide 6)
- CLI entry point (`argparse` or `click`)
- Additional `layouts/*.yaml` for newly encountered formats
- Documentation beyond PRD

---

## Lessons Learned

### Layout Detection Assumptions (from `fix/parser-architecture`)
The initial architecture incorrectly modeled DataGuide 6 as "wide vs long" formats. The **actual** taxonomy is:
- **Time Series** (most common): OHLCV, sales-consensus. Currently only **wide** orientation.
- **Snapshot**: Another DG6 top-level category (not yet implemented).
- **Custom/Misc**: ETF constituents and other unpredictable formats (already in relational form).

The ETF constituent format was **not** a "long version of time series" -- it was a completely different data category with its own header structure. This led to a major restructure (`fix/parser-architecture`) replacing heuristic scanning with coordinate-based layout YAML files. **Monitor for similar assumptions** when adding new formats.

### pandas 2.x Compatibility
- pandas 2.x with PyArrow backend uses `StringDtype` instead of `object` for string columns. Use `pd.api.types.is_string_dtype()` for dtype checks, not `== object`.
- Copy-on-Write is default in pandas 2.x. Chained assignment (e.g., `df["col"].iloc[0] = val`) raises `ChainedAssignmentError`. Use `.loc[row, col] = val` instead.

### Meta Table Design (from `feat/meta-export`)
- The `_meta` table is **descriptive** (execution record), not a duplicate of `fnconfig.yaml` (which is **prescriptive**). They serve different roles and should not be conflated.
- `entity_stats` is structured per-table even though the current empty-entity drop runs pre-split (same stats for all tables). This future-proofs for per-table entity management without changing the `build_meta_table()` signature.
- Explicit column schema on `pd.DataFrame(rows, columns=...)` prevents silent schema drift when the items list is empty.

---

## Git Strategy Reminder

- **Hybrid Gitflow**: `feat/<name>` branches from `develop`, merge with `--no-ff`
- **Commit task-by-task**, run tests before each merge
- **`main`** branch reserved until v1.0
- **Always use** `uv run python -m ...` for execution, never bare `python`
- **Never edit** `pyproject.toml` for deps directly; use `uv add`
