# Checkpoint -- 2026-02-10

## Current State

**Branch**: `develop` (all work merged, 113 tests passing)

**Git history** (newest first):

```
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
| Meta table | `meta.py` | Stub (`_compute_file_hash` implemented, `build_meta_table` not) |
| Exporter | `export.py` | Stub |
| Exceptions | `exceptions.py` | **Done** |

**Tests**: 113 passing (35 config + 12 detect + 66 transforms)

---

## What To Do Next

Work should proceed in this order, each as a `feat/` branch from `develop`:

### 1. `feat/meta-export` -- Meta Table Builder + Exporter
Depends on transforms (needs `unit_info` and `drop_result` from `PipelineResult`).

- **`meta.py`**: Implement `build_meta_table()` -- one row per (source_file, 아이템명) with lineage columns (see FR-9 in PRD). The function signature already exists and accepts:
  - `config: IngestConfig`
  - `items: list[ItemInfo]` (from `ParseResult.items`)
  - `source_last_updated: str | None`
  - `table_assignment: dict[str, str]` (아이템명 -> table_name)
  - `unit_info: dict[str, tuple[str, int]]` (from `PipelineResult.unit_info`)
  - `entity_stats: dict[str, tuple[int, int]]` (from `PipelineResult.drop_result`)
  - `_compute_file_hash()` is already implemented.

- **`export.py`**: Implement `export_tables()` -- write data tables + `_meta` to disk. The function signature already exists and accepts:
  - `tables: dict[str, pd.DataFrame]`
  - `meta_df: pd.DataFrame`
  - `output_dir: str | Path`
  - `output_format: Literal["csv", "parquet"]`
  - Returns `list[str]` of written file paths.

- Tests: unit tests for meta table construction (synthetic data), unit tests for export I/O (using `tmp_path`).

### 2. `feat/api` -- Public API Wiring
Depends on everything above. Wire up the full end-to-end flow.

- **`__init__.py`**: Implement `init()` (first run: detect -> parse -> generate config -> optionally run pipeline) and `ingest()` (subsequent: load config -> parse -> transforms -> export)
- Integration tests: `test_timeseries.py`, `test_misc_etf.py`, `test_config_roundtrip.py`, `test_export.py`

### 3. Deferred (Phase 2)
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

---

## Git Strategy Reminder

- **Hybrid Gitflow**: `feat/<name>` branches from `develop`, merge with `--no-ff`
- **Commit task-by-task**, run tests before each merge
- **`main`** branch reserved until v1.0
- **Always use** `uv run python -m ...` for execution, never bare `python`
- **Never edit** `pyproject.toml` for deps directly; use `uv add`
