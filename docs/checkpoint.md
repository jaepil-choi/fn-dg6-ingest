# Checkpoint -- 2026-02-09

## Current State

**Branch**: `develop` (all work merged, 47 tests passing)

**Git history** (newest first):

```
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
| Number parsing | `transforms/numbers.py` | Stub |
| Unit normalization | `transforms/units.py` | Stub |
| Empty entity drop | `transforms/empty.py` | Stub |
| Table splitter | `transforms/splitter.py` | Stub |
| Pipeline | `transforms/pipeline.py` | Stub |
| Meta table | `meta.py` | Stub (`_compute_file_hash` implemented, `build_meta_table` not) |
| Exporter | `export.py` | Stub |
| Exceptions | `exceptions.py` | **Done** |

**Tests**: 47 passing (35 config + 12 detect)

---

## What To Do Next

Work should proceed in this order, each as a `feat/` branch from `develop`:

### 1. `feat/transforms` -- Transform Pipeline
The core data cleaning and restructuring logic. All stubs exist; need implementation + unit tests.

| Transform | File | What it does |
|-----------|------|-------------|
| Number parsing | `transforms/numbers.py` | Strip Korean thousand separators (commas), whitespace; cast to numeric dtypes |
| Unit normalization | `transforms/units.py` | Detect suffixes like `(천원)`, `(억원)`; apply multiplier; rename to `(원)` |
| Empty entity drop | `transforms/empty.py` | Remove `코드` rows where all value columns are null across all dates |
| Table splitter | `transforms/splitter.py` | Split one DataFrame into multiple by `아이템명` groups from config `tables:` |
| Pipeline orchestrator | `transforms/pipeline.py` | Run the above transforms in sequence; configurable via `OutputConfig` flags |

**Unit tests needed**: `test_numbers.py`, `test_units.py`, `test_empty.py`, `test_splitter.py` (stubs exist)

### 2. `feat/meta-export` -- Meta Table Builder + Exporter
Depends on transforms (needs `unit_info` and `entity_stats` from the pipeline).

- **`meta.py`**: Implement `build_meta_table()` -- one row per (source_file, 아이템명) with lineage columns (see FR-9 in PRD)
- **`export.py`**: Write data tables + `_meta` to disk in CSV or Parquet format
- Tests: unit tests for meta table construction, integration tests for file I/O

### 3. `feat/pipeline` -- Public API Wiring
Depends on everything above. Wire up the full end-to-end flow.

- **`__init__.py`**: Implement `init()` (first run: detect -> parse -> generate config) and `ingest()` (subsequent: load config -> parse -> transforms -> export)
- Integration tests: `test_timeseries.py`, `test_misc_etf.py`, `test_config_roundtrip.py`, `test_export.py`

### 4. Deferred (Phase 2)
- Time series **long** parser (vertical orientation of time series data)
- **Snapshot** parser (needs sample data from DataGuide 6)
- CLI entry point (`argparse` or `click`)
- Additional `layouts/*.yaml` for newly encountered formats
- Documentation beyond PRD

---

## Git Strategy Reminder

- **Hybrid Gitflow**: `feat/<name>` branches from `develop`, merge with `--no-ff`
- **Commit task-by-task**, run tests before each merge
- **`main`** branch reserved until v1.0
- **Always use** `uv run python -m ...` for execution, never bare `python`
- **Never edit** `pyproject.toml` for deps directly; use `uv add`
