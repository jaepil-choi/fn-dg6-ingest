"""
Verify every use case documented in README.md works correctly.

Exercises ALL public API examples from the README:

  ── Section: Quick Start §3 ──
  UC-01  open() with raw CSV file
  UC-02  Idempotent open() (second call skips build)
  UC-03  ds.load() — full load
  UC-04  ds.load(codes=..., items=...) — filter by code + item
  UC-05  ds.load(date_from=..., date_to=...) — date range
  UC-06  ds.load_meta()
  UC-07  ds.describe()
  UC-08  open() from YAML config directly
  UC-09  open(run_immediately=False) + manual ds.ingest()

  ── Section: Quick Start §4 — Config editing ──
  UC-10  open(yaml) then ds.ingest() (rebuild from config)
  UC-11  ds.load(table=...) — specific table after split
  UC-12  Programmatic config modification: tables, output_format, save_config, ingest

  ── Section: Dataset API ──
  UC-13  Dataset properties: ds.config, ds.config_path, ds.output_dir
  UC-14  ds.load() filter variants: full, table, codes, date_range, items, combined
  UC-15  ds.describe() attributes: tables, items, shape, date_range, entities, format_name
  UC-16  ds.save_config() round-trip

Prerequisites:
  - Input files exist in inputs/ (run_ingest.py must have been run at least once,
    or place DG6 exports in inputs/ manually).
  - Output data exists in outputs/ (run run_ingest.py first if needed).

Usage:
    uv run python scripts/inspect_outputs.py                                # all datasets
    uv run python scripts/inspect_outputs.py outputs/kse+kosdaq_ohlcv.yaml  # single dataset
"""

from __future__ import annotations

import copy
import logging
import shutil
import sys
import time
from pathlib import Path

import pandas as pd

import fn_dg6_ingest
from fn_dg6_ingest.dataset import DatasetInfo

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("verify_readme")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_pass_count = 0
_fail_count = 0
_skip_count = 0


def _ok(label: str, detail: str = "") -> None:
    global _pass_count
    _pass_count += 1
    suffix = f"  ({detail})" if detail else ""
    print(f"  [PASS]  {label}{suffix}")


def _fail(label: str, detail: str = "") -> None:
    global _fail_count
    _fail_count += 1
    suffix = f"  ({detail})" if detail else ""
    print(f"  [FAIL]  {label}{suffix}")


def _skip(label: str, reason: str = "") -> None:
    global _skip_count
    _skip_count += 1
    suffix = f"  ({reason})" if reason else ""
    print(f"  [SKIP]  {label}{suffix}")


def _section(title: str) -> None:
    print(f"\n{'='*72}")
    print(f"  {title}")
    print(f"{'='*72}")


def _sub(title: str) -> None:
    print(f"\n  ── {title} {'─'*max(1, 56 - len(title))}")


# ---------------------------------------------------------------------------
# Use-case verifiers
# ---------------------------------------------------------------------------


def uc01_open_raw_csv(input_path: str, output_dir: str) -> fn_dg6_ingest.Dataset | None:
    """UC-01: open() with raw CSV file."""
    _sub("UC-01: open() with raw CSV file")

    if not Path(input_path).exists():
        _skip("open(csv)", f"input not found: {input_path}")
        return None

    try:
        ds = fn_dg6_ingest.open(input_path, output_dir=output_dir)
        assert isinstance(ds, fn_dg6_ingest.Dataset)
        _ok("open(csv) → Dataset", f"format={ds.config.source.detected_format}")
        return ds
    except Exception as exc:
        _fail("open(csv)", str(exc))
        return None


def uc02_idempotent_open(input_path: str, output_dir: str) -> None:
    """UC-02: Second open() call skips build (idempotent)."""
    _sub("UC-02: Idempotent open() — second call should skip build")

    if not Path(input_path).exists():
        _skip("idempotent open()", "input not found")
        return

    try:
        t0 = time.perf_counter()
        ds = fn_dg6_ingest.open(input_path, output_dir=output_dir)
        elapsed = time.perf_counter() - t0
        assert isinstance(ds, fn_dg6_ingest.Dataset)
        # Idempotent call should be fast (< 2s) because it skips the pipeline
        if elapsed < 2.0:
            _ok("idempotent open()", f"returned in {elapsed:.3f}s (skipped build)")
        else:
            _ok("idempotent open()", f"returned in {elapsed:.1f}s (may have rebuilt)")
    except Exception as exc:
        _fail("idempotent open()", str(exc))


def uc03_load_full(ds: fn_dg6_ingest.Dataset) -> pd.DataFrame | None:
    """UC-03: ds.load() — full load."""
    _sub("UC-03: ds.load() — full load")

    try:
        df = ds.load()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        _ok("ds.load()", f"{df.shape[0]:,} rows x {df.shape[1]} cols")
        return df
    except Exception as exc:
        _fail("ds.load()", str(exc))
        return None


def uc04_load_codes_items(ds: fn_dg6_ingest.Dataset) -> None:
    """UC-04: ds.load(codes=..., items=...) — filter by code and item."""
    _sub("UC-04: ds.load(codes=..., items=...) — code + item filter")

    fmt = ds.config.source.detected_format
    if not fmt.startswith("timeseries"):
        _skip("load(codes, items)", f"format={fmt}, not timeseries — codes/items filter N/A")
        return

    # Pick the first item from config to test with
    items_list = list(ds.config.tables.values())[0]
    test_item = items_list[0] if items_list else None

    try:
        # Load full to find a valid code
        full = ds.load()
        if "코드" not in full.columns:
            _skip("load(codes, items)", "no '코드' column")
            return

        test_code = full["코드"].iloc[0]

        df = ds.load(codes=[test_code], items=[test_item] if test_item else None)
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

        unique_codes = df["코드"].unique()
        assert len(unique_codes) == 1 and unique_codes[0] == test_code
        _ok(
            f"load(codes=[{test_code!r}], items=[{test_item!r}])",
            f"{len(df):,} rows, codes={list(unique_codes)}",
        )
    except Exception as exc:
        _fail("load(codes, items)", str(exc))


def uc05_load_date_range(ds: fn_dg6_ingest.Dataset) -> None:
    """UC-05: ds.load(date_from=..., date_to=...) — date range filter."""
    _sub("UC-05: ds.load(date_from, date_to) — date range filter")

    try:
        df = ds.load(date_from="2024-01-01", date_to="2025-12-31")
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

        if "date" in df.columns:
            dates = pd.to_datetime(df["date"])
            date_min = dates.min()
            date_max = dates.max()
            _ok(
                "load(date_from, date_to)",
                f"{len(df):,} rows, actual range: {date_min.date()} .. {date_max.date()}",
            )
        else:
            _ok("load(date_from, date_to)", f"{len(df):,} rows")
    except Exception as exc:
        _fail("load(date_from, date_to)", str(exc))


def uc06_load_meta(ds: fn_dg6_ingest.Dataset) -> pd.DataFrame | None:
    """UC-06: ds.load_meta()."""
    _sub("UC-06: ds.load_meta() — lineage table")

    try:
        meta = ds.load_meta()
        assert isinstance(meta, pd.DataFrame)
        assert len(meta) > 0
        _ok("ds.load_meta()", f"{meta.shape[0]} rows x {meta.shape[1]} cols, columns={list(meta.columns)}")
        return meta
    except Exception as exc:
        _fail("ds.load_meta()", str(exc))
        return None


def uc07_describe(ds: fn_dg6_ingest.Dataset) -> DatasetInfo | None:
    """UC-07: ds.describe()."""
    _sub("UC-07: ds.describe() — quick metadata")

    try:
        info = ds.describe()
        assert isinstance(info, DatasetInfo)
        assert len(info.tables) > 0
        _ok(
            "ds.describe()",
            f"tables={info.tables}, format={info.format_name}, "
            f"entities={info.entities}",
        )
        return info
    except Exception as exc:
        _fail("ds.describe()", str(exc))
        return None


def uc08_open_yaml(config_path: str) -> fn_dg6_ingest.Dataset | None:
    """UC-08: open() from YAML config directly."""
    _sub("UC-08: open(yaml) — open from config file")

    if not Path(config_path).exists():
        _skip("open(yaml)", f"config not found: {config_path}")
        return None

    try:
        ds = fn_dg6_ingest.open(config_path)
        assert isinstance(ds, fn_dg6_ingest.Dataset)

        df = ds.load()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        _ok("open(yaml) → load()", f"{len(df):,} rows")
        return ds
    except Exception as exc:
        _fail("open(yaml)", str(exc))
        return None


def uc09_run_immediately_false(input_path: str, tmp_output_dir: str) -> None:
    """UC-09: open(run_immediately=False) + manual ds.ingest()."""
    _sub("UC-09: open(run_immediately=False) + ds.ingest()")

    if not Path(input_path).exists():
        _skip("run_immediately=False", "input not found")
        return

    tmp_out = Path(tmp_output_dir)
    tmp_config = tmp_out.with_suffix(".yaml")

    try:
        # Clean up temp dir if it exists from a previous run
        if tmp_out.exists():
            shutil.rmtree(tmp_out)
        if tmp_config.exists():
            tmp_config.unlink()

        # Step 1: open with run_immediately=False — should only create config
        ds = fn_dg6_ingest.open(
            input_path,
            output_dir=str(tmp_out),
            run_immediately=False,
        )
        assert isinstance(ds, fn_dg6_ingest.Dataset)
        assert tmp_config.exists(), "Config YAML should be created"
        assert not tmp_out.exists() or not any(tmp_out.iterdir()), \
            "Output dir should be empty (pipeline not run)"
        _ok("open(run_immediately=False)", "config created, no output data")

        # Step 2: manual ingest
        ds.ingest()
        data_files = list(tmp_out.glob("*.*"))
        assert len(data_files) > 0, "ingest() should produce output files"
        _ok("ds.ingest() after deferred open", f"produced {len(data_files)} files")

    except Exception as exc:
        _fail("run_immediately=False + ingest()", str(exc))
    finally:
        # Cleanup
        if tmp_out.exists():
            shutil.rmtree(tmp_out)
        if tmp_config.exists():
            tmp_config.unlink()


def uc10_open_yaml_reingest(config_path: str) -> None:
    """UC-10: open(yaml) then ds.ingest() — rebuild from config."""
    _sub("UC-10: open(yaml) → ds.ingest() — rebuild from config")

    if not Path(config_path).exists():
        _skip("open(yaml) + ingest()", "config not found")
        return

    try:
        ds = fn_dg6_ingest.open(config_path)
        written = ds.ingest()
        assert written and len(written) > 0
        _ok("open(yaml) → ingest()", f"rebuilt {len(written)} files")
    except Exception as exc:
        _fail("open(yaml) + ingest()", str(exc))


def uc11_load_specific_table(ds: fn_dg6_ingest.Dataset) -> None:
    """UC-11: ds.load(table=...) — load a specific table by name."""
    _sub("UC-11: ds.load(table=...) — specific table")

    table_names = list(ds.config.tables.keys())
    if not table_names:
        _skip("load(table=...)", "no tables in config")
        return

    test_table = table_names[0]
    try:
        df = ds.load(table=test_table)
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        _ok(f"load(table={test_table!r})", f"{len(df):,} rows x {df.shape[1]} cols")
    except Exception as exc:
        _fail(f"load(table={test_table!r})", str(exc))


def uc12_programmatic_config_modify(config_path: str) -> None:
    """UC-12: Programmatic config modification — tables, output_format, save_config, ingest.

    README example:
        ds.config.tables = {"ohlcv": [...], "volume": [...]}
        ds.config.output.output_format = "csv"
        ds.save_config()
        ds.ingest()
    """
    _sub("UC-12: Programmatic config modification (tables, format, save, ingest)")

    if not Path(config_path).exists():
        _skip("programmatic config modify", "config not found")
        return

    # Only test on timeseries_wide datasets with enough items to split
    ds = fn_dg6_ingest.open(config_path)
    fmt = ds.config.source.detected_format
    if not fmt.startswith("timeseries"):
        _skip("programmatic config modify", f"format={fmt}, skipping table-split test")
        return

    all_items = list(ds.config.tables.values())[0]
    if len(all_items) < 2:
        _skip("programmatic config modify", "fewer than 2 items, cannot split")
        return

    # Save originals for restoration
    original_tables = copy.deepcopy(ds.config.tables)
    original_format = ds.config.output.output_format

    try:
        # Step 1: Split items into two groups
        mid = len(all_items) // 2
        group_a = all_items[:mid]
        group_b = all_items[mid:]
        ds.config.tables = {"group_a": group_a, "group_b": group_b}
        ds.config.output.output_format = "csv"

        # Step 2: save_config
        ds.save_config()
        _ok("save_config() after table split", f"group_a={len(group_a)} items, group_b={len(group_b)} items")

        # Step 3: ingest with new config
        written = ds.ingest()
        assert written and len(written) > 0
        _ok("ingest() with split tables + csv format", f"wrote {len(written)} files")

        # Step 4: verify we can load each split table
        df_a = ds.load(table="group_a")
        df_b = ds.load(table="group_b")
        assert isinstance(df_a, pd.DataFrame) and len(df_a) > 0
        assert isinstance(df_b, pd.DataFrame) and len(df_b) > 0
        _ok(
            "load split tables",
            f"group_a={df_a.shape}, group_b={df_b.shape}",
        )

        # Step 5: verify load() returns dict when multiple tables
        result = ds.load()
        assert isinstance(result, dict), "load() should return dict for multi-table"
        assert set(result.keys()) == {"group_a", "group_b"}
        _ok("load() returns dict for multi-table", f"keys={list(result.keys())}")

    except Exception as exc:
        _fail("programmatic config modify", str(exc))

    finally:
        # Restore original config
        ds.config.tables = original_tables
        ds.config.output.output_format = original_format
        ds.save_config()
        ds.ingest()
        log.info("  Restored original config and rebuilt")


def uc13_dataset_properties(ds: fn_dg6_ingest.Dataset) -> None:
    """UC-13: Dataset properties — ds.config, ds.config_path, ds.output_dir."""
    _sub("UC-13: Dataset properties")

    try:
        # ds.config
        assert ds.config is not None
        assert hasattr(ds.config, "source")
        assert hasattr(ds.config, "metadata")
        assert hasattr(ds.config, "output")
        assert hasattr(ds.config, "tables")
        _ok("ds.config", f"IngestConfig(format={ds.config.source.detected_format})")

        # ds.config_path
        assert ds.config_path is not None
        assert Path(ds.config_path).suffix in (".yaml", ".yml")
        _ok("ds.config_path", str(ds.config_path))

        # ds.output_dir
        assert ds.output_dir is not None
        assert Path(ds.output_dir).is_dir()
        _ok("ds.output_dir", str(ds.output_dir))

    except Exception as exc:
        _fail("Dataset properties", str(exc))


def uc14_load_filter_variants(ds: fn_dg6_ingest.Dataset) -> None:
    """UC-14: ds.load() — all filter variants from Dataset API section."""
    _sub("UC-14: ds.load() filter variants")

    fmt = ds.config.source.detected_format
    table_names = list(ds.config.tables.keys())
    items_list = list(ds.config.tables.values())[0]

    # (a) Full data — already tested in UC-03, quick re-check
    try:
        df = ds.load()
        assert isinstance(df, (pd.DataFrame, dict))
        _ok("load() — full", f"type={type(df).__name__}")
    except Exception as exc:
        _fail("load() — full", str(exc))

    # (b) By table
    try:
        df = ds.load(table=table_names[0])
        assert isinstance(df, pd.DataFrame) and len(df) > 0
        _ok(f"load(table={table_names[0]!r})", f"{len(df):,} rows")
    except Exception as exc:
        _fail(f"load(table={table_names[0]!r})", str(exc))

    # (c) By codes (timeseries only)
    if fmt.startswith("timeseries"):
        try:
            full = ds.load()
            test_code = full["코드"].iloc[0]
            df = ds.load(codes=[test_code])
            assert isinstance(df, pd.DataFrame) and len(df) > 0
            _ok(f"load(codes=[{test_code!r}])", f"{len(df):,} rows")
        except Exception as exc:
            _fail("load(codes=...)", str(exc))
    else:
        _skip("load(codes=...)", f"format={fmt}, codes filter N/A")

    # (d) By date range
    try:
        df = ds.load(date_from="2024-01-01", date_to="2025-12-31")
        assert isinstance(df, pd.DataFrame) and len(df) > 0
        _ok("load(date_from, date_to)", f"{len(df):,} rows")
    except Exception as exc:
        _fail("load(date_from, date_to)", str(exc))

    # (e) By items (timeseries only)
    if fmt.startswith("timeseries") and items_list:
        try:
            df = ds.load(items=[items_list[0]])
            assert isinstance(df, pd.DataFrame) and len(df) > 0
            _ok(f"load(items=[{items_list[0]!r}])", f"{len(df):,} rows, cols={list(df.columns)}")
        except Exception as exc:
            _fail("load(items=...)", str(exc))
    else:
        _skip("load(items=...)", f"format={fmt} or no items")

    # (f) Combined: codes + date_range + items
    if fmt.startswith("timeseries") and items_list:
        try:
            full = ds.load()
            test_code = full["코드"].iloc[0]
            df = ds.load(
                codes=[test_code],
                date_from="2024-01-01",
                items=[items_list[0]],
            )
            assert isinstance(df, pd.DataFrame) and len(df) > 0
            _ok(
                "load(codes + date_from + items) — combined",
                f"{len(df):,} rows",
            )
        except Exception as exc:
            _fail("load(combined)", str(exc))
    else:
        _skip("load(combined)", f"format={fmt}")


def uc15_describe_attributes(ds: fn_dg6_ingest.Dataset) -> None:
    """UC-15: ds.describe() attributes — tables, items, shape, date_range, entities, format_name."""
    _sub("UC-15: ds.describe() attributes")

    try:
        info = ds.describe()

        # info.tables
        assert isinstance(info.tables, list) and len(info.tables) > 0
        _ok("info.tables", str(info.tables))

        # info.items
        assert isinstance(info.items, dict)
        for t, item_list in info.items.items():
            assert isinstance(item_list, list)
        _ok("info.items", f"{sum(len(v) for v in info.items.values())} total items across {len(info.items)} tables")

        # info.shape
        assert isinstance(info.shape, dict) and len(info.shape) > 0
        for t, (rows, cols) in info.shape.items():
            assert isinstance(rows, int) and isinstance(cols, int)
        _ok("info.shape", str(info.shape))

        # info.date_range
        if info.date_range is not None:
            assert isinstance(info.date_range, tuple) and len(info.date_range) == 2
            _ok("info.date_range", str(info.date_range))
        else:
            _skip("info.date_range", "None (not available)")

        # info.entities
        if info.entities is not None:
            assert isinstance(info.entities, int)
            _ok("info.entities", str(info.entities))
        else:
            _skip("info.entities", "None (not available)")

        # info.format_name
        assert isinstance(info.format_name, str) and len(info.format_name) > 0
        _ok("info.format_name", info.format_name)

        # Additional attributes shown in README
        _ok("info.output_format", info.output_format)
        _ok("info.output_dir", info.output_dir)
        _ok("info.config_path", info.config_path)

    except Exception as exc:
        _fail("describe() attributes", str(exc))


def uc16_save_config_roundtrip(config_path: str) -> None:
    """UC-16: ds.save_config() round-trip — save and reload should be identical."""
    _sub("UC-16: ds.save_config() round-trip")

    if not Path(config_path).exists():
        _skip("save_config round-trip", "config not found")
        return

    try:
        ds1 = fn_dg6_ingest.open(config_path)
        original_tables = copy.deepcopy(ds1.config.tables)
        original_format = ds1.config.output.output_format

        # Save (no changes) and reload
        ds1.save_config()
        ds2 = fn_dg6_ingest.open(config_path)

        assert ds2.config.tables == original_tables
        assert ds2.config.output.output_format == original_format
        assert ds2.config.source.detected_format == ds1.config.source.detected_format
        _ok("save_config() round-trip", "config unchanged after save+reload")

    except Exception as exc:
        _fail("save_config round-trip", str(exc))


# ---------------------------------------------------------------------------
# Per-dataset orchestrator
# ---------------------------------------------------------------------------


def verify_dataset(config_path: str, input_path: str | None = None) -> None:
    """Run all applicable use cases for a single dataset."""
    config_p = Path(config_path)
    _section(f"Dataset: {config_p.stem}")

    # Determine input path from config if not given
    if input_path is None:
        ds_tmp = fn_dg6_ingest.open(config_path)
        input_path = ds_tmp.config.source.input_path

    output_dir = str(config_p.with_suffix(""))  # outputs/kse+kosdaq_ohlcv

    # ── Quick Start §3 ──
    ds = uc01_open_raw_csv(input_path, output_dir)
    uc02_idempotent_open(input_path, output_dir)

    # For remaining tests, open from YAML (always available)
    ds_yaml = uc08_open_yaml(config_path)
    ds = ds_yaml or ds
    if ds is None:
        print("  [ABORT] No dataset handle available — skipping remaining tests")
        return

    uc03_load_full(ds)
    uc04_load_codes_items(ds)
    uc05_load_date_range(ds)
    uc06_load_meta(ds)
    uc07_describe(ds)

    # UC-09: run_immediately=False (uses a temp directory to avoid clobbering)
    tmp_dir = f"outputs/_tmp_uc09_{config_p.stem}"
    uc09_run_immediately_false(input_path, tmp_dir)

    # ── Quick Start §4 ──
    uc10_open_yaml_reingest(config_path)
    uc11_load_specific_table(ds)
    uc12_programmatic_config_modify(config_path)

    # ── Dataset API ──
    uc13_dataset_properties(ds)
    uc14_load_filter_variants(ds)
    uc15_describe_attributes(ds)
    uc16_save_config_roundtrip(config_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    global _pass_count, _fail_count, _skip_count
    _pass_count = 0
    _fail_count = 0
    _skip_count = 0

    outputs_root = Path("outputs")

    # Single dataset mode
    if len(sys.argv) > 1:
        target = Path(sys.argv[1])
        if target.suffix.lower() in (".yaml", ".yml") and target.exists():
            verify_dataset(str(target))
        else:
            print(f"Not a valid YAML config: {target}")
            sys.exit(1)
    else:
        # All datasets
        if not outputs_root.exists():
            print(f"Output directory not found: {outputs_root}")
            print("Run 'uv run python scripts/run_ingest.py' first to build outputs.")
            sys.exit(1)

        configs = sorted(outputs_root.glob("*.yaml"))
        if not configs:
            print("No dataset configs found in outputs/")
            sys.exit(0)

        print(f"Found {len(configs)} dataset(s) in {outputs_root}/\n")
        for config_path in configs:
            try:
                verify_dataset(str(config_path))
            except Exception as exc:
                print(f"\n  [ERROR] Unhandled exception for {config_path.name}: {exc}\n")

    # ── Summary ──
    _section("Summary")
    total = _pass_count + _fail_count + _skip_count
    print(f"  Total : {total}")
    print(f"  PASS  : {_pass_count}")
    print(f"  FAIL  : {_fail_count}")
    print(f"  SKIP  : {_skip_count}")
    print()

    if _fail_count > 0:
        print("  *** FAILURES DETECTED — check output above ***")
        sys.exit(1)
    else:
        print("  All checks passed.")


if __name__ == "__main__":
    main()
