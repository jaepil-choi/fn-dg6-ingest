"""
Inspect exported datasets using the high-level Dataset API.

For each dataset (identified by its YAML config), prints:
  - Config summary via ds.describe()
  - Per-table shape, columns, dtypes, null counts, and sample rows
  - _meta lineage table summary

Usage:
    uv run python scripts/inspect_outputs.py
    uv run python scripts/inspect_outputs.py outputs/kse+kosdaq_ohlcv.yaml   # single dataset
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

import fn_dg6_ingest


def _file_size_str(path: Path) -> str:
    """Human-readable file size."""
    size = path.stat().st_size
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def _print_separator(char: str = "=", width: int = 80) -> None:
    print(char * width)


def _print_df_details(df: pd.DataFrame, label: str, file_path: Path | None = None, sample_rows: int = 30) -> None:
    """Print detailed info about a DataFrame: columns, dtypes, nulls, sample."""
    size_info = f"  ({_file_size_str(file_path)})" if file_path and file_path.exists() else ""
    print(f"\n  Table: {label}{size_info}")
    print(f"  Shape: {df.shape[0]:,} rows x {df.shape[1]} cols")
    print(f"  Memory: {df.memory_usage(deep=True).sum() / 1e6:.1f} MB")

    # Column info
    print(f"\n  {'Column':<30s}  {'Dtype':<20s}  {'Non-null':>10s}  {'Null':>8s}")
    print(f"  {'-'*30}  {'-'*20}  {'-'*10}  {'-'*8}")
    for col in df.columns:
        non_null = df[col].notna().sum()
        null_count = df[col].isna().sum()
        print(f"  {col:<30s}  {str(df[col].dtype):<20s}  {non_null:>10,}  {null_count:>8,}")

    # Sample
    n = min(sample_rows, len(df))
    print(f"\n  Sample ({n} rows):")
    with pd.option_context(
        "display.max_rows", sample_rows,
        "display.max_columns", 20,
        "display.width", 200,
        "display.max_colwidth", 30,
    ):
        sample = df.head(sample_rows).to_string(index=False)
        for line in sample.split("\n"):
            print(f"  {line}")


def inspect_dataset(config_path: Path, sample_rows: int = 30) -> None:
    """Open a dataset from its config and inspect all contents."""
    ds = fn_dg6_ingest.open(str(config_path))
    info = ds.describe()

    _print_separator("=")
    print(f"Dataset: {config_path.stem}")
    _print_separator("=")

    # -- Config summary from describe() --
    print(f"\n  Config       : {info.config_path}")
    print(f"  Format       : {info.format_name}")
    print(f"  Output format: {info.output_format}")
    print(f"  Output dir   : {info.output_dir}")
    print(f"  Tables       : {info.tables}")
    if info.date_range:
        print(f"  Date range   : {info.date_range[0]} .. {info.date_range[1]}")
    if info.entities is not None:
        print(f"  Entities     : {info.entities:,}")

    # Per-table shape
    for t_name, (rows, cols) in info.shape.items():
        items_str = ", ".join(info.items.get(t_name, []))
        print(f"  Table '{t_name}': {rows:,} rows x {cols} cols  [{items_str}]")

    # -- Config details (settings that aren't in describe) --
    cfg = ds.config
    print(f"\n  normalize_units      : {cfg.output.normalize_units}")
    print(f"  drop_empty_entities  : {cfg.output.drop_empty_entities}")
    if cfg.metadata.frequency:
        print(f"  frequency            : {cfg.metadata.frequency}")
    if cfg.metadata.currency:
        print(f"  currency             : {cfg.metadata.currency}")
    if cfg.metadata.period_start:
        print(f"  period_start         : {cfg.metadata.period_start}")
    if cfg.metadata.period_end:
        print(f"  period_end           : {cfg.metadata.period_end}")

    # -- Data tables --
    fmt = info.output_format
    out_dir = Path(info.output_dir)

    for t_name in info.tables:
        file_path = out_dir / f"{t_name}.{fmt}"
        try:
            df = ds.load(table=t_name)
            _print_df_details(df, t_name, file_path, sample_rows)
        except FileNotFoundError:
            print(f"\n  Table '{t_name}': FILE NOT FOUND ({file_path})")

    # -- _meta table --
    try:
        meta = ds.load_meta()
        meta_path = out_dir / f"_meta.{fmt}"
        _print_df_details(meta, "_meta", meta_path, sample_rows)
    except FileNotFoundError:
        print("\n  _meta: FILE NOT FOUND")

    print()


def main() -> None:
    outputs_root = Path("outputs")

    # If a specific config path is given as argument
    if len(sys.argv) > 1:
        target = Path(sys.argv[1])
        if target.suffix.lower() in (".yaml", ".yml") and target.exists():
            inspect_dataset(target)
        elif target.is_dir():
            # Legacy usage: directory path -> find sibling YAML
            config = target.parent / f"{target.name}.yaml"
            if config.exists():
                inspect_dataset(config)
            else:
                print(f"No config YAML found for directory: {target}")
                print(f"  Expected: {config}")
                sys.exit(1)
        else:
            print(f"Not a valid config path or directory: {target}")
            sys.exit(1)
        return

    # Otherwise, find all YAML configs in outputs/
    if not outputs_root.exists():
        print(f"Output directory not found: {outputs_root}")
        sys.exit(1)

    configs = sorted(outputs_root.glob("*.yaml"))
    if not configs:
        print("No dataset configs found in outputs/")
        sys.exit(0)

    print(f"Found {len(configs)} dataset(s) in {outputs_root}/\n")

    for config_path in configs:
        try:
            inspect_dataset(config_path)
        except Exception as exc:
            print(f"\n  ERROR inspecting {config_path.name}: {exc}\n")


if __name__ == "__main__":
    main()
