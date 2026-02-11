"""
Inspect exported data in outputs/.

For each output subdirectory, reads every Parquet/CSV file and prints:
  - File path and size on disk
  - Shape (rows x cols)
  - Column names and dtypes
  - Memory usage
  - First 30 rows as a sample

Also reads the corresponding YAML config and prints a summary.

Usage:
    uv run python scripts/inspect_outputs.py
    uv run python scripts/inspect_outputs.py outputs/kse+kosdaq_ohlcv   # single dataset
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import yaml


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


def inspect_config(config_path: Path) -> None:
    """Print a summary of the YAML config."""
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    print(f"\n  Config: {config_path.name}")
    print(f"    source        : {cfg.get('source', {}).get('input_path', '?')}")
    print(f"    format        : {cfg.get('source', {}).get('detected_format', '?')}")
    print(f"    output_format : {cfg.get('output', {}).get('output_format', '?')}")
    print(f"    normalize     : {cfg.get('output', {}).get('normalize_units', '?')}")
    print(f"    drop_empty    : {cfg.get('output', {}).get('drop_empty_entities', '?')}")

    tables = cfg.get("tables", {})
    print(f"    tables        : {len(tables)}")
    for name, items in tables.items():
        print(f"      {name}: {items}")

    meta = cfg.get("metadata", {})
    for key in ("frequency", "period_start", "period_end", "currency"):
        if meta.get(key):
            print(f"    {key:14s}: {meta[key]}")


def inspect_file(file_path: Path, sample_rows: int = 30) -> None:
    """Read a data file and print its properties and sample data."""
    # Read based on extension
    ext = file_path.suffix.lower()
    if ext == ".parquet":
        df = pd.read_parquet(file_path)
    elif ext == ".csv":
        df = pd.read_csv(file_path, encoding="utf-8-sig")
    else:
        print(f"  [skip] Unknown format: {file_path}")
        return

    print(f"\n  File: {file_path.name}  ({_file_size_str(file_path)})")
    print(f"  Shape: {df.shape[0]:,} rows x {df.shape[1]} cols")
    print(f"  Memory: {df.memory_usage(deep=True).sum() / 1e6:.1f} MB")

    # Column info
    print(f"\n  {'Column':<30s}  {'Dtype':<20s}  {'Non-null':>10s}  {'Null':>8s}")
    print(f"  {'-'*30}  {'-'*20}  {'-'*10}  {'-'*8}")
    for col in df.columns:
        non_null = df[col].notna().sum()
        null = df[col].isna().sum()
        print(f"  {col:<30s}  {str(df[col].dtype):<20s}  {non_null:>10,}  {null:>8,}")

    # Sample
    print(f"\n  Sample ({min(sample_rows, len(df))} rows):")
    with pd.option_context(
        "display.max_rows", sample_rows,
        "display.max_columns", 20,
        "display.width", 200,
        "display.max_colwidth", 30,
    ):
        sample = df.head(sample_rows).to_string(index=False)
        for line in sample.split("\n"):
            print(f"  {line}")


def inspect_dataset(output_dir: Path, config_path: Path | None, sample_rows: int = 30) -> None:
    """Inspect a single dataset (config + data files)."""
    _print_separator("=")
    print(f"Dataset: {output_dir.name}")
    _print_separator("=")

    # Config summary
    if config_path and config_path.exists():
        inspect_config(config_path)
    else:
        print("  (no config YAML found)")

    # Find data files
    data_files = sorted(output_dir.glob("*.parquet")) + sorted(output_dir.glob("*.csv"))
    if not data_files:
        print("  (no data files found)")
        return

    # Show _meta first, then data tables
    meta_files = [f for f in data_files if f.stem == "_meta"]
    table_files = [f for f in data_files if f.stem != "_meta"]

    for f in table_files + meta_files:
        inspect_file(f, sample_rows=sample_rows)

    print()


def main() -> None:
    outputs_root = Path("outputs")

    if not outputs_root.exists():
        print(f"Output directory not found: {outputs_root}")
        sys.exit(1)

    # If a specific path is given as argument, inspect only that
    if len(sys.argv) > 1:
        target = Path(sys.argv[1])
        if target.is_dir():
            config = target.parent / f"{target.name}.yaml"
            if not config.exists():
                config = None
            inspect_dataset(target, config)
        else:
            print(f"Not a directory: {target}")
            sys.exit(1)
        return

    # Otherwise, inspect all subdirectories
    subdirs = sorted(
        d for d in outputs_root.iterdir()
        if d.is_dir()
    )

    if not subdirs:
        print("No output datasets found.")
        sys.exit(0)

    print(f"Found {len(subdirs)} dataset(s) in {outputs_root}/\n")

    for subdir in subdirs:
        config_path = outputs_root / f"{subdir.name}.yaml"
        inspect_dataset(subdir, config_path)


if __name__ == "__main__":
    main()
