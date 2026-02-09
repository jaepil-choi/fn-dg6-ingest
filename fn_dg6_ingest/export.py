"""
Exporter for fn-dg6-ingest.

Writes the transformed data tables and the _meta table to the output
directory in the configured format (CSV or Parquet).

Output file naming convention:
  {table_name}.{format}  -- e.g., "ohlcv.parquet", "volume.csv"
  "_meta.{format}"       -- always written alongside data tables.

Why Parquet is the default:
- Preserves column dtypes (no re-parsing on load).
- Columnar compression reduces file size significantly.
- Fast reads for analytical workloads (column pruning, predicate pushdown).

CSV is supported as a fallback for interoperability with tools that
don't support Parquet (e.g., Excel, legacy systems).
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pandas as pd


def export_tables(
    tables: dict[str, pd.DataFrame],
    meta_df: pd.DataFrame,
    output_dir: str | Path,
    output_format: Literal["csv", "parquet"] = "parquet",
) -> list[str]:
    """Write data tables and _meta table to disk.

    Args:
        tables: Dict mapping table_name -> DataFrame.
        meta_df: The _meta DataFrame.
        output_dir: Directory to write files into (created if needed).
        output_format: "csv" or "parquet".

    Returns:
        List of file paths that were written.

    Raises:
        ExportError: If writing fails.
    """
    raise NotImplementedError("export_tables() not yet implemented")
