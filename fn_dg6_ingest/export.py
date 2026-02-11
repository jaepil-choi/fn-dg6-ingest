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

import logging
from pathlib import Path
from typing import Literal

import pandas as pd

from fn_dg6_ingest.exceptions import ExportError

logger = logging.getLogger(__name__)

_SUPPORTED_FORMATS = {"csv", "parquet"}


def _write_dataframe(
    df: pd.DataFrame,
    path: Path,
    output_format: str,
) -> None:
    """Write a single DataFrame to disk in the specified format.

    Args:
        df: The DataFrame to write.
        path: Full file path (including extension).
        output_format: "csv" or "parquet".

    Raises:
        ExportError: If writing fails for any reason.
    """
    try:
        if output_format == "csv":
            df.to_csv(path, index=False, encoding="utf-8-sig")
        else:  # parquet
            df.to_parquet(path, index=False, engine="pyarrow")
    except Exception as exc:
        raise ExportError(
            f"Failed to write {path.name} as {output_format}: {exc}"
        ) from exc


def export_tables(
    tables: dict[str, pd.DataFrame],
    meta_df: pd.DataFrame,
    output_dir: str | Path,
    output_format: Literal["csv", "parquet"] = "parquet",
) -> list[str]:
    """Write data tables and _meta table to disk.

    File naming:
      - Data tables: ``{table_name}.{format}``
      - Meta table: ``_meta.{format}``

    The output directory is created recursively if it does not exist.
    CSV files are written with ``utf-8-sig`` encoding (BOM) so that
    Korean characters display correctly when opened in Excel.

    Args:
        tables: Dict mapping table_name -> DataFrame.
        meta_df: The _meta DataFrame.
        output_dir: Directory to write files into (created if needed).
        output_format: "csv" or "parquet".

    Returns:
        List of file paths (as strings) that were written, in the order
        data tables first, then ``_meta``.

    Raises:
        ExportError: If *output_format* is unsupported, or if any write fails.
    """
    if output_format not in _SUPPORTED_FORMATS:
        raise ExportError(
            f"Unsupported output format: '{output_format}'. "
            f"Supported formats: {sorted(_SUPPORTED_FORMATS)}"
        )

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    written: list[str] = []

    # -- Write data tables ---------------------------------------------------
    for table_name, df in tables.items():
        file_path = out / f"{table_name}.{output_format}"
        _write_dataframe(df, file_path, output_format)
        written.append(str(file_path))
        logger.info(
            "Exported table '%s' -> %s (%d rows, %d cols)",
            table_name,
            file_path.name,
            len(df),
            len(df.columns),
        )

    # -- Write _meta table ---------------------------------------------------
    meta_path = out / f"_meta.{output_format}"
    _write_dataframe(meta_df, meta_path, output_format)
    written.append(str(meta_path))
    logger.info(
        "Exported _meta -> %s (%d rows)",
        meta_path.name,
        len(meta_df),
    )

    return written
