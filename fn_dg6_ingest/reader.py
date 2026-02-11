"""
Data reading logic for fn-dg6-ingest.

Provides functions to read output tables (Parquet / CSV) with optional
filtering, load the ``_meta`` lineage table, and inspect table metadata
(schema + row count) without scanning data.

This module is the read-side counterpart to ``export.py``. It is
designed to be called by ``Dataset.load()`` / ``Dataset.describe()``
but has **no dependency** on the ``Dataset`` class itself -- it works
purely with paths and format strings, keeping the read layer decoupled.

Filtering strategy:
- **Parquet**: Uses PyArrow's native column pruning (``columns`` param)
  and predicate pushdown (``filters`` param) so only the required
  data is read from disk.  Since ``date`` is stored as ISO-format
  strings (``YYYY-MM-DD``), lexicographic string comparison is
  semantically correct for range filtering.
- **CSV**: Reads the full file (with optional ``usecols`` for column
  pruning) then applies pandas-level row filtering. Same interface,
  lower performance on large files.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

_DEFAULT_KEY_COLUMNS = ["코드", "코드명", "date"]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def read_table(
    output_dir: str | Path,
    table_name: str,
    output_format: str,
    *,
    codes: list[str] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    items: list[str] | None = None,
    key_columns: list[str] | None = None,
) -> pd.DataFrame:
    """Read an output table with optional filtering.

    Args:
        output_dir: Directory containing the output files.
        table_name: Name of the table (e.g., ``"default"``, ``"ohlcv"``).
        output_format: ``"parquet"`` or ``"csv"``.
        codes: Optional list of entity codes to filter on (e.g.,
            ``["A005930"]``).  Filters on the first key column that
            looks like a code column (contains ``"코드"`` but not
            ``"코드명"``), falling back to the first key column.
        date_from: Optional inclusive lower bound for date filtering
            (ISO format, e.g., ``"2024-01-01"``).
        date_to: Optional inclusive upper bound for date filtering.
        items: Optional list of value column names to select.  Key
            columns are always included regardless of this parameter.
        key_columns: Column names that are entity identifiers or dates.
            Defaults to ``["코드", "코드명", "date"]``.

    Returns:
        Filtered ``pandas.DataFrame``.

    Raises:
        FileNotFoundError: If the table file does not exist.
        ValueError: If *output_format* is unsupported.
    """
    file_path = _resolve_table_path(output_dir, table_name, output_format)
    if key_columns is None:
        key_columns = _DEFAULT_KEY_COLUMNS

    if output_format == "parquet":
        return _read_parquet(
            file_path,
            key_columns=key_columns,
            codes=codes,
            date_from=date_from,
            date_to=date_to,
            items=items,
        )
    elif output_format == "csv":
        return _read_csv(
            file_path,
            key_columns=key_columns,
            codes=codes,
            date_from=date_from,
            date_to=date_to,
            items=items,
        )
    else:
        raise ValueError(
            f"Unsupported output format: '{output_format}'. "
            "Supported formats: ['csv', 'parquet']"
        )


def read_meta(
    output_dir: str | Path,
    output_format: str,
) -> pd.DataFrame:
    """Read the ``_meta`` lineage table.

    Args:
        output_dir: Directory containing the output files.
        output_format: ``"parquet"`` or ``"csv"``.

    Returns:
        The ``_meta`` DataFrame.

    Raises:
        FileNotFoundError: If the ``_meta`` file does not exist.
    """
    file_path = _resolve_table_path(output_dir, "_meta", output_format)
    if output_format == "parquet":
        return pd.read_parquet(file_path)
    else:
        return pd.read_csv(file_path, encoding="utf-8-sig")


def read_table_info(
    output_dir: str | Path,
    table_name: str,
    output_format: str,
) -> dict[str, Any]:
    """Read table metadata (schema + shape) without scanning data.

    For Parquet files, this reads only the file footer (``read_metadata``
    and ``read_schema``), which is a zero-data-scan operation.

    For CSV files, reads ``nrows=0`` for the schema and counts lines
    for the row count.

    Args:
        output_dir: Directory containing the output files.
        table_name: Name of the table.
        output_format: ``"parquet"`` or ``"csv"``.

    Returns:
        Dict with keys:
        - ``num_rows`` (int): Number of data rows.
        - ``num_columns`` (int): Number of columns.
        - ``columns`` (list[str]): Column names.
        - ``dtypes`` (dict[str, str]): Column name -> dtype string.

    Raises:
        FileNotFoundError: If the table file does not exist.
    """
    file_path = _resolve_table_path(output_dir, table_name, output_format)

    if output_format == "parquet":
        return _info_parquet(file_path)
    else:
        return _info_csv(file_path)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _resolve_table_path(
    output_dir: str | Path,
    table_name: str,
    output_format: str,
) -> Path:
    """Build and validate the file path for a table."""
    path = Path(output_dir) / f"{table_name}.{output_format}"
    if not path.exists():
        raise FileNotFoundError(
            f"Table file not found: {path}. "
            f"Has the pipeline been run? Check output_dir='{output_dir}'."
        )
    return path


def _identify_code_column(key_columns: list[str]) -> str | None:
    """Pick the column to filter codes on from the key columns.

    Heuristic: prefer a column whose name contains '코드' but NOT
    '코드명' (name column).  Fall back to the first key column if
    none matches.
    """
    for col in key_columns:
        if "코드" in col and "코드명" not in col:
            return col
    return key_columns[0] if key_columns else None


def _identify_date_column(key_columns: list[str]) -> str | None:
    """Pick the date column from the key columns.

    Heuristic: prefer 'date', then any column containing '날짜'.
    """
    if "date" in key_columns:
        return "date"
    for col in key_columns:
        if "날짜" in col:
            return col
    return None


# -- Parquet reading --------------------------------------------------------

def _read_parquet(
    path: Path,
    *,
    key_columns: list[str],
    codes: list[str] | None,
    date_from: str | None,
    date_to: str | None,
    items: list[str] | None,
) -> pd.DataFrame:
    """Read a Parquet table with PyArrow-native filtering."""
    # -- Column pruning --
    columns: list[str] | None = None
    if items is not None:
        # Read the schema to find which key columns actually exist
        schema = pq.read_schema(path)
        existing_cols = set(schema.names)
        selected_keys = [c for c in key_columns if c in existing_cols]
        columns = selected_keys + [i for i in items if i not in selected_keys]

    # -- Row filters (predicate pushdown) --
    filters: list[tuple] | None = _build_parquet_filters(
        key_columns, codes, date_from, date_to
    )

    logger.debug(
        "Reading Parquet %s (columns=%s, filters=%s)",
        path.name, columns, filters,
    )

    table = pq.read_table(
        path,
        columns=columns,
        filters=filters or None,
    )
    return table.to_pandas()


def _build_parquet_filters(
    key_columns: list[str],
    codes: list[str] | None,
    date_from: str | None,
    date_to: str | None,
) -> list[tuple]:
    """Build PyArrow filter tuples for predicate pushdown."""
    filters: list[tuple] = []

    if codes is not None:
        code_col = _identify_code_column(key_columns)
        if code_col:
            filters.append((code_col, "in", codes))

    if date_from is not None or date_to is not None:
        date_col = _identify_date_column(key_columns)
        if date_col:
            if date_from is not None:
                filters.append((date_col, ">=", date_from))
            if date_to is not None:
                filters.append((date_col, "<=", date_to))

    return filters


# -- CSV reading ------------------------------------------------------------

def _read_csv(
    path: Path,
    *,
    key_columns: list[str],
    codes: list[str] | None,
    date_from: str | None,
    date_to: str | None,
    items: list[str] | None,
) -> pd.DataFrame:
    """Read a CSV table with post-load pandas filtering."""
    # -- Column pruning via usecols --
    usecols: list[str] | None = None
    if items is not None:
        # Peek at the header to find which key columns exist
        header_df = pd.read_csv(path, nrows=0, encoding="utf-8-sig")
        existing_cols = set(header_df.columns)
        selected_keys = [c for c in key_columns if c in existing_cols]
        usecols = selected_keys + [i for i in items if i not in selected_keys]

    df = pd.read_csv(path, encoding="utf-8-sig", usecols=usecols)

    # -- Post-load row filtering --
    if codes is not None:
        code_col = _identify_code_column(
            [c for c in key_columns if c in df.columns]
        )
        if code_col and code_col in df.columns:
            df = df[df[code_col].isin(codes)]

    date_col = _identify_date_column(
        [c for c in key_columns if c in df.columns]
    )
    if date_col and date_col in df.columns:
        if date_from is not None:
            df = df[df[date_col] >= date_from]
        if date_to is not None:
            df = df[df[date_col] <= date_to]

    return df.reset_index(drop=True)


# -- Table info -------------------------------------------------------------

def _info_parquet(path: Path) -> dict[str, Any]:
    """Extract schema + row count from Parquet footer (zero data scan)."""
    metadata = pq.read_metadata(path)
    schema = pq.read_schema(path)

    columns = schema.names
    dtypes = {name: str(schema.field(name).type) for name in columns}

    return {
        "num_rows": metadata.num_rows,
        "num_columns": len(columns),
        "columns": columns,
        "dtypes": dtypes,
    }


def _info_csv(path: Path) -> dict[str, Any]:
    """Extract schema from CSV header + count lines for row count."""
    header_df = pd.read_csv(path, nrows=0, encoding="utf-8-sig")
    columns = list(header_df.columns)
    dtypes = {col: str(header_df[col].dtype) for col in columns}

    # Count lines (subtract 1 for header)
    with open(path, "r", encoding="utf-8-sig") as f:
        num_rows = sum(1 for _ in f) - 1

    return {
        "num_rows": max(num_rows, 0),
        "num_columns": len(columns),
        "columns": columns,
        "dtypes": dtypes,
    }
