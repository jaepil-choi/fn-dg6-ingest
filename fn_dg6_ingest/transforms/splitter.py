"""
Table splitter transform for fn-dg6-ingest.

Splits a single DataFrame into multiple DataFrames based on 아이템명
groupings defined in the config's `tables` section.

Each output table includes:
- The key columns: 코드, 코드명, date (always included).
- Only the 아이템명 columns assigned to that group.

If no groupings are specified (or a single 'default' group), the
DataFrame is returned as-is in a single-entry dict.

Why splitting:
  Users often want OHLCV prices in one table and volume/turnover in
  another, or fundamentals separate from price data. Splitting at
  the ingestion layer avoids redundant post-processing.
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def split_tables(
    df: pd.DataFrame,
    table_groups: dict[str, list[str]],
    key_columns: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """Split a DataFrame into multiple tables by 아이템명 groups.

    Each output table contains the **key columns** (entity identifiers +
    date) plus only the 아이템명 value columns assigned to that group.

    If ``table_groups`` has a single entry (typical for the auto-generated
    ``default`` group), this is effectively a no-op that wraps the
    DataFrame in a single-entry dict.

    Columns listed in a group but absent from the DataFrame are silently
    skipped (they may have been renamed by unit normalization). A warning
    is logged so the user can investigate if needed.

    Args:
        df: Input DataFrame with 아이템명 values as columns.
        table_groups: Mapping of table_name -> list of 아이템명 columns.
        key_columns: Columns to always include in every table.
            Defaults to ``["코드", "코드명", "date"]``.

    Returns:
        Dict mapping table_name -> DataFrame subset.
    """
    if key_columns is None:
        key_columns = [c for c in ["코드", "코드명", "date"] if c in df.columns]

    result: dict[str, pd.DataFrame] = {}

    for table_name, item_names in table_groups.items():
        # Filter to only columns that actually exist in the DataFrame.
        # After unit normalization, suffixes may have changed (e.g.,
        # 매출액(억원) -> 매출액(원)), so we warn on mismatches.
        present = [c for c in item_names if c in df.columns]
        missing = [c for c in item_names if c not in df.columns]
        if missing:
            logger.warning(
                "Table '%s': columns not found in DataFrame (possibly "
                "renamed by unit normalization): %s",
                table_name,
                missing,
            )

        if not present:
            logger.warning(
                "Table '%s': no matching columns found, skipping.", table_name
            )
            continue

        cols = key_columns + present
        result[table_name] = df[cols].copy()

    return result
