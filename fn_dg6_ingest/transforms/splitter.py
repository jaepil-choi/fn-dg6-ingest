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

import pandas as pd


def split_tables(
    df: pd.DataFrame,
    table_groups: dict[str, list[str]],
    key_columns: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """Split a DataFrame into multiple tables by 아이템명 groups.

    Args:
        df: Input DataFrame with 아이템명 values as columns.
        table_groups: Mapping of table_name -> list of 아이템명 columns.
        key_columns: Columns to always include in every table.
            Defaults to ["코드", "코드명", "date"].

    Returns:
        Dict mapping table_name -> DataFrame subset.
    """
    raise NotImplementedError("split_tables() not yet implemented")
