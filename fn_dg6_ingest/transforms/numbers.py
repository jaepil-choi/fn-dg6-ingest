"""
Number parsing transform for fn-dg6-ingest.

Handles the conversion of Korean-formatted numeric strings to proper
numeric types. DataGuide 6 exports use:
- Comma as thousand separator (e.g., "25,200", "257,149,317,000")
- Trailing whitespace (e.g., "0.62 ")
- Empty strings for missing values

This transform:
1. Strips whitespace from all cells.
2. Removes commas from numeric-looking strings.
3. Coerces columns to numeric dtype where possible (pd.to_numeric with errors='coerce').
4. Preserves string columns (코드, 코드명, etc.) untouched.
"""

from __future__ import annotations

import pandas as pd


def parse_numbers(df: pd.DataFrame, key_columns: list[str]) -> pd.DataFrame:
    """Parse numeric strings in a DataFrame.

    Args:
        df: Input DataFrame with string values from CSV.
        key_columns: Column names to skip (these are string identifiers, not numbers).

    Returns:
        DataFrame with numeric columns coerced to appropriate dtypes.
    """
    raise NotImplementedError("parse_numbers() not yet implemented")
