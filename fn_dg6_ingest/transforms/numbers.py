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

    Applies three cleaning steps to every non-key column:
    1. Strip leading/trailing whitespace.
    2. Remove comma thousand separators (Korean convention).
    3. Coerce to numeric via ``pd.to_numeric(errors='coerce')``.

    Empty strings are treated as missing (``NaN``) after coercion.
    Key columns are left as-is (string dtype).

    Args:
        df: Input DataFrame with string values from CSV.
        key_columns: Column names to skip (these are string identifiers, not numbers).

    Returns:
        DataFrame with numeric columns coerced to appropriate dtypes.
    """
    df = df.copy()
    key_set = set(key_columns)
    value_cols = [c for c in df.columns if c not in key_set]

    for col in value_cols:
        series = df[col]
        # Step 1 & 2: strip whitespace and remove commas
        cleaned = series.str.strip().str.replace(",", "", regex=False)
        # Step 3: coerce to numeric (empty strings and non-numeric become NaN)
        df[col] = pd.to_numeric(cleaned, errors="coerce")

    return df
