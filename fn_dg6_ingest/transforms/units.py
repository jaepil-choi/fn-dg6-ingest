"""
Unit normalization transform for fn-dg6-ingest.

Detects unit suffixes in column/item names and scales monetary values
to base unit (원). After scaling, renames the column suffix.

Supported unit suffixes and their multipliers:
  (원)    -> 1          (base, no scaling)
  (천원)   -> 1,000
  (백만원)  -> 1,000,000
  (억원)   -> 100,000,000
  (십억원)  -> 1,000,000,000

Non-monetary suffixes are left untouched:
  (주), (%), (계약수), etc.

Why this matters:
  DataGuide 6 exports may use different units for different items in the
  same file (e.g., 매출액(억원) alongside 주가(원)). Normalizing to a
  single base unit prevents unit mismatch bugs in downstream analysis.
"""

from __future__ import annotations

import re

import pandas as pd

# Mapping from unit suffix to multiplier
UNIT_MULTIPLIERS: dict[str, int] = {
    "원": 1,
    "천원": 1_000,
    "백만원": 1_000_000,
    "억원": 100_000_000,
    "십억원": 1_000_000_000,
}

# Regex to extract the unit suffix from a column name like "매출액(천원)"
_UNIT_SUFFIX_RE = re.compile(r"\(([^)]+)\)$")


def detect_unit(column_name: str) -> tuple[str | None, int]:
    """Detect the unit suffix in a column name.

    Args:
        column_name: e.g., "매출액(천원)" or "거래량(주)"

    Returns:
        Tuple of (unit_suffix, multiplier). If no monetary unit is found,
        returns (None, 1).
    """
    match = _UNIT_SUFFIX_RE.search(column_name)
    if not match:
        return None, 1
    unit = match.group(1)
    multiplier = UNIT_MULTIPLIERS.get(unit, 1)
    if multiplier == 1 and unit != "원":
        # Non-monetary unit (주, %, etc.) -- don't scale
        return None, 1
    return unit, multiplier


def normalize_column_name(column_name: str, original_unit: str) -> str:
    """Rename a column suffix from original_unit to 원.

    Example: "매출액(천원)" -> "매출액(원)"
    """
    return column_name.replace(f"({original_unit})", "(원)")


def normalize_units(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, tuple[str, int]]]:
    """Scale monetary columns to base unit (원) and rename suffixes.

    For each column whose name contains a monetary unit suffix (e.g.,
    ``매출액(억원)``), this function:

    1. Multiplies all numeric values in that column by the unit multiplier.
    2. Renames the column suffix to ``(원)`` (e.g., ``매출액(억원)`` -> ``매출액(원)``).

    Columns already in base unit ``(원)`` are recorded in ``unit_info`` with
    multiplier 1 but are *not* modified. Non-monetary suffixes like ``(주)``
    and ``(%)`` are ignored entirely.

    Args:
        df: DataFrame with columns that may have unit suffixes.
            Numeric columns should already be coerced (run ``parse_numbers`` first).

    Returns:
        Tuple of (transformed DataFrame, unit_info dict).
        unit_info maps original_column_name -> (original_unit, multiplier)
        for every column where a monetary unit was detected.
    """
    df = df.copy()
    unit_info: dict[str, tuple[str, int]] = {}
    rename_map: dict[str, str] = {}

    for col in df.columns:
        unit, multiplier = detect_unit(col)
        if unit is None:
            # No monetary unit detected -- skip
            continue

        # Record the original unit info (used by _meta table builder)
        unit_info[col] = (unit, multiplier)

        if multiplier > 1:
            # Scale the values: e.g., 100 (억원) -> 10,000,000,000 (원)
            df[col] = df[col] * multiplier
            # Schedule a rename: 매출액(억원) -> 매출액(원)
            new_name = normalize_column_name(col, unit)
            rename_map[col] = new_name

    # Apply all renames at once to avoid intermediate collisions
    if rename_map:
        df = df.rename(columns=rename_map)

    return df, unit_info
