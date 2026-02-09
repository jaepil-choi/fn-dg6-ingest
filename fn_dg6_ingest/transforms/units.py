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

    Args:
        df: DataFrame with columns that may have unit suffixes.

    Returns:
        Tuple of (transformed DataFrame, unit_info dict).
        unit_info maps original_column_name -> (original_unit, multiplier).
    """
    raise NotImplementedError("normalize_units() not yet implemented")
