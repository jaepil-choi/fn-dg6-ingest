"""
Empty entity dropper transform for fn-dg6-ingest.

Removes entities (identified by 코드) that have no non-null values
across all dates for the data columns in a given table.

Why:
  DataGuide 6 exports often include entities that were historically
  in an index but have been delisted or have no data for the queried
  period. Including these all-null rows wastes space and can cause
  unexpected NaN propagation in downstream analysis.

Returns:
  The cleaned DataFrame plus counts of total/dropped entities
  (used by the _meta table builder).
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class DropResult:
    """Result of the empty-entity dropping step."""
    df: pd.DataFrame
    entities_total: int
    entities_dropped: int


def drop_empty_entities(
    df: pd.DataFrame,
    entity_column: str = "코드",
    value_columns: list[str] | None = None,
) -> DropResult:
    """Drop entities with all-null data.

    Args:
        df: Input DataFrame in long form.
        entity_column: Column identifying the entity (default: 코드).
        value_columns: Columns to check for non-null values. If None,
            uses all numeric columns.

    Returns:
        DropResult with cleaned DataFrame and entity counts.
    """
    raise NotImplementedError("drop_empty_entities() not yet implemented")
