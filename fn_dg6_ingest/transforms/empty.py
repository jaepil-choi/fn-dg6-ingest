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

    An entity is "empty" if **every** value column is ``NaN`` for **every**
    row belonging to that entity.  This typically happens with delisted
    stocks or entities outside the queried date range.

    The check uses ``notna().any()`` per group -- if no cell in any value
    column is non-null, the entity is dropped.

    Args:
        df: Input DataFrame (after number parsing, so value columns are
            numeric and missing data is ``NaN``).
        entity_column: Column identifying the entity (default: ``코드``).
        value_columns: Columns to check for non-null values. If ``None``,
            uses all columns with a numeric dtype.

    Returns:
        DropResult with cleaned DataFrame and entity counts.
    """
    if entity_column not in df.columns:
        # Nothing to drop if the entity column is absent
        return DropResult(df=df, entities_total=0, entities_dropped=0)

    if value_columns is None:
        value_columns = list(df.select_dtypes(include="number").columns)

    if not value_columns:
        # No numeric columns to check -- return as-is
        entities_total = df[entity_column].nunique()
        return DropResult(df=df, entities_total=entities_total, entities_dropped=0)

    entities_total = df[entity_column].nunique()

    # For each entity, check if there is at least one non-null value
    # across all value columns. ``any(axis=1)`` checks row-wise, then
    # ``any()`` per group checks across all rows for that entity.
    has_data = (
        df.groupby(entity_column)[value_columns]
        .apply(lambda g: g.notna().any().any())
    )
    entities_with_data = set(has_data[has_data].index)
    entities_dropped = entities_total - len(entities_with_data)

    if entities_dropped > 0:
        df_clean = df[df[entity_column].isin(entities_with_data)].reset_index(drop=True)
    else:
        df_clean = df

    return DropResult(
        df=df_clean,
        entities_total=entities_total,
        entities_dropped=entities_dropped,
    )
