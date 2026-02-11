"""
Transform pipeline orchestrator for fn-dg6-ingest.

Runs a configurable sequence of transform steps on the parsed DataFrame.
The default pipeline order is:

1. **NumberParser**: Strip commas and whitespace, coerce to numeric types.
2. **UnitNormalizer**: Scale monetary columns to base unit (원), rename suffixes.
3. **EmptyEntityDropper**: Remove entities with all-null data.
4. **TableSplitter**: Split into multiple DataFrames based on 아이템명 groupings.

The pipeline receives the full ``IngestConfig`` so each step can check
relevant flags (e.g., ``normalize_units``, ``drop_empty_entities``).

It also accepts ``key_columns`` to distinguish entity/date columns from
value columns. This avoids fragile inference and keeps the contract explicit.

Returns a ``PipelineResult`` with the split tables, unit_info, and
entity statistics (all needed by the ``_meta`` table builder).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import pandas as pd

from fn_dg6_ingest.config import IngestConfig
from fn_dg6_ingest.transforms.empty import DropResult, drop_empty_entities
from fn_dg6_ingest.transforms.numbers import parse_numbers
from fn_dg6_ingest.transforms.splitter import split_tables
from fn_dg6_ingest.transforms.units import normalize_units

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Output of the transform pipeline.

    Carries everything downstream consumers need: the split tables,
    unit normalization metadata (for the _meta table), and entity
    drop statistics.

    Attributes:
        tables: Dict mapping table_name -> cleaned DataFrame.
        unit_info: Maps original_column_name -> (original_unit, multiplier).
            Empty if ``normalize_units`` was disabled.
        drop_result: Entity drop statistics. ``None`` if
            ``drop_empty_entities`` was disabled.
    """

    tables: dict[str, pd.DataFrame] = field(default_factory=dict)
    unit_info: dict[str, tuple[str, int]] = field(default_factory=dict)
    drop_result: DropResult | None = None


class TransformPipeline:
    """Orchestrates the sequence of data transforms.

    The pipeline is **stateless** -- each call to ``run()`` processes a
    fresh DataFrame independently. Configuration flags in ``OutputConfig``
    control which steps are executed:

    - ``normalize_units``: When ``False``, the unit normalization step is
      skipped entirely (values and column names are left as-is).
    - ``drop_empty_entities``: When ``False``, no entities are dropped.

    Number parsing and table splitting always run.
    """

    def __init__(self, config: IngestConfig) -> None:
        self.config = config

    def run(
        self,
        df: pd.DataFrame,
        key_columns: list[str] | None = None,
    ) -> PipelineResult:
        """Run all transforms and return split tables.

        Args:
            df: The parsed DataFrame (from a parser's ``ParseResult.df``).
                All cells are still strings at this point.
            key_columns: Columns that are entity identifiers / dates and
                should **not** be treated as numeric values. Defaults to
                ``["코드", "코드명", "date"]`` (timeseries convention).

        Returns:
            ``PipelineResult`` with tables, unit_info, and drop statistics.
        """
        if key_columns is None:
            key_columns = [c for c in ["코드", "코드명", "date"] if c in df.columns]

        # -- Step 1: Number parsing (always runs) -------------------------
        logger.info("Step 1/4: Parsing numbers (%d cols)", len(df.columns) - len(key_columns))
        df = parse_numbers(df, key_columns=key_columns)

        # -- Step 2: Unit normalization (configurable) --------------------
        unit_info: dict[str, tuple[str, int]] = {}
        if self.config.output.normalize_units:
            logger.info("Step 2/4: Normalizing units")
            df, unit_info = normalize_units(df)
        else:
            logger.info("Step 2/4: Unit normalization SKIPPED (disabled in config)")

        # -- Step 3: Empty entity drop (configurable) ---------------------
        drop_result: DropResult | None = None
        if self.config.output.drop_empty_entities:
            logger.info("Step 3/4: Dropping empty entities")
            drop_result = drop_empty_entities(df, entity_column="코드")
            df = drop_result.df
            logger.info(
                "  Entities: %d total, %d dropped",
                drop_result.entities_total,
                drop_result.entities_dropped,
            )
        else:
            logger.info("Step 3/4: Empty entity drop SKIPPED (disabled in config)")

        # -- Step 4: Table splitting (always runs) ------------------------
        # After unit normalization, column names may have changed (e.g.,
        # 매출액(억원) -> 매출액(원)). We need to update table_groups to
        # match the *current* column names.
        table_groups = self._resolve_table_groups(unit_info)
        logger.info("Step 4/4: Splitting into %d table(s)", len(table_groups))
        tables = split_tables(df, table_groups=table_groups, key_columns=key_columns)

        return PipelineResult(
            tables=tables,
            unit_info=unit_info,
            drop_result=drop_result,
        )

    def _resolve_table_groups(
        self, unit_info: dict[str, tuple[str, int]]
    ) -> dict[str, list[str]]:
        """Adjust table group item names after unit normalization.

        If a column was renamed (e.g., ``매출액(억원)`` -> ``매출액(원)``),
        we update the corresponding entry in the table groups so that the
        splitter finds the correct column name.

        Args:
            unit_info: The unit_info dict from ``normalize_units()``.

        Returns:
            Updated table_groups dict with post-normalization column names.
        """
        if not unit_info:
            return dict(self.config.tables)

        # Build a rename map: original_name -> new_name
        from fn_dg6_ingest.transforms.units import normalize_column_name

        rename_map: dict[str, str] = {}
        for original_col, (unit, multiplier) in unit_info.items():
            if multiplier > 1:
                rename_map[original_col] = normalize_column_name(original_col, unit)

        if not rename_map:
            return dict(self.config.tables)

        resolved: dict[str, list[str]] = {}
        for table_name, items in self.config.tables.items():
            resolved[table_name] = [rename_map.get(item, item) for item in items]

        return resolved
