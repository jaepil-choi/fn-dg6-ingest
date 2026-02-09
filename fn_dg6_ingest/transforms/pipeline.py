"""
Transform pipeline orchestrator for fn-dg6-ingest.

Runs a configurable sequence of transform steps on the parsed DataFrame.
The default pipeline order is:

1. NumberParser: Strip commas and whitespace, coerce to numeric types.
2. UnitNormalizer: Scale monetary columns to base unit (원), rename suffixes.
3. EmptyEntityDropper: Remove entities with all-null data.
4. TableSplitter: Split into multiple DataFrames based on 아이템명 groupings.

The pipeline receives the full IngestConfig so each step can check
relevant flags (e.g., normalize_units, drop_empty_entities).

Returns a dict[str, DataFrame] mapping table_name -> DataFrame.
"""

from __future__ import annotations

import pandas as pd

from fn_dg6_ingest.config import IngestConfig


class TransformPipeline:
    """Orchestrates the sequence of data transforms."""

    def __init__(self, config: IngestConfig) -> None:
        self.config = config

    def run(self, df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        """Run all transforms and return split tables.

        Args:
            df: The parsed DataFrame in long form.

        Returns:
            Dict mapping table_name -> transformed DataFrame.
        """
        raise NotImplementedError("TransformPipeline.run() not yet implemented")
