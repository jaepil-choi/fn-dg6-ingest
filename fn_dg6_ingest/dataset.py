"""
Dataset handle for fn-dg6-ingest.

The ``Dataset`` class is a **handle object** that encapsulates a
parsed configuration and its associated output directory. Once
created (via ``fn_dg6_ingest.open()``), it remembers all paths so
users never need to specify them again.

It unifies both the **write side** (``ingest()``) and the **read
side** (``load()``, ``load_meta()``, ``describe()``) of the data
lifecycle in a single object.

Design rationale:
- **Handle pattern**: Captures path state once, avoids repetitive
  path arguments in every call.
- **Parquet-optimised reads**: ``load()`` delegates to
  ``reader.read_table()`` which uses PyArrow column pruning and
  predicate pushdown for efficient large-file access.
- **Zero-scan metadata**: ``describe()`` reads Parquet file footers
  and the ``_meta`` table without scanning data.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from fn_dg6_ingest.config import (
    IngestConfig,
    load_config,
    save_config,
    validate_tables_against_data,
)
from fn_dg6_ingest.detect import detect_format
from fn_dg6_ingest.reader import read_meta, read_table, read_table_info

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DatasetInfo -- lightweight metadata snapshot
# ---------------------------------------------------------------------------

@dataclass
class DatasetInfo:
    """Structured metadata about a dataset, returned by ``Dataset.describe()``.

    All fields are populated without scanning data rows (Parquet
    footer + ``_meta`` table).

    Attributes:
        config_path: Path to the ``fnconfig.yaml``.
        format_name: Detected layout format name (e.g., ``timeseries_wide``).
        tables: List of table names in the dataset.
        items: Mapping of table name → list of item (value) column names.
        shape: Mapping of table name → ``(num_rows, num_columns)``.
        date_range: ``(earliest_date, latest_date)`` from ``_meta``, or
            ``None`` if unavailable.
        entities: Number of unique entities, or ``None`` if unavailable.
        output_format: ``"parquet"`` or ``"csv"``.
        output_dir: Path to the output directory.
    """

    config_path: str
    format_name: str
    tables: list[str] = field(default_factory=list)
    items: dict[str, list[str]] = field(default_factory=dict)
    shape: dict[str, tuple[int, int]] = field(default_factory=dict)
    date_range: tuple[str, str] | None = None
    entities: int | None = None
    output_format: str = "parquet"
    output_dir: str = ""


# ---------------------------------------------------------------------------
# Dataset -- the main handle class
# ---------------------------------------------------------------------------

class Dataset:
    """Handle object for a fn-dg6-ingest dataset.

    Created by ``fn_dg6_ingest.open()``, ``init()``, or ``ingest()``.
    Provides unified access to write (``ingest``) and read (``load``,
    ``load_meta``, ``describe``) operations.

    Attributes:
        config: The parsed ``IngestConfig`` (always available).
        config_path: Path to the ``fnconfig.yaml`` on disk.
    """

    def __init__(self, config: IngestConfig, config_path: str | Path) -> None:
        self.config = config
        self.config_path = Path(config_path)

    # -- Properties ---------------------------------------------------------

    @property
    def output_dir(self) -> Path:
        """Resolved output directory from the config."""
        return Path(self.config.output.output_dir)

    def __repr__(self) -> str:
        fmt = self.config.source.detected_format
        tables = list(self.config.tables.keys())
        return (
            f"Dataset(format={fmt!r}, tables={tables}, "
            f"config_path={str(self.config_path)!r})"
        )

    # -- Write side ---------------------------------------------------------

    def ingest(self) -> list[str]:
        """Rebuild the output database from the current config.

        Orchestration:
          1. Re-detect format and re-parse the source file.
          2. Validate config items against source data.
          3. Run the transform pipeline → build ``_meta`` → export.

        Returns:
            List of output file paths that were written.
        """
        from fn_dg6_ingest._pipeline import run_pipeline_and_export

        logger.info("Dataset.ingest() -- config_path=%s", self.config_path)

        # Re-detect and re-parse
        parser_cls, layout = detect_format(self.config.source.input_path)
        parser = parser_cls()
        parse_result = parser.parse(self.config.source.input_path, layout)

        # Validate config against actual data
        available_items = {item.아이템명 for item in parse_result.items}
        validate_tables_against_data(self.config, available_items)

        # Run pipeline
        written = run_pipeline_and_export(self.config, parse_result)
        return written

    def save_config(self) -> None:
        """Write the current ``self.config`` to ``self.config_path``.

        Use this after programmatically modifying ``self.config``
        (e.g., changing table groupings) and before calling
        ``ingest()`` to rebuild with the new settings.
        """
        save_config(self.config, self.config_path)
        logger.info("Saved config to %s", self.config_path)

    # -- Read side ----------------------------------------------------------

    def load(
        self,
        table: str | None = None,
        codes: list[str] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        items: list[str] | None = None,
    ) -> pd.DataFrame | dict[str, pd.DataFrame]:
        """Load output data with optional filtering.

        When ``table`` is specified, returns a single DataFrame.
        When ``table`` is ``None``:
        - If only one table exists, returns a single DataFrame.
        - If multiple tables exist, returns ``dict[str, DataFrame]``.

        Filtering parameters are passed through to ``reader.read_table()``
        which uses Parquet-native column pruning and predicate pushdown
        when the output format is Parquet.

        Args:
            table: Specific table name to load.  If ``None``, loads all.
            codes: Entity codes to filter (e.g., ``["A005930"]``).
            date_from: Inclusive lower date bound (ISO format).
            date_to: Inclusive upper date bound (ISO format).
            items: Value column names to select.  Key columns are
                always included regardless.

        Returns:
            A single DataFrame or a dict mapping table names to DataFrames.

        Raises:
            ValueError: If *table* is specified but doesn't exist in config.
            FileNotFoundError: If the output file doesn't exist on disk.
        """
        output_format = self.config.output.output_format
        key_cols = self._infer_key_columns()

        if table is not None:
            if table not in self.config.tables:
                raise ValueError(
                    f"Table '{table}' not found in config. "
                    f"Available tables: {list(self.config.tables.keys())}"
                )
            return read_table(
                self.output_dir, table, output_format,
                codes=codes, date_from=date_from, date_to=date_to,
                items=items, key_columns=key_cols,
            )

        # Load all tables
        table_names = list(self.config.tables.keys())

        if len(table_names) == 1:
            return read_table(
                self.output_dir, table_names[0], output_format,
                codes=codes, date_from=date_from, date_to=date_to,
                items=items, key_columns=key_cols,
            )

        result: dict[str, pd.DataFrame] = {}
        for t_name in table_names:
            result[t_name] = read_table(
                self.output_dir, t_name, output_format,
                codes=codes, date_from=date_from, date_to=date_to,
                items=items, key_columns=key_cols,
            )
        return result

    def load_meta(self) -> pd.DataFrame:
        """Load the ``_meta`` lineage table.

        Returns:
            The ``_meta`` DataFrame with all FR-9 columns.

        Raises:
            FileNotFoundError: If the ``_meta`` file doesn't exist.
        """
        return read_meta(self.output_dir, self.config.output.output_format)

    def describe(self) -> DatasetInfo:
        """Quick metadata lookup without scanning data.

        For Parquet, reads file footers for row count and schema.
        Enriches with ``_meta`` table data for ``date_range`` and
        ``entities`` when available.

        Returns:
            A ``DatasetInfo`` instance.
        """
        output_format = self.config.output.output_format
        table_names = list(self.config.tables.keys())

        # -- Shape and items from table info --
        shape: dict[str, tuple[int, int]] = {}
        items_map: dict[str, list[str]] = {}
        key_cols = self._infer_key_columns()

        for t_name in table_names:
            try:
                info = read_table_info(
                    self.output_dir, t_name, output_format
                )
                shape[t_name] = (info["num_rows"], info["num_columns"])
                # Items = all columns minus key columns
                items_map[t_name] = [
                    c for c in info["columns"] if c not in key_cols
                ]
            except FileNotFoundError:
                logger.warning(
                    "Table file not found for '%s' -- skipping in describe()",
                    t_name,
                )

        # -- date_range and entities from _meta --
        date_range: tuple[str, str] | None = None
        entities: int | None = None
        try:
            meta = self.load_meta()
            if "period_start" in meta.columns and "period_end" in meta.columns:
                starts = meta["period_start"].dropna()
                ends = meta["period_end"].dropna()
                if len(starts) > 0 and len(ends) > 0:
                    date_range = (str(starts.iloc[0]), str(ends.iloc[0]))
            if "entities_total" in meta.columns:
                vals = meta["entities_total"].dropna()
                if len(vals) > 0:
                    entities = int(vals.iloc[0])
        except FileNotFoundError:
            logger.debug("_meta file not found -- describe() without enrichment")

        return DatasetInfo(
            config_path=str(self.config_path),
            format_name=self.config.source.detected_format,
            tables=table_names,
            items=items_map,
            shape=shape,
            date_range=date_range,
            entities=entities,
            output_format=output_format,
            output_dir=str(self.output_dir),
        )

    # -- Private helpers ----------------------------------------------------

    def _infer_key_columns(self) -> list[str]:
        """Infer key columns from the detected format.

        Time series formats use ``["코드", "코드명", "date"]``.
        Misc/custom formats have varying key columns -- we attempt
        to read them from the ``_meta`` table or fall back to defaults.
        """
        fmt = self.config.source.detected_format
        if fmt.startswith("timeseries"):
            return ["코드", "코드명", "date"]

        # For misc formats, try to read column names from the first table
        # and infer key columns as non-item columns.
        try:
            table_names = list(self.config.tables.keys())
            if table_names:
                info = read_table_info(
                    self.output_dir,
                    table_names[0],
                    self.config.output.output_format,
                )
                all_cols = info["columns"]
                # Items from config (may be post-normalization names)
                all_items = set()
                for item_list in self.config.tables.values():
                    all_items.update(item_list)
                return [c for c in all_cols if c not in all_items]
        except FileNotFoundError:
            pass

        # Ultimate fallback
        return ["코드", "코드명", "date"]
