"""
Time Series parser for DataGuide 6 exports.

Handles the most common DG6 format: time series data where dates are
spread across columns and rows are (코드, 아이템) pairs.

Currently implements: TimeSeriesWideParser (wide orientation).
Future: TimeSeriesLongParser (long/vertical orientation) can be added here.

Input structure (wide):
  - Rows 0-7: metadata header block (cell coordinates defined in layout YAML)
  - Row 8: data header row (코드, 코드명, 유형, 아이템코드, 아이템명, 집계주기, date1, date2, ...)
  - Rows 9+: data rows, one per (코드, 아이템) combination

Key transformation (wide):
  The raw wide data has one row per (코드, 아이템명) with dates as columns.
  We melt then pivot to produce one row per (코드, date) with 아이템명 as columns.

Metadata extraction uses the Layout's cell coordinates -- no heuristic scanning.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from fn_dg6_ingest.config import MetadataConfig
from fn_dg6_ingest.exceptions import ParsingError
from fn_dg6_ingest.layout_registry import Layout, extract_settings
from fn_dg6_ingest.parsers.base import BaseParser, ItemInfo, ParseResult

logger = logging.getLogger(__name__)

# Fixed columns in the wide data header row
_FIXED_COLS = ["코드", "코드명", "유형", "아이템코드", "아이템명", "집계주기"]


def _read_rows(path: Path, n_rows: int = 30) -> list[list[str]]:
    """Read the first n_rows of a CSV as lists of cell strings."""
    rows: list[list[str]] = []
    with open(path, "r", encoding="utf-8-sig") as f:
        for i, line in enumerate(f):
            if i >= n_rows:
                break
            rows.append([c.strip() for c in line.rstrip("\n\r").split(",")])
    return rows


class TimeSeriesWideParser(BaseParser):
    """Parser for time series wide-format DataGuide 6 exports."""

    def parse(self, path: str | Path, layout: Layout) -> ParseResult:
        path = Path(path)
        logger.info("Parsing time series wide file: %s", path)

        # Step 1: Read header rows and extract metadata via layout coordinates
        head_rows = _read_rows(path, n_rows=layout.data_header_row + 2)
        settings = extract_settings(layout, head_rows)

        source_last_updated = settings.pop("last_updated", None)

        # Build MetadataConfig from extracted settings
        metadata = MetadataConfig(**{
            k: v for k, v in settings.items()
            if k in MetadataConfig.model_fields
        })
        # Store any extra settings not in MetadataConfig
        extra_keys = set(settings.keys()) - set(MetadataConfig.model_fields.keys())
        metadata.extra = {k: settings[k] for k in extra_keys if settings[k] is not None}

        # Step 2: Parse the data grid using pandas
        header_row = layout.data_header_row
        df = pd.read_csv(
            path,
            skiprows=header_row,
            encoding="utf-8-sig",
            dtype=str,
            keep_default_na=False,
        )
        df.columns = [c.strip() for c in df.columns]

        # Validate expected fixed columns
        missing_cols = set(_FIXED_COLS) - set(df.columns)
        if missing_cols:
            raise ParsingError(
                f"Expected columns {missing_cols} not found. "
                f"Columns found: {list(df.columns[:10])}"
            )

        # Step 3: Identify date columns (everything after 집계주기)
        fixed_end = df.columns.get_loc("집계주기") + 1
        date_columns = list(df.columns[fixed_end:])

        if not date_columns:
            raise ParsingError("No date columns found after 집계주기 column.")

        logger.info("Found %d date columns, %d data rows", len(date_columns), len(df))

        # Step 4: Extract item-level metadata (unique items)
        item_info_df = (
            df[["아이템코드", "아이템명", "유형", "집계주기"]]
            .drop_duplicates(subset=["아이템코드", "아이템명"])
            .reset_index(drop=True)
        )
        items = [
            ItemInfo(
                아이템코드=row["아이템코드"] or None,
                아이템명=row["아이템명"],
                유형=row["유형"] or None,
                집계주기=row["집계주기"] or None,
            )
            for _, row in item_info_df.iterrows()
        ]

        # Step 5: Melt wide -> long
        melted = df.melt(
            id_vars=_FIXED_COLS,
            value_vars=date_columns,
            var_name="date",
            value_name="value",
        )

        # Step 6: Pivot so each 아이템명 becomes a column
        pivoted = melted.pivot_table(
            index=["코드", "코드명", "date"],
            columns="아이템명",
            values="value",
            aggfunc="first",
        ).reset_index()

        pivoted.columns.name = None

        logger.info("Pivoted to %d rows x %d columns", len(pivoted), len(pivoted.columns))

        return ParseResult(
            df=pivoted,
            metadata=metadata,
            items=items,
            source_last_updated=source_last_updated,
            format_name=layout.format_name,
        )
