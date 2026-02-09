"""
Misc/Custom parser for DataGuide 6 exports.

Handles custom/miscellaneous DG6 formats that don't fit the standard
time series or snapshot patterns. Examples: ETF constituent data.

These formats are typically already in normal (long) form, but with
varying header structures. The parser relies on a Layout YAML for
coordinate-based metadata extraction and data_header_row to find
where data begins.

Key column detection uses a data-driven approach: columns whose values
are predominantly numeric are treated as value columns; the rest are
key columns (entity identifiers).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from fn_dg6_ingest.config import MetadataConfig
from fn_dg6_ingest.exceptions import ParsingError
from fn_dg6_ingest.layouts import Layout, extract_settings
from fn_dg6_ingest.parsers.base import BaseParser, ItemInfo, ParseResult

logger = logging.getLogger(__name__)


def _detect_value_columns(df: pd.DataFrame, date_col: str = "date") -> list[str]:
    """Identify value columns using a data-driven numeric check.

    Columns whose first 20 non-empty values are predominantly numeric
    (after stripping commas/whitespace) are classified as value columns.
    Everything else is a key column.

    Args:
        df: The parsed DataFrame.
        date_col: The date column name (always a key).

    Returns:
        List of column names identified as value columns.
    """
    value_cols: list[str] = []
    for col in df.columns:
        if col == date_col:
            continue
        sample = df[col].head(20).replace("", pd.NA).dropna()
        if len(sample) == 0:
            continue
        # Strip commas and whitespace, then try numeric conversion
        cleaned = sample.str.replace(",", "", regex=False).str.strip()
        numeric = pd.to_numeric(cleaned, errors="coerce")
        ratio = numeric.notna().sum() / len(sample)
        if ratio > 0.5:
            value_cols.append(col)
    return value_cols


class MiscParser(BaseParser):
    """Parser for custom/miscellaneous DataGuide 6 exports."""

    def parse(self, path: str | Path, layout: Layout) -> ParseResult:
        path = Path(path)
        logger.info("Parsing misc format file: %s (layout: %s)", path, layout.format_name)

        # Step 1: Read header rows and extract metadata via layout coordinates
        head_rows: list[list[str]] = []
        with open(path, "r", encoding="utf-8-sig") as f:
            for i, line in enumerate(f):
                if i >= layout.data_header_row + 2:
                    break
                head_rows.append([c.strip() for c in line.rstrip("\n\r").split(",")])

        settings = extract_settings(layout, head_rows)
        source_last_updated = settings.pop("last_updated", None)

        # Build MetadataConfig from extracted settings
        metadata = MetadataConfig(**{
            k: v for k, v in settings.items()
            if k in MetadataConfig.model_fields
        })
        extra_keys = set(settings.keys()) - set(MetadataConfig.model_fields.keys())
        metadata.extra = {k: settings[k] for k in extra_keys if settings[k] is not None}

        # Step 2: Parse data with pandas
        header_row = layout.data_header_row
        df = pd.read_csv(
            path,
            skiprows=header_row,
            encoding="utf-8-sig",
            dtype=str,
            keep_default_na=False,
        )
        df.columns = [c.strip() for c in df.columns]

        # Normalize date column name
        if "날짜" in df.columns:
            df = df.rename(columns={"날짜": "date"})
        elif "date" not in df.columns:
            raise ParsingError(
                f"No date column ('날짜' or 'date') found. Columns: {list(df.columns[:10])}"
            )

        logger.info("Parsed %d rows x %d columns", len(df), len(df.columns))

        # Step 3: Identify value columns via data-driven numeric check
        value_columns = _detect_value_columns(df)
        items = [ItemInfo(아이템명=col) for col in value_columns]

        logger.info(
            "Detected %d value columns, %d key columns",
            len(value_columns),
            len(df.columns) - len(value_columns),
        )

        return ParseResult(
            df=df,
            metadata=metadata,
            items=items,
            source_last_updated=source_last_updated,
            format_name=layout.format_name,
        )
