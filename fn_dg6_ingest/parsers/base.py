"""
Base parser protocol / ABC for fn-dg6-ingest.

All format-specific parsers must implement this interface. The contract is:
1. parse() takes a file path and a Layout, and returns a ParseResult.
2. ParseResult contains the raw DataFrame, extracted metadata, and
   discovered item-level attributes (아이템코드, 유형, 집계주기, etc.).

Why an ABC:
- Enforces a consistent interface across parsers.
- Makes it easy to add new format parsers without touching existing code.
- Enables type-safe usage in the pipeline orchestrator.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from fn_dg6_ingest.config import MetadataConfig


@dataclass
class ItemInfo:
    """Item-level metadata discovered from data rows.

    Primarily populated by the time series parser, where each row
    has explicit item attributes. For misc/custom formats, only
    아이템명 may be populated (from column names).
    """
    아이템코드: str | None = None
    아이템명: str = ""
    유형: str | None = None
    집계주기: str | None = None


@dataclass
class ParseResult:
    """Standardized output from any parser.

    Attributes:
        df: The parsed data as a DataFrame in long (normal) form.
            For time series wide: pivoted to (코드, 코드명, date) + item columns.
            For misc/long: returned as-is with date column renamed to 'date'.
        metadata: Source-level metadata extracted via layout coordinates.
        items: List of item-level metadata (one per unique item).
        source_last_updated: The 'Last Updated' timestamp from the Refresh header.
        format_name: The layout format_name that was used to parse this file.
        key_columns: Column names that are entity identifiers or dates
            (NOT value/item columns). Populated by each parser so the
            pipeline knows which columns to exclude from numeric transforms
            and table splitting. E.g., ``["코드", "코드명", "date"]`` for
            time series, or ``["날짜", "ETF코드", "ETF명", ...]`` for misc.
    """
    df: pd.DataFrame
    metadata: MetadataConfig
    items: list[ItemInfo] = field(default_factory=list)
    source_last_updated: str | None = None
    format_name: str = ""
    key_columns: list[str] = field(default_factory=list)


class BaseParser(ABC):
    """Abstract base class for DataGuide 6 format parsers.

    Subclasses must implement parse(). The Layout object is passed
    to parse() so parsers can use cell coordinates for metadata
    extraction instead of heuristic scanning.
    """

    @abstractmethod
    def parse(self, path: str | Path, layout: object) -> ParseResult:
        """Parse a DataGuide 6 export file.

        Args:
            path: Path to the CSV/Excel file.
            layout: A Layout object with cell coordinates and detection config.

        Returns:
            ParseResult with normalized DataFrame and metadata.

        Raises:
            ParsingError: If the file structure is unexpected.
        """
