"""
Base parser protocol / ABC for fn-dg6-ingest.

All format-specific parsers must implement this interface. The contract is:
1. parse() takes a file path and returns a ParseResult.
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
    """Item-level metadata discovered from data rows (wide format).

    For long-format data, some of these fields may be None since
    the structure doesn't have per-item rows.
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
            Must have at least columns: 코드, 코드명, date, plus one
            column per 아이템명 (for wide -> long transformation) or
            the original columns (for already-long data).
        metadata: Source-level metadata extracted from the file header.
        items: List of item-level metadata (one per unique 아이템명).
        source_last_updated: The 'Last Updated' timestamp from the Refresh header.
    """
    df: pd.DataFrame
    metadata: MetadataConfig
    items: list[ItemInfo] = field(default_factory=list)
    source_last_updated: str | None = None


class BaseParser(ABC):
    """Abstract base class for DataGuide 6 format parsers.

    Subclasses must implement parse().
    """

    @abstractmethod
    def parse(self, path: str | Path) -> ParseResult:
        """Parse a DataGuide 6 export file.

        Args:
            path: Path to the CSV/Excel file.

        Returns:
            ParseResult with normalized DataFrame and metadata.

        Raises:
            ParsingError: If the file structure is unexpected.
        """
