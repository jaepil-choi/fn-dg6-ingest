"""
Wide-format (pivot) parser for DataGuide 6 exports.

Handles files where dates are spread across columns and rows are
(코드, 아이템) pairs. This is the most common DataGuide 6 export format,
used for OHLCV, sales-consensus, and similar datasets.

Input structure:
  - Lines 1-N: metadata header block (Refresh, 출력주기, 비영업일, etc.)
  - Line N+1: data header row (코드, 코드명, 유형, 아이템코드, 아이템명, 집계주기, date1, date2, ...)
  - Lines N+2+: data rows, one per (코드, 아이템) combination

Output:
  - ParseResult with a long-form DataFrame (melted from wide) containing:
    코드, 코드명, date, and one column per 아이템명.
  - MetadataConfig populated from the header block.
  - ItemInfo list with per-item attributes (아이템코드, 유형, 집계주기).

Key transformation:
  Wide -> Long via pivot: each (코드, date) becomes a row, with 아이템명
  values becoming columns.
"""

from __future__ import annotations

from pathlib import Path

from fn_dg6_ingest.parsers.base import BaseParser, ParseResult


class WideFormatParser(BaseParser):
    """Parser for wide/pivot-style DataGuide 6 exports."""

    def parse(self, path: str | Path) -> ParseResult:
        raise NotImplementedError("WideFormatParser.parse() not yet implemented")
