"""
Long-format (normal form) parser for DataGuide 6 exports.

Handles files that are already in relational/normal form, such as
ETF constituent data. These files have a small metadata header block
followed by a proper column header row and data rows.

Input structure:
  - Lines 1-N: metadata header block (Refresh, ETF 구성종목, 출력주기, 조회기간, etc.)
  - Line N+1: column header row (날짜, ETF코드, ETF명, 구성종목코드, 구성종목, ...)
  - Lines N+2+: data rows in normal form

Output:
  - ParseResult with the DataFrame essentially as-is (already in long form),
    plus MetadataConfig from the header block.
  - ItemInfo list is populated from column names (each non-key column is an "item").

Key difference from wide parser:
  No pivot/melt needed -- the data is already in the target shape.
  Cleaning (number parsing, date normalization) still applies.
"""

from __future__ import annotations

from pathlib import Path

from fn_dg6_ingest.parsers.base import BaseParser, ParseResult


class LongFormatParser(BaseParser):
    """Parser for long/normal-form DataGuide 6 exports."""

    def parse(self, path: str | Path) -> ParseResult:
        raise NotImplementedError("LongFormatParser.parse() not yet implemented")
