"""
Snapshot parser for DataGuide 6 exports (placeholder).

Handles DG6 snapshot-style data. This is a top-level category in
DataGuide 6's interface alongside time series and custom/misc.

Not yet implemented -- will be added when sample data is available.
The parser will follow the same pattern as TimeSeriesWideParser:
receive a Layout with cell coordinates, extract metadata, parse data.
"""

from __future__ import annotations

from pathlib import Path

from fn_dg6_ingest.layouts import Layout
from fn_dg6_ingest.parsers.base import BaseParser, ParseResult


class SnapshotParser(BaseParser):
    """Parser for snapshot-style DataGuide 6 exports (placeholder)."""

    def parse(self, path: str | Path, layout: Layout) -> ParseResult:
        raise NotImplementedError(
            "SnapshotParser is not yet implemented. "
            "Add a snapshot layout YAML and implement parsing when sample data is available."
        )
