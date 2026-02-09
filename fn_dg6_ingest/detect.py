"""
Format detection for FnGuide DataGuide 6 exports.

This module implements the FormatDetector, which reads the first N lines
of a file and determines whether it is in "wide" (pivot) or "long" (normal)
format using heuristic rules.

Design: Strategy Pattern
- The detector returns the appropriate parser class (not an instance).
- New formats can be added by extending the detection heuristic and
  registering a new parser in parsers/.

Detection heuristics:
- Wide format: metadata key rows (출력주기, 비영업일, 주말포함, 기간) present,
  AND a data header row with 코드, 코드명, 유형, 아이템코드, 아이템명 followed
  by date-like columns.
- Long format: a header row early in the file with descriptive column names
  (e.g., 날짜, ETF코드, ETF명) and no date-like columns in the header.
- Fallback: raise UnknownFormatError.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING

from fn_dg6_ingest.exceptions import UnknownFormatError

if TYPE_CHECKING:
    from fn_dg6_ingest.parsers.base import BaseParser


# Date pattern: YYYY-MM-DD or YYYYMMDD
_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}|\d{8}")

# Wide-format metadata keys that appear in the header block
_WIDE_METADATA_KEYS = {"출력주기", "비영업일", "주말포함", "기간"}

# Wide-format data header columns
_WIDE_DATA_HEADER_COLS = {"코드", "코드명", "유형", "아이템코드", "아이템명"}


def _read_head(path: Path, n_lines: int = 20) -> list[str]:
    """Read the first n_lines of a file as raw strings."""
    lines: list[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= n_lines:
                break
            lines.append(line.rstrip("\n\r"))
    return lines


def detect_format(path: str | Path) -> type[BaseParser]:
    """Detect the format of a DataGuide 6 export file.

    Args:
        path: Path to the CSV/Excel file.

    Returns:
        The parser class to use (WideFormatParser or LongFormatParser).

    Raises:
        UnknownFormatError: If the file does not match any known format.
    """
    # Lazy imports to avoid circular dependencies
    from fn_dg6_ingest.parsers.wide import WideFormatParser
    from fn_dg6_ingest.parsers.long import LongFormatParser

    path = Path(path)
    lines = _read_head(path, n_lines=20)

    if not lines:
        raise UnknownFormatError(f"File is empty: {path}")

    # Check for wide-format indicators
    found_metadata_keys: set[str] = set()
    has_wide_data_header = False

    for line in lines:
        cells = [c.strip() for c in line.split(",")]
        first_cell = cells[0] if cells else ""

        # Check if this line starts with a known metadata key
        if first_cell in _WIDE_METADATA_KEYS:
            found_metadata_keys.add(first_cell)

        # Check if this looks like the wide-format data header row
        cell_set = set(cells)
        if _WIDE_DATA_HEADER_COLS.issubset(cell_set):
            # Also check that later cells look like dates
            tail_cells = cells[len(_WIDE_DATA_HEADER_COLS) + 1 :]  # skip 집계주기 too
            if any(_DATE_PATTERN.match(c) for c in tail_cells):
                has_wide_data_header = True

    if found_metadata_keys and has_wide_data_header:
        return WideFormatParser

    # Check for long-format indicators
    # Long format has a header row with descriptive names and NO date columns
    for line in lines:
        cells = [c.strip() for c in line.split(",")]
        if "날짜" in cells and not any(_DATE_PATTERN.match(c) for c in cells):
            return LongFormatParser

    raise UnknownFormatError(
        f"Could not detect format for: {path}\n"
        f"First few lines:\n" + "\n".join(lines[:10])
    )
