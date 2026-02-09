"""
Format detection for FnGuide DataGuide 6 exports.

Uses coordinate-based layout YAML files for detection instead of
heuristic scanning. Each layout defines detection rules (cell value
checks, data header column verification) that are tested in priority
order: timeseries -> snapshot -> misc.

Design: Strategy Pattern
- detect_format() returns a (parser_class, layout) tuple.
- The parser class is selected based on the layout's format_category.
- New formats are added by creating a layout YAML file -- no code changes.

Detection algorithm:
1. Load all layout YAML files from fn_dg6_ingest/layouts/.
2. Read the first N rows of the input file.
3. For each layout (ordered by category priority):
   a. Verify all check_cell rules match exact cell values.
   b. If check_data_header_cols is set, verify the data header row contains them.
   c. If all checks pass, return (parser_class, layout).
4. Fallback: raise UnknownFormatError.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

from fn_dg6_ingest.exceptions import UnknownFormatError
from fn_dg6_ingest.layout_registry import Layout, load_all_layouts
from fn_dg6_ingest.parsers.base import BaseParser

logger = logging.getLogger(__name__)

# Date pattern for verifying data header rows have date-like trailing columns
_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}$|\d{8}$")

# Maps (format_category, format_orientation) to parser class
_PARSER_MAP: dict[tuple[str, str], type[BaseParser]] = {}


def _get_parser_map() -> dict[tuple[str, str], type[BaseParser]]:
    """Lazily build the parser map to avoid circular imports."""
    if not _PARSER_MAP:
        from fn_dg6_ingest.parsers.timeseries import TimeSeriesWideParser
        from fn_dg6_ingest.parsers.snapshot import SnapshotParser
        from fn_dg6_ingest.parsers.misc import MiscParser

        _PARSER_MAP[("timeseries", "wide")] = TimeSeriesWideParser
        _PARSER_MAP[("timeseries", "long")] = TimeSeriesWideParser  # placeholder until long is implemented
        _PARSER_MAP[("snapshot", "wide")] = SnapshotParser
        _PARSER_MAP[("snapshot", "long")] = SnapshotParser
        _PARSER_MAP[("misc", "wide")] = MiscParser
        _PARSER_MAP[("misc", "long")] = MiscParser
    return _PARSER_MAP


def _read_rows(path: Path, n_rows: int) -> list[list[str]]:
    """Read the first n_rows of a file as lists of cell strings."""
    rows: list[list[str]] = []
    with open(path, "r", encoding="utf-8-sig") as f:
        for i, line in enumerate(f):
            if i >= n_rows:
                break
            rows.append([c.strip() for c in line.rstrip("\n\r").split(",")])
    return rows


def _check_layout(layout: Layout, rows: list[list[str]]) -> bool:
    """Test whether a file's rows match a layout's detection rules.

    Returns True if all detection checks pass.
    """
    # Check each cell value rule
    for check in layout.detection.check_cell:
        if check.row >= len(rows):
            return False
        row = rows[check.row]
        if check.col >= len(row):
            return False
        if row[check.col] != check.value:
            return False

    # Check data header columns if specified
    if layout.detection.check_data_header_cols:
        header_row_idx = layout.detection.data_header_row
        if header_row_idx >= len(rows):
            return False
        header_cells = set(rows[header_row_idx])
        required = set(layout.detection.check_data_header_cols)
        if not required.issubset(header_cells):
            return False
        # For wide time series, also verify trailing cells look like dates
        if layout.format_orientation == "wide" and layout.format_category == "timeseries":
            # Check cells after the fixed columns
            header = rows[header_row_idx]
            tail_start = len(layout.detection.check_data_header_cols)
            tail_cells = header[tail_start:]
            if not any(_DATE_PATTERN.match(c) for c in tail_cells if c):
                return False

    return True


def detect_format(
    path: str | Path,
    layouts: list[Layout] | None = None,
) -> tuple[type[BaseParser], Layout]:
    """Detect the format of a DataGuide 6 export file.

    Args:
        path: Path to the CSV/Excel file.
        layouts: Pre-loaded layouts (optional; loads from disk if None).

    Returns:
        Tuple of (parser_class, matched_layout).

    Raises:
        UnknownFormatError: If the file does not match any known layout.
    """
    path = Path(path)

    if layouts is None:
        layouts = load_all_layouts()

    if not layouts:
        raise UnknownFormatError("No layout YAML files found. Cannot detect format.")

    # Read enough rows to cover the largest possible data_header_row + 1
    max_rows = max(l.data_header_row for l in layouts) + 2
    rows = _read_rows(path, n_rows=max_rows)

    if not rows:
        raise UnknownFormatError(f"File is empty: {path}")

    parser_map = _get_parser_map()

    # Try each layout in priority order (timeseries -> snapshot -> misc)
    for layout in layouts:
        if _check_layout(layout, rows):
            key = (layout.format_category, layout.format_orientation)
            parser_cls = parser_map.get(key)
            if parser_cls is None:
                logger.warning(
                    "Layout '%s' matched but no parser for (%s, %s)",
                    layout.format_name, layout.format_category, layout.format_orientation,
                )
                continue
            logger.info(
                "Detected format '%s' for %s", layout.format_name, path
            )
            return parser_cls, layout

    # Build diagnostic message
    first_lines = "\n".join(
        ",".join(r) for r in rows[:10]
    )
    raise UnknownFormatError(
        f"Could not detect format for: {path}\n"
        f"Tried {len(layouts)} layouts, none matched.\n"
        f"First few lines:\n{first_lines}"
    )
