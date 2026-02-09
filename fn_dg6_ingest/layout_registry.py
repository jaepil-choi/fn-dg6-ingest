"""
Layout loader for fn-dg6-ingest.

Loads layout YAML files from fn_dg6_ingest/layouts/ and provides
structured access via Pydantic models. Each layout defines:
- format_name: unique identifier (e.g., "timeseries_wide")
- format_category: top-level DG6 category (timeseries | snapshot | misc)
- format_orientation: data orientation (wide | long)
- detection: rules for identifying if a file matches this layout
- settings: cell-coordinate mappings for metadata extraction
- data_header_row: the row index where data column headers start

Why YAML instead of hardcoded:
- New formats can be added by dropping a YAML file, no code changes.
- Cell coordinates are easily editable when FnGuide changes layouts.
- Separation of structure knowledge (YAML) from parsing logic (Python).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# Directory containing layout YAML files (sibling package)
_LAYOUTS_DIR = Path(__file__).parent / "layouts"

# Detection priority: timeseries -> snapshot -> misc
_CATEGORY_PRIORITY = {"timeseries": 0, "snapshot": 1, "misc": 2}


class CellCheck(BaseModel):
    """A detection rule that checks a specific cell's value."""
    row: int
    col: int
    value: str


class CellCoord(BaseModel):
    """A setting coordinate: which cell to read and how to parse it."""
    row: int
    col: int
    parse: str = "raw"  # "raw" | "strip_prefix:<prefix>" | "presence"


class DetectionConfig(BaseModel):
    """Detection rules for a layout."""
    check_cell: list[CellCheck] = Field(default_factory=list)
    check_data_header_cols: list[str] | None = None
    data_header_row: int


class Layout(BaseModel):
    """A complete layout definition loaded from YAML."""
    format_name: str
    format_category: Literal["timeseries", "snapshot", "misc"]
    format_orientation: Literal["wide", "long"]
    description: str = ""
    detection: DetectionConfig
    settings: dict[str, CellCoord]

    @property
    def data_header_row(self) -> int:
        return self.detection.data_header_row

    @property
    def priority(self) -> int:
        """Lower number = checked first during detection."""
        return _CATEGORY_PRIORITY.get(self.format_category, 99)


def _parse_settings(raw_settings: dict[str, Any]) -> dict[str, CellCoord]:
    """Convert raw YAML settings dict into CellCoord models."""
    result: dict[str, CellCoord] = {}
    for key, spec in raw_settings.items():
        if isinstance(spec, dict):
            result[key] = CellCoord(**spec)
        else:
            raise ValueError(f"Invalid setting spec for '{key}': {spec}")
    return result


def load_layout(path: Path) -> Layout:
    """Load a single layout YAML file."""
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    # Parse detection config
    det_raw = raw.get("detection", {})
    check_cells = [CellCheck(**c) for c in det_raw.get("check_cell", [])]
    detection = DetectionConfig(
        check_cell=check_cells,
        check_data_header_cols=det_raw.get("check_data_header_cols"),
        data_header_row=det_raw["data_header_row"],
    )

    # Parse settings
    settings = _parse_settings(raw.get("settings", {}))

    return Layout(
        format_name=raw["format_name"],
        format_category=raw["format_category"],
        format_orientation=raw["format_orientation"],
        description=raw.get("description", ""),
        detection=detection,
        settings=settings,
    )


def load_all_layouts(layouts_dir: Path | None = None) -> list[Layout]:
    """Load all layout YAML files, sorted by detection priority.

    Args:
        layouts_dir: Directory to scan for .yaml files. Defaults to
            the built-in layouts/ directory.

    Returns:
        List of Layout objects, sorted by category priority
        (timeseries first, then snapshot, then misc).
    """
    layouts_dir = layouts_dir or _LAYOUTS_DIR
    layouts: list[Layout] = []
    for yaml_path in sorted(layouts_dir.glob("*.yaml")):
        try:
            layout = load_layout(yaml_path)
            layouts.append(layout)
            logger.debug("Loaded layout: %s from %s", layout.format_name, yaml_path)
        except Exception as e:
            logger.warning("Failed to load layout from %s: %s", yaml_path, e)
    # Sort by priority (timeseries first)
    layouts.sort(key=lambda l: l.priority)
    logger.info("Loaded %d layouts", len(layouts))
    return layouts


def extract_settings(layout: Layout, rows: list[list[str]]) -> dict[str, Any]:
    """Extract metadata settings from file rows using a layout's coordinate map.

    Args:
        layout: The matched Layout definition.
        rows: The first N rows of the file, each split into cells.

    Returns:
        Dict mapping setting name -> extracted value.
    """
    result: dict[str, Any] = {}
    for key, coord in layout.settings.items():
        # Safely get the cell value
        if coord.row < len(rows) and coord.col < len(rows[coord.row]):
            raw_value = rows[coord.row][coord.col].strip()
        else:
            raw_value = ""

        # Apply parse mode
        if coord.parse == "presence":
            result[key] = bool(raw_value)
        elif coord.parse.startswith("strip_prefix:"):
            prefix = coord.parse[len("strip_prefix:"):]
            result[key] = raw_value.replace(prefix, "").strip() if raw_value else None
        else:  # "raw"
            result[key] = raw_value if raw_value else None

    return result
