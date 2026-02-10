"""
Meta table builder for fn-dg6-ingest.

Builds the flat _meta table that is output alongside the data tables.
One row per (source_file, 아이템명) combination.

Purpose:
  The _meta table is DESCRIPTIVE -- it records what the pipeline did,
  providing data lineage and processing statistics. This complements
  fnconfig.yaml which is PRESCRIPTIVE (records what the user wants).

  Key information captured:
  - Source-level: filename, hash, last_updated, detected_format, frequency, etc.
  - Item-level: 아이템코드, 유형, 집계주기 (discovered from data, NOT user-configured).
  - Processing: unit normalization applied, entities dropped, timestamp.

Schema: see FR-9 in docs/vibe/prd.md for the full column specification.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from fn_dg6_ingest.config import IngestConfig
from fn_dg6_ingest.parsers.base import ItemInfo
from fn_dg6_ingest.transforms.units import normalize_column_name

logger = logging.getLogger(__name__)


def _compute_file_hash(path: Path) -> str:
    """Compute SHA-256 hash of a file for reproducibility tracking."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _resolve_normalized_name(
    item_name: str,
    unit_info: dict[str, tuple[str, int]],
) -> str:
    """Derive the post-unit-normalization item name.

    If the item was subject to unit scaling (multiplier > 1), the column
    was renamed from e.g. ``매출액(억원)`` to ``매출액(원)``.  Otherwise
    the name is unchanged.

    Args:
        item_name: Original 아이템명 from the source data.
        unit_info: Maps original 아이템명 -> (original_unit, multiplier).

    Returns:
        The normalized item name.
    """
    if item_name in unit_info:
        original_unit, multiplier = unit_info[item_name]
        if multiplier > 1:
            return normalize_column_name(item_name, original_unit)
    return item_name


def build_meta_table(
    config: IngestConfig,
    items: list[ItemInfo],
    source_last_updated: str | None,
    table_assignment: dict[str, str],  # 아이템명 -> table_name
    unit_info: dict[str, tuple[str, int]],  # 아이템명 -> (original_unit, multiplier)
    entity_stats: dict[str, tuple[int, int]],  # table_name -> (total, dropped)
) -> pd.DataFrame:
    """Build the flat _meta table.

    One row per ``(source_file, 아이템명)`` combination.  The table is
    purely **descriptive** -- it records what the pipeline did (data
    lineage), complementing ``fnconfig.yaml`` which is prescriptive.

    Column construction logic:

    - **Source-level** columns (same for every row): ``source_file``,
      ``source_hash``, ``source_last_updated``, ``detected_format``,
      ``frequency``, ``period_start``, ``period_end``,
      ``non_business_days``, ``include_weekends``.  Pulled from
      ``config`` and the ``source_last_updated`` argument.
    - **Item-level** columns (differ per row): ``아이템코드``, ``아이템명``,
      ``아이템명_normalized``, ``유형``, ``집계주기``.  Pulled from each
      ``ItemInfo`` in ``items``.
    - **Processing** columns: ``table_name``, ``unit_original``,
      ``unit_multiplier``, ``entities_total``, ``entities_dropped``,
      ``processed_at``.  Derived from ``table_assignment``,
      ``unit_info``, and ``entity_stats``.

    Args:
        config: The IngestConfig used for this pipeline run.
        items: Item-level metadata from the parser.
        source_last_updated: 'Last Updated' timestamp from Refresh header.
        table_assignment: Maps each 아이템명 to its output table name.
        unit_info: Maps each 아이템명 to (original_unit, multiplier_applied).
        entity_stats: Maps each table_name to (entities_total, entities_dropped).

    Returns:
        DataFrame with one row per (source_file, 아이템명).
        Columns use semantic English keys matching MetadataConfig fields:
        - table_name, source_file, source_hash, source_last_updated
        - detected_format, 아이템코드, 아이템명, 아이템명_normalized
        - 유형, 집계주기, frequency, period_start, period_end
        - unit_original, unit_multiplier, non_business_days, include_weekends
        - entities_total, entities_dropped, processed_at
    """
    # -- Source-level constants ----------------------------------------------
    source_path = Path(config.source.input_path)
    source_file = source_path.name

    # Compute hash; gracefully handle missing files (e.g. in unit tests with
    # synthetic data where the source file doesn't exist on disk).
    try:
        source_hash = _compute_file_hash(source_path)
    except FileNotFoundError:
        logger.warning(
            "Source file not found for hashing: %s (using empty hash)",
            source_path,
        )
        source_hash = ""

    processed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    meta = config.metadata

    # -- Build rows ----------------------------------------------------------
    rows: list[dict] = []
    for item in items:
        table_name = table_assignment.get(item.아이템명, "")

        # Unit normalization info for this item
        original_unit: str | None = None
        multiplier: int = 1
        if item.아이템명 in unit_info:
            original_unit, multiplier = unit_info[item.아이템명]

        normalized_name = _resolve_normalized_name(item.아이템명, unit_info)

        # Entity stats are per-table
        total, dropped = entity_stats.get(table_name, (0, 0))

        rows.append(
            {
                "table_name": table_name,
                "source_file": source_file,
                "source_hash": source_hash,
                "source_last_updated": source_last_updated,
                "detected_format": config.source.detected_format,
                "아이템코드": item.아이템코드,
                "아이템명": item.아이템명,
                "아이템명_normalized": normalized_name,
                "유형": item.유형,
                "집계주기": item.집계주기,
                "frequency": meta.frequency,
                "period_start": meta.period_start,
                "period_end": meta.period_end,
                "unit_original": original_unit,
                "unit_multiplier": multiplier,
                "non_business_days": meta.non_business_days,
                "include_weekends": meta.include_weekends,
                "entities_total": total,
                "entities_dropped": dropped,
                "processed_at": processed_at,
            }
        )

    logger.info("Built _meta table: %d rows", len(rows))

    # Explicit column order ensures a consistent schema even when rows is
    # empty (pd.DataFrame([]) would produce zero columns otherwise).
    columns = [
        "table_name", "source_file", "source_hash", "source_last_updated",
        "detected_format", "아이템코드", "아이템명", "아이템명_normalized",
        "유형", "집계주기", "frequency", "period_start", "period_end",
        "unit_original", "unit_multiplier", "non_business_days", "include_weekends",
        "entities_total", "entities_dropped", "processed_at",
    ]
    return pd.DataFrame(rows, columns=columns)
