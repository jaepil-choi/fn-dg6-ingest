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

from pathlib import Path

import hashlib
import pandas as pd

from fn_dg6_ingest.config import IngestConfig
from fn_dg6_ingest.parsers.base import ItemInfo


def _compute_file_hash(path: Path) -> str:
    """Compute SHA-256 hash of a file for reproducibility tracking."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def build_meta_table(
    config: IngestConfig,
    items: list[ItemInfo],
    source_last_updated: str | None,
    table_assignment: dict[str, str],  # 아이템명 -> table_name
    unit_info: dict[str, tuple[str, int]],  # 아이템명 -> (original_unit, multiplier)
    entity_stats: dict[str, tuple[int, int]],  # table_name -> (total, dropped)
) -> pd.DataFrame:
    """Build the flat _meta table.

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
    raise NotImplementedError("build_meta_table() not yet implemented")
