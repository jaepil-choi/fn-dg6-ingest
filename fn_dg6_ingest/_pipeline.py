"""
Internal pipeline orchestration for fn-dg6-ingest.

Extracted from ``__init__.py`` so that both the module-level
``init()``/``ingest()`` functions and ``Dataset.ingest()`` can
reuse the same pipeline → meta → export sequence without circular
imports.

This module is **not** part of the public API.
"""

from __future__ import annotations

import logging

from fn_dg6_ingest.config import IngestConfig
from fn_dg6_ingest.export import export_tables
from fn_dg6_ingest.meta import build_meta_table
from fn_dg6_ingest.parsers.base import ParseResult
from fn_dg6_ingest.transforms.pipeline import TransformPipeline

logger = logging.getLogger(__name__)


def build_table_assignment(
    config: IngestConfig,
    unit_info: dict[str, tuple[str, int]],
) -> dict[str, str]:
    """Build post-normalization ``아이템명 -> table_name`` mapping.

    After unit normalization, column names may have changed (e.g.,
    ``매출액(억원)`` → ``매출액(원)``).  This function applies those
    renames so the ``_meta`` table correctly maps items to tables.
    """
    from fn_dg6_ingest.transforms.units import normalize_column_name

    table_assignment: dict[str, str] = {}
    for table_name, items in config.tables.items():
        for item in items:
            if item in unit_info:
                _unit, multiplier = unit_info[item]
                if multiplier > 1:
                    renamed = normalize_column_name(item, _unit)
                    table_assignment[renamed] = table_name
                    continue
            table_assignment[item] = table_name
    return table_assignment


def build_entity_stats(
    pipeline_result: object,  # PipelineResult
    config: IngestConfig,
) -> dict[str, tuple[int, int]]:
    """Build per-table entity statistics from the pipeline result.

    The empty-entity drop runs *before* the table split, so all tables
    share the same ``(total, dropped)`` counts.  We still structure
    this per-table for future flexibility.
    """
    if pipeline_result.drop_result is not None:  # type: ignore[union-attr]
        total = pipeline_result.drop_result.entities_total  # type: ignore[union-attr]
        dropped = pipeline_result.drop_result.entities_dropped  # type: ignore[union-attr]
    else:
        total = 0
        dropped = 0

    return {table_name: (total, dropped) for table_name in config.tables}


def run_pipeline_and_export(
    config: IngestConfig,
    parse_result: ParseResult,
) -> list[str]:
    """Run the transform pipeline, build ``_meta``, and export to disk.

    Steps:
      1. Run the transform pipeline (numbers → units → empty drop → split).
      2. Build ``table_assignment`` and ``entity_stats`` dicts.
      3. Build the ``_meta`` DataFrame.
      4. Export all tables + ``_meta`` to disk.

    Args:
        config: The validated IngestConfig.
        parse_result: Output from the parser.

    Returns:
        List of output file paths that were written.
    """
    # 1. Transform pipeline
    pipeline = TransformPipeline(config)
    pipeline_result = pipeline.run(
        parse_result.df,
        key_columns=parse_result.key_columns or None,
    )

    # 2. Build helper dicts
    table_assignment = build_table_assignment(config, pipeline_result.unit_info)
    entity_stats = build_entity_stats(pipeline_result, config)

    # 3. Build _meta table
    meta_df = build_meta_table(
        config=config,
        items=parse_result.items,
        source_last_updated=parse_result.source_last_updated,
        table_assignment=table_assignment,
        unit_info=pipeline_result.unit_info,
        entity_stats=entity_stats,
    )

    # 4. Export
    written = export_tables(
        tables=pipeline_result.tables,
        meta_df=meta_df,
        output_dir=config.output.output_dir,
        output_format=config.output.output_format,
    )

    logger.info("Pipeline complete: wrote %d files", len(written))
    return written
