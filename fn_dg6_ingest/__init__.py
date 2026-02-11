"""
fn-dg6-ingest: Python library for ingesting FnGuide DataGuide 6 exports.

This is the public API surface. Two primary entry points:

- init(): First-run workflow. Detects format, discovers items, generates
  fnconfig.yaml, and optionally builds the output DB immediately.

- ingest(): Subsequent-run workflow. Loads and validates fnconfig.yaml,
  then rebuilds the entire output DB from that config.

See docs/vibe/prd.md for full requirements and architecture.
"""

from __future__ import annotations

import logging
from pathlib import Path

from fn_dg6_ingest.config import (
    IngestConfig,
    generate_default_config,
    load_config,
    save_config,
    validate_tables_against_data,
)
from fn_dg6_ingest.detect import detect_format
from fn_dg6_ingest.export import export_tables
from fn_dg6_ingest.meta import build_meta_table
from fn_dg6_ingest.parsers.base import ParseResult
from fn_dg6_ingest.transforms.pipeline import TransformPipeline

__all__ = ["init", "ingest"]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _build_table_assignment(config: IngestConfig) -> dict[str, str]:
    """Invert ``config.tables`` into a map of ``아이템명 -> table_name``.

    The config stores ``{table_name: [item_names]}``.  Downstream
    ``build_meta_table`` needs the inverse: ``{item_name: table_name}``.
    """
    assignment: dict[str, str] = {}
    for table_name, items in config.tables.items():
        for item in items:
            assignment[item] = table_name
    return assignment


def _build_entity_stats(
    pipeline_result,  # PipelineResult -- avoid circular import at module level
    config: IngestConfig,
) -> dict[str, tuple[int, int]]:
    """Build per-table entity statistics from the pipeline result.

    The empty-entity drop currently runs *before* the table split, so
    all tables share the same ``(total, dropped)`` counts.  We still
    structure this per-table for future flexibility (the
    ``build_meta_table`` signature expects it).
    """
    if pipeline_result.drop_result is not None:
        total = pipeline_result.drop_result.entities_total
        dropped = pipeline_result.drop_result.entities_dropped
    else:
        total = 0
        dropped = 0

    return {table_name: (total, dropped) for table_name in config.tables}


def _run_pipeline_and_export(
    config: IngestConfig,
    parse_result: ParseResult,
) -> list[str]:
    """Shared logic for the pipeline -> meta -> export sequence.

    Used by both ``init(run_immediately=True)`` and ``ingest()``.

    Steps:
      1. Run the transform pipeline (numbers -> units -> empty drop -> split).
      2. Build ``table_assignment`` and ``entity_stats`` dicts.
      3. Build the ``_meta`` DataFrame.
      4. Export all tables + ``_meta`` to disk.

    Args:
        config: The validated IngestConfig.
        parse_result: Output from the parser (df, metadata, items, key_columns).

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
    #    table_assignment must use post-unit-rename item names so that
    #    _meta correctly maps items to tables after unit normalization.
    from fn_dg6_ingest.transforms.units import normalize_column_name

    table_assignment: dict[str, str] = {}
    for table_name, items in config.tables.items():
        for item in items:
            # Check if this item was renamed by unit normalization
            if item in pipeline_result.unit_info:
                _unit, multiplier = pipeline_result.unit_info[item]
                if multiplier > 1:
                    renamed = normalize_column_name(item, _unit)
                    table_assignment[renamed] = table_name
                    continue
            table_assignment[item] = table_name

    entity_stats = _build_entity_stats(pipeline_result, config)

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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def init(
    input_path: str,
    output_dir: str = "outputs/",
    config_path: str = "fnconfig.yaml",
    run_immediately: bool = True,
) -> str:
    """First-run entry point: detect format, generate config, optionally build DB.

    Orchestration:
      1. ``detect_format()`` -> ``(parser_class, layout)``
      2. ``parser.parse()`` -> ``ParseResult``
      3. ``generate_default_config()`` -> ``IngestConfig``
      4. ``save_config()`` to *config_path*
      5. If *run_immediately* is True, run the full pipeline via
         ``_run_pipeline_and_export()``.

    Args:
        input_path: Path to the DataGuide 6 CSV/Excel export file.
        output_dir: Directory where output tables will be written.
        config_path: Where to write the generated fnconfig.yaml.
        run_immediately: If True, also run the full pipeline after config
            generation.  If False, only generate the config file and stop.

    Returns:
        The path to the generated fnconfig.yaml.

    Raises:
        UnknownFormatError: If the input file doesn't match any known layout.
        ParsingError: If the parser encounters unexpected data structure.
    """
    logger.info("init() -- input_path=%s, output_dir=%s", input_path, output_dir)

    # Step 1: Detect format
    parser_cls, layout = detect_format(input_path)
    logger.info("Detected format: %s", layout.format_name)

    # Step 2: Parse the file
    parser = parser_cls()
    parse_result = parser.parse(input_path, layout)
    logger.info(
        "Parsed: %d rows, %d items, key_columns=%s",
        len(parse_result.df),
        len(parse_result.items),
        parse_result.key_columns,
    )

    # Step 3: Generate default config
    discovered_items = [item.아이템명 for item in parse_result.items]
    config = generate_default_config(
        input_path=input_path,
        detected_format=parse_result.format_name,
        metadata=parse_result.metadata,
        discovered_items=discovered_items,
        output_dir=output_dir,
    )

    # Step 4: Save config
    save_config(config, config_path)
    logger.info("Saved config to %s", config_path)

    # Step 5: Optionally run the full pipeline
    if run_immediately:
        logger.info("run_immediately=True -- running pipeline")
        _run_pipeline_and_export(config, parse_result)

    return config_path


def ingest(config_path: str = "fnconfig.yaml") -> list[str]:
    """Subsequent-run entry point: load config, validate, rebuild output DB.

    Orchestration:
      1. ``load_config()`` -> ``IngestConfig`` (Pydantic validation on load).
      2. ``detect_format()`` to get the parser for the source file.
      3. ``parser.parse()`` -> ``ParseResult``.
      4. ``validate_tables_against_data()`` -- cross-check config items vs
         source data.
      5. ``_run_pipeline_and_export()`` -- transform, build meta, export.

    Args:
        config_path: Path to fnconfig.yaml (must already exist).

    Returns:
        List of output file paths that were written.

    Raises:
        FileNotFoundError: If *config_path* does not exist.
        pydantic.ValidationError: If config fails Pydantic validation.
        ConfigValidationError: If config items don't match source data.
        UnknownFormatError: If the source file doesn't match any known layout.
        ParsingError: If the parser encounters unexpected data structure.
    """
    logger.info("ingest() -- config_path=%s", config_path)

    # Step 1: Load and validate config
    config = load_config(config_path)
    logger.info(
        "Loaded config: format=%s, %d table group(s)",
        config.source.detected_format,
        len(config.tables),
    )

    # Step 2: Detect format (re-detect to get the parser class)
    parser_cls, layout = detect_format(config.source.input_path)

    # Step 3: Parse the source file
    parser = parser_cls()
    parse_result = parser.parse(config.source.input_path, layout)

    # Step 4: Cross-validate config against source data
    available_items = {item.아이템명 for item in parse_result.items}
    validate_tables_against_data(config, available_items)

    # Step 5: Run pipeline -> meta -> export
    written = _run_pipeline_and_export(config, parse_result)

    return written
