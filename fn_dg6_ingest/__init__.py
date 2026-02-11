"""
fn-dg6-ingest: Python library for ingesting FnGuide DataGuide 6 exports.

Public API surface:

- ``open(path, ...)`` -- **recommended entry point**. Polymorphic: accepts
  either a DG6 source file or an existing ``fnconfig.yaml`` path and
  returns a ``Dataset`` handle.

- ``init(...)`` -- First-run workflow. Detects format, discovers items,
  generates ``fnconfig.yaml``, and optionally builds the output DB.
  Returns a ``Dataset``.

- ``ingest(...)`` -- Subsequent-run workflow. Loads and validates
  ``fnconfig.yaml``, then rebuilds the entire output DB.  Returns a
  ``Dataset``.

See docs/vibe/prd.md for full requirements and architecture.
"""

from __future__ import annotations

import logging
from pathlib import Path

from fn_dg6_ingest._pipeline import run_pipeline_and_export
from fn_dg6_ingest.config import (
    IngestConfig,
    generate_default_config,
    load_config,
    save_config,
    validate_tables_against_data,
)
from fn_dg6_ingest.dataset import Dataset
from fn_dg6_ingest.detect import detect_format

__all__ = ["open", "init", "ingest", "Dataset"]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _outputs_exist(config: IngestConfig) -> bool:
    """Check whether the output files for a config already exist on disk.

    Returns ``True`` when **all** expected table files plus the
    ``_meta`` file are present.  This is used by ``open()`` to skip
    the expensive pipeline when data has already been built.
    """
    out = Path(config.output.output_dir)
    fmt = config.output.output_format

    if not out.is_dir():
        return False

    # Check every table declared in config
    for table_name in config.tables:
        if not (out / f"{table_name}.{fmt}").exists():
            return False

    # Check _meta
    if not (out / f"_meta.{fmt}").exists():
        return False

    return True


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def open(
    path: str,
    output_dir: str | None = None,
    config_path: str | None = None,
    run_immediately: bool = True,
    force: bool = False,
) -> Dataset:
    """Single entry point: open a DG6 source file or an existing config.

    Polymorphic behaviour based on the file extension of *path*:

    - **YAML file** (``.yaml`` / ``.yml``): Loads the existing config and
      returns a ``Dataset`` handle.  No pipeline execution.

    - **DG6 source file** (CSV / Excel): **Idempotent** first-run
      workflow.  If the config and output data already exist, skips the
      expensive transformation and returns a ``Dataset`` handle pointing
      at the existing outputs.  If outputs are missing, detects the
      format, generates the config, and (when *run_immediately* is True)
      runs the pipeline.  Pass ``force=True`` to always rebuild.

    Args:
        path: Path to either a DG6 source file (CSV/Excel) or an
            existing ``fnconfig.yaml``.
        output_dir: Where output tables will be written.  Required for
            source files; ignored when opening a YAML config.  If
            ``None`` for a source file, defaults to ``"outputs/"``.
        config_path: Where to write the generated config.  If ``None``,
            derived as ``{output_dir}.yaml`` (sibling of output_dir).
            Ignored when opening a YAML config.
        run_immediately: If ``True`` (default), run the full pipeline
            immediately after config generation.  Only applies to
            source files (and only when outputs don't already exist).
        force: If ``True``, always re-run the pipeline even when
            outputs already exist.

    Returns:
        A ``Dataset`` handle.

    Examples::

        # First run -- builds everything
        ds = fn_dg6_ingest.open(
            "inputs/dataguide_ohlcv.csv",
            output_dir="outputs/ohlcv",
        )

        # Second call -- detects existing outputs, skips build
        ds = fn_dg6_ingest.open(
            "inputs/dataguide_ohlcv.csv",
            output_dir="outputs/ohlcv",
        )

        # Force rebuild
        ds = fn_dg6_ingest.open(
            "inputs/dataguide_ohlcv.csv",
            output_dir="outputs/ohlcv",
            force=True,
        )

        # Open from config directly
        ds = fn_dg6_ingest.open("outputs/ohlcv.yaml")

        # Read data
        df = ds.load(codes=["A005930"])
    """
    p = Path(path)

    if p.suffix.lower() in (".yaml", ".yml"):
        # -- Open existing config --
        logger.info("open() -- loading config from %s", path)
        config = load_config(path)
        return Dataset(config, p)

    # -- Source file path --
    if output_dir is None:
        output_dir = "outputs/"
    if config_path is None:
        # Derive config path as sibling YAML of the output_dir
        # e.g., output_dir="outputs/ohlcv" -> config_path="outputs/ohlcv.yaml"
        config_path = str(Path(output_dir).with_suffix(".yaml"))
        # If output_dir ends with "/" (bare), put yaml next to it
        if output_dir.endswith("/") or output_dir.endswith("\\"):
            config_path = str(Path(output_dir.rstrip("/\\")).with_suffix(".yaml"))

    # -- Skip if config + outputs already exist (idempotent open) --
    if not force and Path(config_path).exists():
        config = load_config(config_path)
        if _outputs_exist(config):
            logger.info(
                "open() -- outputs already exist, skipping build "
                "(config=%s, output_dir=%s)",
                config_path, config.output.output_dir,
            )
            return Dataset(config, config_path)
        # Config exists but outputs are missing -- rebuild
        logger.info(
            "open() -- config exists but outputs missing, running pipeline"
        )

    ds = init(
        input_path=path,
        output_dir=output_dir,
        config_path=config_path,
        run_immediately=run_immediately,
    )
    return ds


def init(
    input_path: str,
    output_dir: str = "outputs/",
    config_path: str = "fnconfig.yaml",
    run_immediately: bool = True,
) -> Dataset:
    """First-run entry point: detect format, generate config, optionally build DB.

    Orchestration:
      1. ``detect_format()`` -> ``(parser_class, layout)``
      2. ``parser.parse()`` -> ``ParseResult``
      3. ``generate_default_config()`` -> ``IngestConfig``
      4. ``save_config()`` to *config_path*
      5. If *run_immediately* is True, run the full pipeline via
         ``run_pipeline_and_export()``.

    Args:
        input_path: Path to the DataGuide 6 CSV/Excel export file.
        output_dir: Directory where output tables will be written.
        config_path: Where to write the generated fnconfig.yaml.
        run_immediately: If True, also run the full pipeline after config
            generation.  If False, only generate the config file and stop.

    Returns:
        A ``Dataset`` handle with the generated config.

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
        run_pipeline_and_export(config, parse_result)

    return Dataset(config, config_path)


def ingest(config_path: str = "fnconfig.yaml") -> Dataset:
    """Subsequent-run entry point: load config, validate, rebuild output DB.

    Orchestration:
      1. ``load_config()`` -> ``IngestConfig`` (Pydantic validation on load).
      2. ``detect_format()`` to get the parser for the source file.
      3. ``parser.parse()`` -> ``ParseResult``.
      4. ``validate_tables_against_data()`` -- cross-check config items vs
         source data.
      5. ``run_pipeline_and_export()`` -- transform, build meta, export.

    Args:
        config_path: Path to fnconfig.yaml (must already exist).

    Returns:
        A ``Dataset`` handle with the loaded config.

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
    run_pipeline_and_export(config, parse_result)

    return Dataset(config, config_path)
