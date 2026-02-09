"""
fn-dg6-ingest: Python library for ingesting FnGuide DataGuide 6 exports.

This is the public API surface. Two primary entry points:

- init(): First-run workflow. Detects format, discovers items, generates
  fnconfig.yaml, and optionally builds the output DB immediately.

- ingest(): Subsequent-run workflow. Loads and validates fnconfig.yaml,
  then rebuilds the entire output DB from that config.

See docs/vibe/prd.md for full requirements and architecture.
"""

__all__ = ["init", "ingest"]


def init(
    input_path: str,
    output_dir: str = "outputs/",
    config_path: str = "fnconfig.yaml",
    run_immediately: bool = True,
) -> str:
    """First-run entry point: detect format, generate config, optionally build DB.

    Args:
        input_path: Path to the DataGuide 6 CSV/Excel export file.
        output_dir: Directory where output tables will be written.
        config_path: Where to write the generated fnconfig.yaml.
        run_immediately: If True, also run the full pipeline after config generation.
            If False, only generate the config file and stop.

    Returns:
        The path to the generated fnconfig.yaml.
    """
    raise NotImplementedError("init() not yet implemented -- see implement-init-ingest-api todo")


def ingest(config_path: str = "fnconfig.yaml") -> list[str]:
    """Subsequent-run entry point: load config, validate, rebuild output DB.

    Args:
        config_path: Path to fnconfig.yaml (must already exist).

    Returns:
        List of output file paths that were written.

    Raises:
        FileNotFoundError: If config_path does not exist.
        ValidationError: If config fails Pydantic validation.
    """
    raise NotImplementedError("ingest() not yet implemented -- see implement-init-ingest-api todo")
