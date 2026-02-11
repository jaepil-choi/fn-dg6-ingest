"""
Configuration models and YAML I/O for fn-dg6-ingest.

This module defines the Pydantic models that map 1:1 to fnconfig.yaml,
plus helper functions for loading, saving, and auto-generating the config.

Key models:
- IngestConfig: Top-level config (source + metadata + output + tables).
- SourceConfig: Input file path and detected format name.
- MetadataConfig: Extracted DataGuide 6 header metadata with semantic English keys.
- OutputConfig: Output directory, format, normalization and cleaning toggles.

Key functions:
- load_config(path) -> IngestConfig: Load and validate from YAML.
- save_config(config, path): Serialize to YAML.
- generate_default_config(...) -> IngestConfig: Build config from parsed data.
- validate_tables_against_data(config, available_items): Cross-check config vs data.

Why Pydantic + YAML:
- Pydantic gives us strict validation, type coercion, and clear error messages.
- YAML is human-editable (the user will hand-edit table groupings).
- Round-trip fidelity: load -> modify -> save preserves structure.
"""

from __future__ import annotations

import logging
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, model_validator

from fn_dg6_ingest.exceptions import ConfigValidationError

logger = logging.getLogger(__name__)


class SourceConfig(BaseModel):
    """Source file information."""

    input_path: str = Field(..., description="Path to the DataGuide 6 export file")
    detected_format: str = Field(
        ...,
        description=(
            "Auto-detected layout format_name from the layout YAML files. "
            "Examples: 'timeseries_wide', 'misc_etf'. "
            "Matches a format_name in fn_dg6_ingest/layouts/*.yaml."
        ),
    )


class MetadataConfig(BaseModel):
    """Metadata extracted from the DataGuide 6 file header block.

    These values are discovered from the file via layout cell coordinates,
    not user-configured. They use semantic English keys rather than Korean
    labels, because the Korean labels (e.g., 기본설정) are UI identifiers,
    not semantic names.

    Settings extracted by the layout are stored here. Keys that don't match
    a defined field go into the `extra` dict.
    """

    last_updated: str | None = None
    data_category: str | None = Field(
        None, description="Data category from header (e.g., 'ETF 구성종목')"
    )
    calendar_basis: bool | None = None
    code_portfolio: str | None = None
    frequency: str | None = Field(None, description="Output frequency (e.g., 일간)")
    currency: str | None = Field(None, description="Currency setting (e.g., 원화)")
    sort_order: str | None = Field(None, description="Sort order (e.g., 오름차순)")
    non_business_days: str | None = Field(None, description="Non-business day handling (e.g., 제외)")
    include_weekends: str | None = Field(None, description="Weekend inclusion (e.g., 제외)")
    period_start: str | None = Field(None, description="Data period start (e.g., 20160101)")
    period_end: str | None = Field(None, description="Data period end (e.g., 최근일자(20260206))")
    # Catch-all for layout-specific fields not in the standard schema
    extra: dict[str, str] = Field(
        default_factory=dict,
        description="Additional settings from layout that don't have dedicated fields",
    )


class OutputConfig(BaseModel):
    """Output settings."""

    output_dir: str = Field("outputs/", description="Directory for output files")
    output_format: str = Field(
        "parquet", description="Output format: csv | parquet"
    )
    normalize_units: bool = Field(
        True, description="If True, scale monetary columns to base unit (원)"
    )
    drop_empty_entities: bool = Field(
        True, description="If True, drop entities with all-null data"
    )


class IngestConfig(BaseModel):
    """Top-level configuration for fn-dg6-ingest.

    Maps 1:1 to fnconfig.yaml. This is the single source of truth
    for the pipeline on subsequent runs.
    """

    source: SourceConfig
    metadata: MetadataConfig = Field(default_factory=MetadataConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)
    tables: dict[str, list[str]] = Field(
        default_factory=dict,
        description=(
            "Table groupings: key = table name, value = list of 아이템명. "
            "If empty or single 'default' key, all items go into one table."
        ),
    )

    @model_validator(mode="after")
    def _check_tables_not_empty_lists(self) -> IngestConfig:
        """Validate that no table group has an empty item list."""
        for table_name, items in self.tables.items():
            if not items:
                raise ValueError(
                    f"Table group '{table_name}' has an empty item list. "
                    "Each table group must contain at least one 아이템명."
                )
        return self


def load_config(path: str | Path) -> IngestConfig:
    """Load and validate fnconfig.yaml into an IngestConfig model.

    Raises:
        FileNotFoundError: If the config file does not exist.
        pydantic.ValidationError: If the YAML content fails schema validation.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if raw is None:
        raise ConfigValidationError(f"Config file is empty: {path}")
    logger.info("Loaded config from %s", path)
    return IngestConfig.model_validate(raw)


def save_config(config: IngestConfig, path: str | Path) -> None:
    """Serialize an IngestConfig to YAML.

    Writes a human-readable YAML file with a header comment.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = config.model_dump(mode="json")
    with open(path, "w", encoding="utf-8") as f:
        f.write("# fn-dg6-ingest configuration\n")
        f.write(
            "# Edit this file to customize table groupings, output format, etc.\n\n"
        )
        yaml.dump(
            data,
            f,
            allow_unicode=True,
            default_flow_style=False,
            sort_keys=False,
        )
    logger.info("Saved config to %s", path)


def generate_default_config(
    input_path: str,
    detected_format: str,
    metadata: MetadataConfig,
    discovered_items: list[str],
    output_dir: str = "outputs/",
) -> IngestConfig:
    """Build an IngestConfig from parsed data (used on first run).

    Args:
        input_path: Path to the source file.
        detected_format: The layout format_name (e.g., 'timeseries_wide').
        metadata: Metadata extracted from the file header.
        discovered_items: List of all 아이템명 found in the data.
        output_dir: Where output files should be written.

    Returns:
        A fully populated IngestConfig with all items in a single 'default' table.
    """
    return IngestConfig(
        source=SourceConfig(input_path=input_path, detected_format=detected_format),
        metadata=metadata,
        output=OutputConfig(output_dir=output_dir),
        tables={"default": discovered_items},
    )


def validate_tables_against_data(
    config: IngestConfig, available_items: set[str]
) -> None:
    """Cross-validate that all 아이템명 referenced in config exist in the source data.

    Called on subsequent runs (when config already exists) to catch stale
    or mistyped item names early, before the pipeline processes data.

    Args:
        config: The loaded IngestConfig.
        available_items: Set of 아이템명 actually present in the source data.

    Raises:
        ConfigValidationError: If any referenced item does not exist in the data.
    """
    missing: dict[str, list[str]] = {}
    for table_name, items in config.tables.items():
        not_found = [item for item in items if item not in available_items]
        if not_found:
            missing[table_name] = not_found

    if missing:
        details = "\n".join(
            f"  Table '{t}': {items}" for t, items in missing.items()
        )
        raise ConfigValidationError(
            f"The following 아이템명 in fnconfig.yaml do not exist in the source data:\n"
            f"{details}\n"
            f"Available items: {sorted(available_items)}"
        )
    logger.info(
        "Config validation passed: all %d items found in source data",
        sum(len(items) for items in config.tables.values()),
    )
