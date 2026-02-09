"""
Configuration models and YAML I/O for fn-dg6-ingest.

This module defines the Pydantic models that map 1:1 to fnconfig.yaml,
plus helper functions for loading, saving, and auto-generating the config.

Key models:
- IngestConfig: Top-level config (source + metadata + output + tables).
- SourceConfig: Input file path and detected format.
- MetadataConfig: Extracted DataGuide 6 header metadata (출력주기, 비영업일, etc.).
- OutputConfig: Output directory, format, normalization and cleaning toggles.

Key functions:
- load_config(path) -> IngestConfig: Load and validate from YAML.
- save_config(config, path): Serialize to YAML.
- generate_default_config(...) -> IngestConfig: Build config from parsed data.

Why Pydantic + YAML:
- Pydantic gives us strict validation, type coercion, and clear error messages.
- YAML is human-editable (the user will hand-edit table groupings).
- Round-trip fidelity: load -> modify -> save preserves structure.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field


class SourceConfig(BaseModel):
    """Source file information."""
    input_path: str = Field(..., description="Path to the DataGuide 6 export file")
    detected_format: Literal["wide", "long"] = Field(
        ..., description="Auto-detected format; 'wide' for pivot, 'long' for normal form"
    )


class MetadataConfig(BaseModel):
    """Metadata extracted from the DataGuide 6 file header block.

    These values are discovered from the file, not user-configured.
    They are stored in the config for reference and reproducibility.
    """
    출력주기: str | None = None
    비영업일: str | None = None
    주말포함: str | None = None
    기간: list[str] | None = None
    기본설정: list[str] | None = None
    달력기준: bool | None = None
    # Long-format specific
    조회기간: list[str] | None = None
    # Additional fields can be added as new formats are discovered


class OutputConfig(BaseModel):
    """Output settings."""
    output_dir: str = Field("outputs/", description="Directory for output files")
    output_format: Literal["csv", "parquet"] = Field(
        "parquet", description="Output format"
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


def load_config(path: str | Path) -> IngestConfig:
    """Load and validate fnconfig.yaml into an IngestConfig model.

    Raises:
        FileNotFoundError: If the config file does not exist.
        pydantic.ValidationError: If the YAML content fails validation.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return IngestConfig.model_validate(raw)


def save_config(config: IngestConfig, path: str | Path) -> None:
    """Serialize an IngestConfig to YAML.

    Writes a human-readable YAML file with comments where possible.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = config.model_dump(mode="json")
    with open(path, "w", encoding="utf-8") as f:
        f.write("# fn-dg6-ingest configuration\n")
        f.write("# Edit this file to customize table groupings, output format, etc.\n\n")
        yaml.dump(data, f, allow_unicode=True, default_flow_style=False, sort_keys=False)


def generate_default_config(
    input_path: str,
    detected_format: Literal["wide", "long"],
    metadata: MetadataConfig,
    discovered_items: list[str],
    output_dir: str = "outputs/",
) -> IngestConfig:
    """Build an IngestConfig from parsed data (used on first run).

    Args:
        input_path: Path to the source file.
        detected_format: The auto-detected format ('wide' or 'long').
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
