"""
Unit tests for config models and YAML I/O (fn_dg6_ingest.config).

Tests Pydantic model validation, YAML serialization round-trip,
default config generation, and table-vs-data cross-validation.
"""

import pytest
from pydantic import ValidationError

from fn_dg6_ingest.config import (
    IngestConfig,
    MetadataConfig,
    OutputConfig,
    SourceConfig,
    generate_default_config,
    load_config,
    save_config,
    validate_tables_against_data,
)
from fn_dg6_ingest.exceptions import ConfigValidationError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_source(fmt: str = "timeseries_wide") -> SourceConfig:
    return SourceConfig(input_path="inputs/test.csv", detected_format=fmt)


def _make_config(**overrides) -> IngestConfig:
    defaults = {
        "source": _make_source(),
        "tables": {"default": ["수정시가(원)", "수정고가(원)"]},
    }
    defaults.update(overrides)
    return IngestConfig(**defaults)


# ---------------------------------------------------------------------------
# SourceConfig
# ---------------------------------------------------------------------------

class TestSourceConfig:
    """Tests for SourceConfig validation."""

    def test_valid_timeseries_wide(self):
        cfg = SourceConfig(input_path="data.csv", detected_format="timeseries_wide")
        assert cfg.detected_format == "timeseries_wide"

    def test_valid_misc_etf(self):
        cfg = SourceConfig(input_path="data.csv", detected_format="misc_etf")
        assert cfg.detected_format == "misc_etf"

    def test_any_format_name_accepted(self):
        """detected_format is a free-form string matching layout format_name."""
        cfg = SourceConfig(input_path="data.csv", detected_format="snapshot_custom")
        assert cfg.detected_format == "snapshot_custom"

    def test_missing_input_path(self):
        with pytest.raises(ValidationError, match="input_path"):
            SourceConfig(detected_format="timeseries_wide")


# ---------------------------------------------------------------------------
# MetadataConfig
# ---------------------------------------------------------------------------

class TestMetadataConfig:
    """Tests for MetadataConfig -- all fields optional."""

    def test_empty_is_valid(self):
        cfg = MetadataConfig()
        assert cfg.frequency is None
        assert cfg.currency is None

    def test_full_metadata(self):
        cfg = MetadataConfig(
            frequency="일간",
            currency="원화",
            sort_order="오름차순",
            non_business_days="제외",
            include_weekends="제외",
            period_start="20160101",
            period_end="최근일자(20260206)",
            calendar_basis=True,
        )
        assert cfg.frequency == "일간"
        assert cfg.currency == "원화"
        assert cfg.period_start == "20160101"
        assert cfg.calendar_basis is True

    def test_extra_fields(self):
        cfg = MetadataConfig(
            data_category="ETF 구성종목",
            extra={"etf_code": "A069500", "etf_name": "KODEX 200"},
        )
        assert cfg.data_category == "ETF 구성종목"
        assert cfg.extra["etf_code"] == "A069500"


# ---------------------------------------------------------------------------
# OutputConfig
# ---------------------------------------------------------------------------

class TestOutputConfig:
    """Tests for OutputConfig defaults and validation."""

    def test_defaults(self):
        cfg = OutputConfig()
        assert cfg.output_dir == "outputs/"
        assert cfg.output_format == "parquet"
        assert cfg.normalize_units is True
        assert cfg.drop_empty_entities is True

    def test_csv_format(self):
        cfg = OutputConfig(output_format="csv")
        assert cfg.output_format == "csv"

    def test_toggle_flags(self):
        cfg = OutputConfig(normalize_units=False, drop_empty_entities=False)
        assert cfg.normalize_units is False
        assert cfg.drop_empty_entities is False


# ---------------------------------------------------------------------------
# IngestConfig
# ---------------------------------------------------------------------------

class TestIngestConfig:
    """Tests for IngestConfig top-level model."""

    def test_minimal_valid(self):
        cfg = _make_config()
        assert cfg.source.detected_format == "timeseries_wide"
        assert "default" in cfg.tables

    def test_defaults_applied(self):
        """MetadataConfig and OutputConfig should use defaults if not provided."""
        cfg = IngestConfig(
            source=_make_source(),
            tables={"default": ["item1"]},
        )
        assert cfg.metadata.frequency is None
        assert cfg.output.output_format == "parquet"

    def test_missing_source_rejected(self):
        with pytest.raises(ValidationError, match="source"):
            IngestConfig(tables={"default": ["item1"]})

    def test_empty_table_group_rejected(self):
        """A table group with an empty item list should fail validation."""
        with pytest.raises(ValidationError, match="empty item list"):
            IngestConfig(
                source=_make_source(),
                tables={"ohlcv": []},
            )

    def test_multiple_table_groups(self):
        cfg = _make_config(tables={
            "ohlcv": ["수정시가(원)", "수정고가(원)"],
            "volume": ["거래량(주)"],
        })
        assert len(cfg.tables) == 2
        assert cfg.tables["volume"] == ["거래량(주)"]


# ---------------------------------------------------------------------------
# YAML round-trip: save_config -> load_config
# ---------------------------------------------------------------------------

class TestYamlRoundTrip:
    """Tests for save_config / load_config round-trip fidelity."""

    def test_round_trip_minimal(self, tmp_path):
        original = _make_config()
        yaml_path = tmp_path / "fnconfig.yaml"
        save_config(original, yaml_path)
        loaded = load_config(yaml_path)
        assert loaded == original

    def test_round_trip_full_metadata(self, tmp_path):
        original = _make_config(
            metadata=MetadataConfig(
                frequency="일간",
                currency="원화",
                sort_order="오름차순",
                non_business_days="제외",
                include_weekends="제외",
                period_start="20160101",
                period_end="최근일자(20260206)",
                calendar_basis=True,
            ),
        )
        yaml_path = tmp_path / "fnconfig.yaml"
        save_config(original, yaml_path)
        loaded = load_config(yaml_path)
        assert loaded.metadata == original.metadata

    def test_round_trip_multiple_tables(self, tmp_path):
        original = _make_config(tables={
            "ohlcv": ["수정시가(원)", "수정고가(원)", "수정저가(원)", "수정주가(원)"],
            "volume": ["거래량(주)", "거래대금(원)"],
        })
        yaml_path = tmp_path / "fnconfig.yaml"
        save_config(original, yaml_path)
        loaded = load_config(yaml_path)
        assert loaded.tables == original.tables

    def test_round_trip_csv_format(self, tmp_path):
        original = _make_config(
            output=OutputConfig(output_format="csv", normalize_units=False),
        )
        yaml_path = tmp_path / "fnconfig.yaml"
        save_config(original, yaml_path)
        loaded = load_config(yaml_path)
        assert loaded.output.output_format == "csv"
        assert loaded.output.normalize_units is False

    def test_yaml_file_has_header_comment(self, tmp_path):
        yaml_path = tmp_path / "fnconfig.yaml"
        save_config(_make_config(), yaml_path)
        content = yaml_path.read_text(encoding="utf-8")
        assert content.startswith("# fn-dg6-ingest configuration")

    def test_yaml_contains_korean(self, tmp_path):
        """Verify that Korean characters survive the YAML round-trip."""
        yaml_path = tmp_path / "fnconfig.yaml"
        original = _make_config(
            metadata=MetadataConfig(frequency="일간"),
            tables={"default": ["수정시가(원)"]},
        )
        save_config(original, yaml_path)
        content = yaml_path.read_text(encoding="utf-8")
        assert "수정시가(원)" in content
        assert "일간" in content

    def test_round_trip_extra_metadata(self, tmp_path):
        """Extra metadata fields should survive round-trip."""
        original = _make_config(
            metadata=MetadataConfig(
                extra={"etf_code": "A069500", "etf_name": "KODEX 200"},
            ),
        )
        yaml_path = tmp_path / "fnconfig.yaml"
        save_config(original, yaml_path)
        loaded = load_config(yaml_path)
        assert loaded.metadata.extra == {"etf_code": "A069500", "etf_name": "KODEX 200"}


# ---------------------------------------------------------------------------
# load_config error cases
# ---------------------------------------------------------------------------

class TestLoadConfigErrors:
    """Tests for load_config failure modes."""

    def test_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="Config file not found"):
            load_config(tmp_path / "nonexistent.yaml")

    def test_empty_file(self, tmp_path):
        yaml_path = tmp_path / "empty.yaml"
        yaml_path.write_text("", encoding="utf-8")
        with pytest.raises(ConfigValidationError, match="empty"):
            load_config(yaml_path)

    def test_invalid_yaml_structure(self, tmp_path):
        yaml_path = tmp_path / "bad.yaml"
        yaml_path.write_text("source: just_a_string\n", encoding="utf-8")
        with pytest.raises(ValidationError):
            load_config(yaml_path)

    def test_missing_required_field(self, tmp_path):
        yaml_path = tmp_path / "incomplete.yaml"
        yaml_path.write_text(
            "output:\n  output_format: parquet\ntables:\n  default:\n    - item1\n",
            encoding="utf-8",
        )
        with pytest.raises(ValidationError, match="source"):
            load_config(yaml_path)


# ---------------------------------------------------------------------------
# generate_default_config
# ---------------------------------------------------------------------------

class TestGenerateDefaultConfig:
    """Tests for generate_default_config()."""

    def test_basic_generation(self):
        items = ["수정시가(원)", "수정고가(원)", "거래량(주)"]
        meta = MetadataConfig(frequency="일간", non_business_days="제외")
        cfg = generate_default_config(
            input_path="inputs/test.csv",
            detected_format="timeseries_wide",
            metadata=meta,
            discovered_items=items,
        )
        assert cfg.source.input_path == "inputs/test.csv"
        assert cfg.source.detected_format == "timeseries_wide"
        assert cfg.metadata.frequency == "일간"
        assert cfg.tables == {"default": items}
        assert cfg.output.output_format == "parquet"

    def test_misc_format(self):
        cfg = generate_default_config(
            input_path="inputs/etf.csv",
            detected_format="misc_etf",
            metadata=MetadataConfig(data_category="ETF 구성종목"),
            discovered_items=["주식수(계약수)", "금액", "금액기준 구성비중(%)"],
        )
        assert cfg.source.detected_format == "misc_etf"
        assert len(cfg.tables["default"]) == 3

    def test_custom_output_dir(self):
        cfg = generate_default_config(
            input_path="x.csv",
            detected_format="timeseries_wide",
            metadata=MetadataConfig(),
            discovered_items=["a"],
            output_dir="custom_output/",
        )
        assert cfg.output.output_dir == "custom_output/"

    def test_round_trip_after_generation(self, tmp_path):
        """Generated config should survive YAML round-trip."""
        cfg = generate_default_config(
            input_path="inputs/test.csv",
            detected_format="timeseries_wide",
            metadata=MetadataConfig(
                frequency="일간",
                period_start="20160101",
                period_end="최근일자(20260206)",
            ),
            discovered_items=["수정시가(원)", "거래량(주)"],
        )
        yaml_path = tmp_path / "fnconfig.yaml"
        save_config(cfg, yaml_path)
        loaded = load_config(yaml_path)
        assert loaded == cfg


# ---------------------------------------------------------------------------
# validate_tables_against_data
# ---------------------------------------------------------------------------

class TestValidateTablesAgainstData:
    """Tests for cross-validation of config tables vs available data items."""

    def test_all_items_present(self):
        cfg = _make_config(tables={"default": ["item_a", "item_b"]})
        validate_tables_against_data(cfg, {"item_a", "item_b", "item_c"})

    def test_missing_items_raises(self):
        cfg = _make_config(tables={"ohlcv": ["item_a", "item_missing"]})
        with pytest.raises(ConfigValidationError, match="item_missing"):
            validate_tables_against_data(cfg, {"item_a", "item_b"})

    def test_multiple_tables_missing(self):
        cfg = _make_config(tables={
            "ohlcv": ["item_a", "bad_1"],
            "volume": ["bad_2"],
        })
        with pytest.raises(ConfigValidationError) as exc_info:
            validate_tables_against_data(cfg, {"item_a"})
        msg = str(exc_info.value)
        assert "bad_1" in msg
        assert "bad_2" in msg

    def test_error_shows_available_items(self):
        cfg = _make_config(tables={"x": ["missing"]})
        with pytest.raises(ConfigValidationError, match="Available items"):
            validate_tables_against_data(cfg, {"real_a", "real_b"})

    def test_empty_tables_dict(self):
        cfg = IngestConfig(source=_make_source(), tables={})
        validate_tables_against_data(cfg, {"item_a"})
