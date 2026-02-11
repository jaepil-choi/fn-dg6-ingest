"""
Integration tests: config round-trip workflow.

Tests the full cycle: init() -> edit config -> ingest() rebuild.
Verifies that config modifications (e.g., splitting tables) are
correctly reflected in the rebuilt output.
"""

from __future__ import annotations

import pandas as pd
import pytest

from fn_dg6_ingest.exceptions import ConfigValidationError
from tests.conftest import WIDE_OHLCV_CSV

_skip_ohlcv = pytest.mark.skipif(
    not WIDE_OHLCV_CSV.exists(),
    reason=f"Input file not found: {WIDE_OHLCV_CSV}",
)


@pytest.mark.integration
class TestConfigRoundtrip:
    """Tests for the config-first workflow."""

    @_skip_ohlcv
    def test_init_generates_valid_config(self, tmp_path):
        """init() -> load generated config -> Pydantic validation passes."""
        from fn_dg6_ingest import init
        from fn_dg6_ingest.config import load_config

        config_path = str(tmp_path / "fnconfig.yaml")

        init(
            input_path=str(WIDE_OHLCV_CSV),
            output_dir=str(tmp_path / "out"),
            config_path=config_path,
            run_immediately=False,
        )

        # load_config runs Pydantic validation -- no exception means valid
        cfg = load_config(config_path)
        assert cfg.source.detected_format == "timeseries_wide"
        assert cfg.source.input_path == str(WIDE_OHLCV_CSV)
        assert len(cfg.tables) == 1
        assert "default" in cfg.tables
        assert len(cfg.tables["default"]) > 0

    @_skip_ohlcv
    def test_config_survives_edit_cycle(self, tmp_path):
        """init() -> load -> modify tables -> save -> ingest() succeeds."""
        from fn_dg6_ingest import init, ingest
        from fn_dg6_ingest.config import load_config, save_config

        config_path = str(tmp_path / "fnconfig.yaml")
        output_dir = str(tmp_path / "out")

        init(
            input_path=str(WIDE_OHLCV_CSV),
            output_dir=output_dir,
            config_path=config_path,
            run_immediately=False,
        )

        # Load, edit, save
        cfg = load_config(config_path)
        original_items = list(cfg.tables["default"])
        # Split into two groups (first half / second half)
        mid = len(original_items) // 2
        cfg.tables = {
            "group_a": original_items[:mid],
            "group_b": original_items[mid:],
        }
        save_config(cfg, config_path)

        # ingest() should succeed with the modified config
        written = ingest(config_path=config_path)
        assert len(written) == 3  # group_a, group_b, _meta

        out = tmp_path / "out"
        assert (out / "group_a.parquet").exists()
        assert (out / "group_b.parquet").exists()
        assert (out / "_meta.parquet").exists()

    @_skip_ohlcv
    def test_ingest_rejects_bad_item_names(self, tmp_path):
        """Edit config with non-existent 아이템명 -> ingest() raises ConfigValidationError."""
        from fn_dg6_ingest import init, ingest
        from fn_dg6_ingest.config import load_config, save_config

        config_path = str(tmp_path / "fnconfig.yaml")

        init(
            input_path=str(WIDE_OHLCV_CSV),
            output_dir=str(tmp_path / "out"),
            config_path=config_path,
            run_immediately=False,
        )

        # Inject a bad item name
        cfg = load_config(config_path)
        cfg.tables["default"].append("존재하지않는아이템(원)")
        save_config(cfg, config_path)

        with pytest.raises(ConfigValidationError, match="존재하지않는아이템"):
            ingest(config_path=config_path)

    @_skip_ohlcv
    def test_config_metadata_matches_source(self, tmp_path):
        """Metadata in generated config matches what the parser extracts."""
        from fn_dg6_ingest import init
        from fn_dg6_ingest.config import load_config
        from fn_dg6_ingest.detect import detect_format

        config_path = str(tmp_path / "fnconfig.yaml")

        init(
            input_path=str(WIDE_OHLCV_CSV),
            output_dir=str(tmp_path / "out"),
            config_path=config_path,
            run_immediately=False,
        )

        # Load config and re-parse to compare
        cfg = load_config(config_path)
        parser_cls, layout = detect_format(str(WIDE_OHLCV_CSV))
        parse_result = parser_cls().parse(str(WIDE_OHLCV_CSV), layout)

        assert cfg.metadata.frequency == parse_result.metadata.frequency
        assert cfg.metadata.period_start == parse_result.metadata.period_start
        assert cfg.metadata.period_end == parse_result.metadata.period_end
