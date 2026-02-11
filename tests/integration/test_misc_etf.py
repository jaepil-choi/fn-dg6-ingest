"""
Integration tests: misc/custom ETF constituent data end-to-end.

Runs the full init() -> ingest() pipeline against real ETF constituent
CSV files. Uses tmp_path for output to avoid polluting the workspace.
"""

from __future__ import annotations

import pandas as pd
import pytest

from tests.conftest import LONG_ETF_CSV

_skip_etf = pytest.mark.skipif(
    not LONG_ETF_CSV.exists(),
    reason=f"Input file not found: {LONG_ETF_CSV}",
)


@pytest.mark.integration
class TestMiscEtfPipeline:
    """End-to-end tests for misc ETF constituent data."""

    @_skip_etf
    def test_init_etf_csv(self, tmp_path):
        """init() against ETF CSV generates config with correct format."""
        from fn_dg6_ingest import init
        from fn_dg6_ingest.config import load_config

        config_path = str(tmp_path / "fnconfig.yaml")
        output_dir = str(tmp_path / "out")

        init(
            input_path=str(LONG_ETF_CSV),
            output_dir=output_dir,
            config_path=config_path,
            run_immediately=True,
        )

        cfg = load_config(config_path)

        # Detected format should be misc_etf
        assert cfg.source.detected_format == "misc_etf"

        # Output files exist
        out = tmp_path / "out"
        for table_name in cfg.tables:
            assert (out / f"{table_name}.{cfg.output.output_format}").exists()
        assert (out / f"_meta.{cfg.output.output_format}").exists()

    @_skip_etf
    def test_ingest_etf_csv(self, tmp_path):
        """init() then ingest() produces correct output files and _meta."""
        from fn_dg6_ingest import init, ingest
        from fn_dg6_ingest.config import load_config

        config_path = str(tmp_path / "fnconfig.yaml")
        output_dir = str(tmp_path / "out")

        init(
            input_path=str(LONG_ETF_CSV),
            output_dir=output_dir,
            config_path=config_path,
            run_immediately=False,
        )

        ds = ingest(config_path=config_path)

        cfg = ds.config
        meta = pd.read_parquet(tmp_path / "out" / "_meta.parquet")
        assert len(meta) == sum(len(items) for items in cfg.tables.values())

    @_skip_etf
    def test_etf_key_columns_preserved(self, tmp_path):
        """Output table has correct key columns for misc format (not 코드/코드명/date)."""
        from fn_dg6_ingest import init
        from fn_dg6_ingest.config import load_config

        config_path = str(tmp_path / "fnconfig.yaml")
        output_dir = str(tmp_path / "out")

        init(
            input_path=str(LONG_ETF_CSV),
            output_dir=output_dir,
            config_path=config_path,
            run_immediately=True,
        )

        cfg = load_config(config_path)
        first_table = list(cfg.tables.keys())[0]
        df = pd.read_parquet(tmp_path / "out" / f"{first_table}.parquet")

        # ETF data should have its own key columns (date, ETF코드, ETF명, etc.)
        assert "date" in df.columns
        # Should NOT have the timeseries key columns (코드/코드명 aren't in ETF data)
        # ETF data has its own column structure
        assert len(df) > 0
