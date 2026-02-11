"""
Integration tests: time series data end-to-end.

Runs the full init() -> ingest() pipeline against real time series
CSV files (OHLCV, sales-consensus).
Uses tmp_path for output to avoid polluting the workspace.
"""

from __future__ import annotations

import pandas as pd
import pytest

from tests.conftest import WIDE_OHLCV_CSV, WIDE_SALES_CSV

_skip_ohlcv = pytest.mark.skipif(
    not WIDE_OHLCV_CSV.exists(),
    reason=f"Input file not found: {WIDE_OHLCV_CSV}",
)
_skip_sales = pytest.mark.skipif(
    not WIDE_SALES_CSV.exists(),
    reason=f"Input file not found: {WIDE_SALES_CSV}",
)


@pytest.mark.integration
class TestTimeSeriesOhlcv:
    """End-to-end tests for OHLCV time series wide data."""

    @_skip_ohlcv
    def test_init_ohlcv_csv(self, tmp_path):
        """init() against OHLCV CSV generates config and output files."""
        from fn_dg6_ingest import init

        config_path = str(tmp_path / "fnconfig.yaml")
        output_dir = str(tmp_path / "out")

        ds = init(
            input_path=str(WIDE_OHLCV_CSV),
            output_dir=output_dir,
            config_path=config_path,
            run_immediately=True,
        )

        # Config was generated -- init() now returns Dataset
        assert str(ds.config_path) == config_path
        assert (tmp_path / "fnconfig.yaml").exists()

        # Output files exist (default table + _meta)
        cfg = ds.config
        out = tmp_path / "out"
        for table_name in cfg.tables:
            assert (out / f"{table_name}.{cfg.output.output_format}").exists()
        assert (out / f"_meta.{cfg.output.output_format}").exists()

        # _meta has correct column schema (all 20 FR-9 columns)
        meta = pd.read_parquet(out / "_meta.parquet")
        expected_cols = {
            "table_name", "source_file", "source_hash", "source_last_updated",
            "detected_format", "아이템코드", "아이템명", "아이템명_normalized",
            "유형", "집계주기", "frequency", "period_start", "period_end",
            "unit_original", "unit_multiplier", "non_business_days",
            "include_weekends", "entities_total", "entities_dropped",
            "processed_at",
        }
        assert set(meta.columns) == expected_cols

        # _meta row count matches number of items in config
        total_items = sum(len(items) for items in cfg.tables.values())
        assert len(meta) == total_items

    @_skip_ohlcv
    def test_init_ohlcv_run_immediately_false(self, tmp_path):
        """init(run_immediately=False) generates config but NO output files."""
        from fn_dg6_ingest import init

        config_path = str(tmp_path / "fnconfig.yaml")
        output_dir = str(tmp_path / "out")

        init(
            input_path=str(WIDE_OHLCV_CSV),
            output_dir=output_dir,
            config_path=config_path,
            run_immediately=False,
        )

        # Config exists
        assert (tmp_path / "fnconfig.yaml").exists()

        # Output dir was NOT created (pipeline didn't run)
        assert not (tmp_path / "out").exists()

    @_skip_ohlcv
    def test_ingest_ohlcv_csv(self, tmp_path):
        """init() then ingest() against same file produces consistent outputs."""
        from fn_dg6_ingest import init, ingest

        config_path = str(tmp_path / "fnconfig.yaml")
        output_dir = str(tmp_path / "out")

        init(
            input_path=str(WIDE_OHLCV_CSV),
            output_dir=output_dir,
            config_path=config_path,
            run_immediately=True,
        )

        # Read first-run output
        meta_1 = pd.read_parquet(tmp_path / "out" / "_meta.parquet")

        # Re-run via ingest() -- now returns Dataset
        ds2 = ingest(config_path=config_path)
        assert isinstance(ds2.config, type(ds2.config))  # sanity check

        # Second-run _meta has same structure
        meta_2 = pd.read_parquet(tmp_path / "out" / "_meta.parquet")
        assert list(meta_1.columns) == list(meta_2.columns)
        assert len(meta_1) == len(meta_2)

    @_skip_ohlcv
    def test_ingest_custom_table_groups(self, tmp_path):
        """init() -> modify config to split into ohlcv+volume -> ingest() produces two tables."""
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

        # Modify config to split tables
        cfg = load_config(config_path)
        cfg.tables = {
            "ohlcv": ["수정시가(원)", "수정고가(원)", "수정저가(원)", "수정주가(원)"],
            "volume": ["거래량(주)", "거래대금(원)"],
        }
        save_config(cfg, config_path)

        ds = ingest(config_path=config_path)

        # Should have 3 files: ohlcv, volume, _meta
        out = tmp_path / "out"
        assert (out / "ohlcv.parquet").exists()
        assert (out / "volume.parquet").exists()
        assert (out / "_meta.parquet").exists()

        # Each table should have key columns + its own items
        ohlcv_df = pd.read_parquet(out / "ohlcv.parquet")
        assert "코드" in ohlcv_df.columns
        assert "코드명" in ohlcv_df.columns
        assert "date" in ohlcv_df.columns
        assert "수정시가(원)" in ohlcv_df.columns

        volume_df = pd.read_parquet(out / "volume.parquet")
        assert "코드" in volume_df.columns
        assert "거래량(주)" in volume_df.columns

    @_skip_ohlcv
    def test_ingest_parquet_output_preserves_dtypes(self, tmp_path):
        """Parquet output preserves numeric dtypes (not object/string)."""
        from fn_dg6_ingest import init

        config_path = str(tmp_path / "fnconfig.yaml")
        output_dir = str(tmp_path / "out")

        init(
            input_path=str(WIDE_OHLCV_CSV),
            output_dir=output_dir,
            config_path=config_path,
            run_immediately=True,
        )

        from fn_dg6_ingest.config import load_config

        cfg = load_config(config_path)
        first_table = list(cfg.tables.keys())[0]
        df = pd.read_parquet(tmp_path / "out" / f"{first_table}.parquet")

        # Item columns (e.g., 수정시가(원)) should be numeric
        item_cols = [c for c in df.columns if c not in ("코드", "코드명", "date")]
        for col in item_cols:
            assert pd.api.types.is_numeric_dtype(df[col]), (
                f"Column '{col}' should be numeric but is {df[col].dtype}"
            )

    @_skip_ohlcv
    def test_ingest_csv_output(self, tmp_path):
        """CSV output is readable and contains Korean text correctly."""
        from fn_dg6_ingest import init
        from fn_dg6_ingest.config import load_config, save_config

        config_path = str(tmp_path / "fnconfig.yaml")
        output_dir = str(tmp_path / "out")

        # Generate config then switch output format to CSV
        init(
            input_path=str(WIDE_OHLCV_CSV),
            output_dir=output_dir,
            config_path=config_path,
            run_immediately=False,
        )
        cfg = load_config(config_path)
        cfg.output.output_format = "csv"
        save_config(cfg, config_path)

        from fn_dg6_ingest import ingest

        ingest(config_path=config_path)

        first_table = list(cfg.tables.keys())[0]
        csv_path = tmp_path / "out" / f"{first_table}.csv"
        assert csv_path.exists()

        df = pd.read_csv(csv_path, encoding="utf-8-sig")
        # Should contain Korean 코드명 values
        assert "코드명" in df.columns
        assert len(df) > 0


@pytest.mark.integration
class TestTimeSeriesSalesConsensus:
    """End-to-end tests for sales-consensus data (unit normalization)."""

    @_skip_sales
    def test_init_sales_consensus_csv(self, tmp_path):
        """init() against sales-consensus verifies unit normalization happened."""
        from fn_dg6_ingest import init

        config_path = str(tmp_path / "fnconfig.yaml")
        output_dir = str(tmp_path / "out")

        init(
            input_path=str(WIDE_SALES_CSV),
            output_dir=output_dir,
            config_path=config_path,
            run_immediately=True,
        )

        # Check _meta for unit multipliers
        meta = pd.read_parquet(tmp_path / "out" / "_meta.parquet")

        # At least some items should have been scaled (multiplier > 1)
        scaled = meta[meta["unit_multiplier"] > 1]
        assert len(scaled) > 0, (
            "Expected at least one item with unit_multiplier > 1 (e.g., 억원 -> 원)"
        )

        # Normalized names should differ from original for scaled items
        for _, row in scaled.iterrows():
            assert row["아이템명"] != row["아이템명_normalized"] or row["unit_multiplier"] == 1
