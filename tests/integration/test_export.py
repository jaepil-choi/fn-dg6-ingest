"""
Integration tests: export verification.

Tests that output files (CSV and Parquet) are correctly written,
loadable, and contain expected data.
"""

from __future__ import annotations

import pandas as pd
import pytest

from tests.conftest import WIDE_OHLCV_CSV

_skip_ohlcv = pytest.mark.skipif(
    not WIDE_OHLCV_CSV.exists(),
    reason=f"Input file not found: {WIDE_OHLCV_CSV}",
)


@pytest.mark.integration
class TestExportIntegration:
    """Tests for CSV and Parquet export via the full pipeline."""

    @_skip_ohlcv
    def test_meta_table_schema(self, tmp_path):
        """_meta output has all 20 FR-9 columns."""
        from fn_dg6_ingest import init

        config_path = str(tmp_path / "fnconfig.yaml")
        output_dir = str(tmp_path / "out")

        init(
            input_path=str(WIDE_OHLCV_CSV),
            output_dir=output_dir,
            config_path=config_path,
            run_immediately=True,
        )

        meta = pd.read_parquet(tmp_path / "out" / "_meta.parquet")
        assert len(meta.columns) == 20

        expected_cols = [
            "table_name", "source_file", "source_hash", "source_last_updated",
            "detected_format", "아이템코드", "아이템명", "아이템명_normalized",
            "유형", "집계주기", "frequency", "period_start", "period_end",
            "unit_original", "unit_multiplier", "non_business_days",
            "include_weekends", "entities_total", "entities_dropped",
            "processed_at",
        ]
        assert list(meta.columns) == expected_cols

    @_skip_ohlcv
    def test_meta_row_count_matches_items(self, tmp_path):
        """_meta row count == number of unique 아이템명 in the source."""
        from fn_dg6_ingest import init
        from fn_dg6_ingest.config import load_config
        from fn_dg6_ingest.detect import detect_format

        config_path = str(tmp_path / "fnconfig.yaml")
        output_dir = str(tmp_path / "out")

        init(
            input_path=str(WIDE_OHLCV_CSV),
            output_dir=output_dir,
            config_path=config_path,
            run_immediately=True,
        )

        # Count items from parser
        parser_cls, layout = detect_format(str(WIDE_OHLCV_CSV))
        parse_result = parser_cls().parse(str(WIDE_OHLCV_CSV), layout)

        meta = pd.read_parquet(tmp_path / "out" / "_meta.parquet")
        assert len(meta) == len(parse_result.items)

    @_skip_ohlcv
    def test_output_files_named_correctly(self, tmp_path):
        """Filenames match {table_name}.{format} convention."""
        from fn_dg6_ingest import init
        from fn_dg6_ingest.config import load_config

        config_path = str(tmp_path / "fnconfig.yaml")
        output_dir = str(tmp_path / "out")

        init(
            input_path=str(WIDE_OHLCV_CSV),
            output_dir=output_dir,
            config_path=config_path,
            run_immediately=True,
        )

        cfg = load_config(config_path)
        out = tmp_path / "out"
        fmt = cfg.output.output_format

        for table_name in cfg.tables:
            expected = out / f"{table_name}.{fmt}"
            assert expected.exists(), f"Expected output file: {expected}"

        assert (out / f"_meta.{fmt}").exists()

    @_skip_ohlcv
    def test_parquet_dtypes_numeric(self, tmp_path):
        """Numeric columns in Parquet output are float/int, not object."""
        from fn_dg6_ingest import init
        from fn_dg6_ingest.config import load_config

        config_path = str(tmp_path / "fnconfig.yaml")
        output_dir = str(tmp_path / "out")

        init(
            input_path=str(WIDE_OHLCV_CSV),
            output_dir=output_dir,
            config_path=config_path,
            run_immediately=True,
        )

        cfg = load_config(config_path)
        first_table = list(cfg.tables.keys())[0]
        df = pd.read_parquet(tmp_path / "out" / f"{first_table}.parquet")

        key_cols = {"코드", "코드명", "date"}
        for col in df.columns:
            if col not in key_cols:
                assert pd.api.types.is_numeric_dtype(df[col]), (
                    f"Column '{col}' should be numeric but is {df[col].dtype}"
                )
