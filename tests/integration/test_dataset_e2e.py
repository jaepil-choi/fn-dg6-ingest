"""
Integration tests: Dataset handle end-to-end.

Tests the full Dataset lifecycle using ``fn_dg6_ingest.open()`` and
the ``Dataset`` methods (``load``, ``load_meta``, ``describe``,
``ingest``, ``save_config``).

Uses real input files (OHLCV CSV and ETF CSV) with ``tmp_path`` for
output to avoid polluting the workspace.
"""

from __future__ import annotations

import pandas as pd
import pytest

from fn_dg6_ingest.dataset import Dataset, DatasetInfo
from tests.conftest import LONG_ETF_CSV, WIDE_OHLCV_CSV

_skip_ohlcv = pytest.mark.skipif(
    not WIDE_OHLCV_CSV.exists(),
    reason=f"Input file not found: {WIDE_OHLCV_CSV}",
)
_skip_etf = pytest.mark.skipif(
    not LONG_ETF_CSV.exists(),
    reason=f"Input file not found: {LONG_ETF_CSV}",
)


# ---------------------------------------------------------------------------
# OHLCV time series tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestDatasetOhlcv:
    """Dataset end-to-end tests against OHLCV time series data."""

    @_skip_ohlcv
    def test_open_source_file(self, tmp_path):
        """open(source_csv) returns Dataset with config and output files."""
        import fn_dg6_ingest

        config_path = str(tmp_path / "ohlcv.yaml")
        output_dir = str(tmp_path / "out")

        ds = fn_dg6_ingest.open(
            str(WIDE_OHLCV_CSV),
            output_dir=output_dir,
            config_path=config_path,
        )

        assert isinstance(ds, Dataset)
        assert ds.config.source.detected_format == "timeseries_wide"
        assert ds.config_path.exists()
        assert (tmp_path / "out" / "default.parquet").exists()
        assert (tmp_path / "out" / "_meta.parquet").exists()

    @_skip_ohlcv
    def test_open_config_yaml(self, tmp_path):
        """open(yaml_path) loads existing config and enables load()."""
        import fn_dg6_ingest

        config_path = str(tmp_path / "ohlcv.yaml")
        output_dir = str(tmp_path / "out")

        # First, create the dataset
        fn_dg6_ingest.open(
            str(WIDE_OHLCV_CSV),
            output_dir=output_dir,
            config_path=config_path,
        )

        # Now open from config
        ds2 = fn_dg6_ingest.open(config_path)
        assert isinstance(ds2, Dataset)
        assert ds2.config.source.detected_format == "timeseries_wide"

        # load() should work
        df = ds2.load()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    @_skip_ohlcv
    def test_load_full(self, tmp_path):
        """ds.load() returns full DataFrame with all rows and columns."""
        import fn_dg6_ingest

        ds = fn_dg6_ingest.open(
            str(WIDE_OHLCV_CSV),
            output_dir=str(tmp_path / "out"),
            config_path=str(tmp_path / "ohlcv.yaml"),
        )

        df = ds.load()
        assert isinstance(df, pd.DataFrame)
        assert "코드" in df.columns
        assert "코드명" in df.columns
        assert "date" in df.columns
        # Should have at least thousands of rows (OHLCV has ~7.6M)
        assert len(df) > 1000

    @_skip_ohlcv
    def test_load_filter_codes(self, tmp_path):
        """ds.load(codes=...) returns only matching entities."""
        import fn_dg6_ingest

        ds = fn_dg6_ingest.open(
            str(WIDE_OHLCV_CSV),
            output_dir=str(tmp_path / "out"),
            config_path=str(tmp_path / "ohlcv.yaml"),
        )

        df = ds.load(codes=["A005930"])
        assert len(df) > 0
        assert set(df["코드"].unique()) == {"A005930"}
        assert "삼성전자" in df["코드명"].values

    @_skip_ohlcv
    def test_load_filter_date_range(self, tmp_path):
        """ds.load(date_from=..., date_to=...) narrows to date range."""
        import fn_dg6_ingest

        ds = fn_dg6_ingest.open(
            str(WIDE_OHLCV_CSV),
            output_dir=str(tmp_path / "out"),
            config_path=str(tmp_path / "ohlcv.yaml"),
        )

        df = ds.load(date_from="2025-01-01", date_to="2025-12-31")
        assert len(df) > 0
        assert all(d >= "2025-01-01" for d in df["date"])
        assert all(d <= "2025-12-31" for d in df["date"])

    @_skip_ohlcv
    def test_load_filter_items(self, tmp_path):
        """ds.load(items=...) selects only specified value columns."""
        import fn_dg6_ingest

        ds = fn_dg6_ingest.open(
            str(WIDE_OHLCV_CSV),
            output_dir=str(tmp_path / "out"),
            config_path=str(tmp_path / "ohlcv.yaml"),
        )

        df = ds.load(items=["수정주가(원)"])
        assert "수정주가(원)" in df.columns
        # Key columns should always be present
        assert "코드" in df.columns
        assert "date" in df.columns
        # Other item columns should be absent
        assert "거래량(주)" not in df.columns

    @_skip_ohlcv
    def test_load_combined_filters(self, tmp_path):
        """ds.load() with codes + date + items filters applied together."""
        import fn_dg6_ingest

        ds = fn_dg6_ingest.open(
            str(WIDE_OHLCV_CSV),
            output_dir=str(tmp_path / "out"),
            config_path=str(tmp_path / "ohlcv.yaml"),
        )

        df = ds.load(
            codes=["A005930", "A000660"],
            date_from="2024-01-01",
            items=["수정주가(원)"],
        )
        assert len(df) > 0
        assert set(df["코드"].unique()).issubset({"A005930", "A000660"})
        assert all(d >= "2024-01-01" for d in df["date"])
        assert set(df.columns) == {"코드", "코드명", "date", "수정주가(원)"}

    @_skip_ohlcv
    def test_load_meta(self, tmp_path):
        """ds.load_meta() returns the _meta table with all 20 FR-9 columns."""
        import fn_dg6_ingest

        ds = fn_dg6_ingest.open(
            str(WIDE_OHLCV_CSV),
            output_dir=str(tmp_path / "out"),
            config_path=str(tmp_path / "ohlcv.yaml"),
        )

        meta = ds.load_meta()
        assert isinstance(meta, pd.DataFrame)
        assert len(meta.columns) == 20
        assert "table_name" in meta.columns
        assert "아이템명" in meta.columns
        assert "source_hash" in meta.columns

    @_skip_ohlcv
    def test_describe(self, tmp_path):
        """ds.describe() returns a DatasetInfo with correct structure."""
        import fn_dg6_ingest

        ds = fn_dg6_ingest.open(
            str(WIDE_OHLCV_CSV),
            output_dir=str(tmp_path / "out"),
            config_path=str(tmp_path / "ohlcv.yaml"),
        )

        info = ds.describe()
        assert isinstance(info, DatasetInfo)
        assert info.format_name == "timeseries_wide"
        assert "default" in info.tables
        assert "default" in info.shape
        assert info.shape["default"][0] > 1000  # many rows
        assert info.shape["default"][1] > 3  # key cols + items
        assert info.date_range is not None
        assert info.entities is not None
        assert info.entities > 0
        assert info.output_format == "parquet"

    @_skip_ohlcv
    def test_ingest_rebuild(self, tmp_path):
        """Modify config -> save_config() -> ingest() -> verify rebuild."""
        import fn_dg6_ingest

        ds = fn_dg6_ingest.open(
            str(WIDE_OHLCV_CSV),
            output_dir=str(tmp_path / "out"),
            config_path=str(tmp_path / "ohlcv.yaml"),
        )

        # Split into two tables
        ds.config.tables = {
            "ohlcv": ["수정시가(원)", "수정고가(원)", "수정저가(원)", "수정주가(원)"],
            "volume": ["거래량(주)", "거래대금(원)"],
        }
        ds.save_config()

        written = ds.ingest()
        assert len(written) == 3  # ohlcv, volume, _meta

        out = tmp_path / "out"
        assert (out / "ohlcv.parquet").exists()
        assert (out / "volume.parquet").exists()

        # Load the split tables
        ohlcv_df = ds.load(table="ohlcv")
        assert "수정주가(원)" in ohlcv_df.columns
        assert "거래량(주)" not in ohlcv_df.columns

        volume_df = ds.load(table="volume")
        assert "거래량(주)" in volume_df.columns


# ---------------------------------------------------------------------------
# ETF misc format tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestDatasetEtf:
    """Dataset end-to-end tests against ETF constituent data."""

    @_skip_etf
    def test_open_etf_source(self, tmp_path):
        """open() + load() works for misc ETF format."""
        import fn_dg6_ingest

        ds = fn_dg6_ingest.open(
            str(LONG_ETF_CSV),
            output_dir=str(tmp_path / "out"),
            config_path=str(tmp_path / "etf.yaml"),
        )

        assert isinstance(ds, Dataset)
        assert ds.config.source.detected_format == "misc_etf"

        df = ds.load()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    @_skip_etf
    def test_etf_describe(self, tmp_path):
        """describe() provides correct info for misc format."""
        import fn_dg6_ingest

        ds = fn_dg6_ingest.open(
            str(LONG_ETF_CSV),
            output_dir=str(tmp_path / "out"),
            config_path=str(tmp_path / "etf.yaml"),
        )

        info = ds.describe()
        assert info.format_name == "misc_etf"
        assert len(info.tables) > 0
        assert info.output_format == "parquet"

    @_skip_etf
    def test_etf_load_meta(self, tmp_path):
        """load_meta() works for ETF datasets."""
        import fn_dg6_ingest

        ds = fn_dg6_ingest.open(
            str(LONG_ETF_CSV),
            output_dir=str(tmp_path / "out"),
            config_path=str(tmp_path / "etf.yaml"),
        )

        meta = ds.load_meta()
        assert isinstance(meta, pd.DataFrame)
        assert "table_name" in meta.columns
        assert len(meta) > 0
