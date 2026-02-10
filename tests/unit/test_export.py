"""
Unit tests for the exporter (fn_dg6_ingest.export).

Tests CSV and Parquet export, directory creation, error handling,
and round-trip fidelity using pytest's tmp_path fixture.
"""

from __future__ import annotations

import pandas as pd
import pytest

from fn_dg6_ingest.exceptions import ExportError
from fn_dg6_ingest.export import export_tables


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_tables() -> dict[str, pd.DataFrame]:
    """Build a pair of small DataFrames for testing."""
    return {
        "ohlcv": pd.DataFrame({
            "코드": ["A001", "A001"],
            "코드명": ["삼성전자", "삼성전자"],
            "date": ["2024-01-01", "2024-01-02"],
            "수정시가(원)": [25200.0, 25400.0],
        }),
        "volume": pd.DataFrame({
            "코드": ["A001", "A001"],
            "코드명": ["삼성전자", "삼성전자"],
            "date": ["2024-01-01", "2024-01-02"],
            "거래량(주)": [306939.0, 310000.0],
        }),
    }


def _make_meta() -> pd.DataFrame:
    """Build a small _meta DataFrame for testing."""
    return pd.DataFrame({
        "table_name": ["ohlcv", "volume"],
        "source_file": ["test.csv", "test.csv"],
        "아이템명": ["수정시가(원)", "거래량(주)"],
    })


# ---------------------------------------------------------------------------
# CSV export tests
# ---------------------------------------------------------------------------

class TestExportCSV:
    """Tests for CSV export."""

    def test_basic_csv_export(self, tmp_path):
        """All tables + _meta are written as CSV files."""
        tables = _make_tables()
        meta_df = _make_meta()
        paths = export_tables(tables, meta_df, tmp_path, output_format="csv")

        assert len(paths) == 3  # ohlcv + volume + _meta
        for p in paths:
            assert p.endswith(".csv")

        # Files should exist on disk
        assert (tmp_path / "ohlcv.csv").exists()
        assert (tmp_path / "volume.csv").exists()
        assert (tmp_path / "_meta.csv").exists()

    def test_csv_round_trip(self, tmp_path):
        """Data survives a write-read round trip (CSV)."""
        tables = _make_tables()
        meta_df = _make_meta()
        export_tables(tables, meta_df, tmp_path, output_format="csv")

        loaded = pd.read_csv(tmp_path / "ohlcv.csv", encoding="utf-8-sig")
        assert list(loaded.columns) == ["코드", "코드명", "date", "수정시가(원)"]
        assert len(loaded) == 2
        assert loaded["수정시가(원)"].iloc[0] == 25200.0

    def test_meta_csv_round_trip(self, tmp_path):
        """_meta table survives CSV round trip."""
        tables = _make_tables()
        meta_df = _make_meta()
        export_tables(tables, meta_df, tmp_path, output_format="csv")

        loaded = pd.read_csv(tmp_path / "_meta.csv", encoding="utf-8-sig")
        assert len(loaded) == 2
        assert list(loaded["table_name"]) == ["ohlcv", "volume"]


# ---------------------------------------------------------------------------
# Parquet export tests
# ---------------------------------------------------------------------------

class TestExportParquet:
    """Tests for Parquet export."""

    def test_basic_parquet_export(self, tmp_path):
        """All tables + _meta are written as Parquet files."""
        tables = _make_tables()
        meta_df = _make_meta()
        paths = export_tables(tables, meta_df, tmp_path, output_format="parquet")

        assert len(paths) == 3
        for p in paths:
            assert p.endswith(".parquet")

        assert (tmp_path / "ohlcv.parquet").exists()
        assert (tmp_path / "volume.parquet").exists()
        assert (tmp_path / "_meta.parquet").exists()

    def test_parquet_round_trip(self, tmp_path):
        """Data survives a write-read round trip (Parquet)."""
        tables = _make_tables()
        meta_df = _make_meta()
        export_tables(tables, meta_df, tmp_path, output_format="parquet")

        loaded = pd.read_parquet(tmp_path / "ohlcv.parquet")
        assert list(loaded.columns) == ["코드", "코드명", "date", "수정시가(원)"]
        assert len(loaded) == 2
        assert loaded["수정시가(원)"].iloc[0] == 25200.0

    def test_parquet_preserves_dtypes(self, tmp_path):
        """Parquet preserves numeric dtypes (no string re-parsing needed)."""
        tables = {"data": pd.DataFrame({"value": [1.5, 2.5, 3.5]})}
        meta_df = _make_meta()
        export_tables(tables, meta_df, tmp_path, output_format="parquet")

        loaded = pd.read_parquet(tmp_path / "data.parquet")
        assert loaded["value"].dtype.kind == "f"  # float


# ---------------------------------------------------------------------------
# Directory creation and return values
# ---------------------------------------------------------------------------

class TestExportDirectoryAndPaths:
    """Tests for output directory creation and returned paths."""

    def test_creates_output_dir(self, tmp_path):
        """Output directory is created if it doesn't exist."""
        nested = tmp_path / "a" / "b" / "c"
        assert not nested.exists()

        tables = {"t1": pd.DataFrame({"x": [1]})}
        meta_df = _make_meta()
        export_tables(tables, meta_df, nested, output_format="csv")

        assert nested.exists()
        assert (nested / "t1.csv").exists()

    def test_return_paths_order(self, tmp_path):
        """Return value lists data tables first, then _meta last."""
        tables = _make_tables()
        meta_df = _make_meta()
        paths = export_tables(tables, meta_df, tmp_path, output_format="csv")

        # Last path should be _meta
        assert paths[-1].endswith("_meta.csv")

        # First paths should be data tables
        data_paths = paths[:-1]
        assert len(data_paths) == 2

    def test_single_table(self, tmp_path):
        """Works correctly with a single data table."""
        tables = {"default": pd.DataFrame({"v": [1, 2]})}
        meta_df = _make_meta()
        paths = export_tables(tables, meta_df, tmp_path, output_format="csv")
        assert len(paths) == 2  # default + _meta

    def test_default_format_is_parquet(self, tmp_path):
        """Default output format should be parquet."""
        tables = {"t": pd.DataFrame({"v": [1]})}
        meta_df = _make_meta()
        paths = export_tables(tables, meta_df, tmp_path)
        assert all(p.endswith(".parquet") for p in paths)


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestExportErrors:
    """Tests for export error handling."""

    def test_unsupported_format(self, tmp_path):
        """Unsupported output format raises ExportError."""
        tables = {"t": pd.DataFrame({"v": [1]})}
        meta_df = _make_meta()
        with pytest.raises(ExportError, match="Unsupported output format"):
            export_tables(tables, meta_df, tmp_path, output_format="xlsx")

    def test_empty_tables(self, tmp_path):
        """Empty tables dict still writes _meta."""
        meta_df = _make_meta()
        paths = export_tables({}, meta_df, tmp_path, output_format="csv")
        assert len(paths) == 1
        assert paths[0].endswith("_meta.csv")

    def test_string_output_dir(self, tmp_path):
        """output_dir can be a string (not just Path)."""
        tables = {"t": pd.DataFrame({"v": [1]})}
        meta_df = _make_meta()
        paths = export_tables(tables, meta_df, str(tmp_path), output_format="csv")
        assert len(paths) == 2
