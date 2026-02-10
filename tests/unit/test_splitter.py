"""
Unit tests for table splitter (fn_dg6_ingest.transforms.splitter).

Tests that DataFrames are correctly split into groups, key columns
are always included, and single-group returns the full DataFrame.
"""

from __future__ import annotations

import pandas as pd
import pytest

from fn_dg6_ingest.transforms.splitter import split_tables


class TestSplitTables:
    """Tests for split_tables()."""

    @pytest.fixture()
    def sample_df(self) -> pd.DataFrame:
        """A small pivoted DataFrame mimicking TimeSeriesWideParser output."""
        return pd.DataFrame({
            "코드": ["A001", "A001", "A002", "A002"],
            "코드명": ["삼성전자", "삼성전자", "SK하이닉스", "SK하이닉스"],
            "date": ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"],
            "수정시가(원)": [100.0, 101.0, 200.0, 201.0],
            "수정고가(원)": [105.0, 106.0, 210.0, 211.0],
            "거래량(주)": [1000.0, 1100.0, 2000.0, 2100.0],
            "거래대금(원)": [50000.0, 51000.0, 60000.0, 61000.0],
        })

    # -----------------------------------------------------------------
    # Single group (default behavior)
    # -----------------------------------------------------------------

    def test_single_default_group(self, sample_df: pd.DataFrame):
        """A single 'default' group should return all value columns."""
        groups = {
            "default": ["수정시가(원)", "수정고가(원)", "거래량(주)", "거래대금(원)"],
        }
        result = split_tables(sample_df, groups)
        assert len(result) == 1
        assert "default" in result
        df = result["default"]
        assert set(df.columns) == {
            "코드", "코드명", "date",
            "수정시가(원)", "수정고가(원)", "거래량(주)", "거래대금(원)",
        }
        assert len(df) == 4

    # -----------------------------------------------------------------
    # Multiple groups
    # -----------------------------------------------------------------

    def test_two_groups(self, sample_df: pd.DataFrame):
        """Two groups should each get their own columns + key columns."""
        groups = {
            "ohlcv": ["수정시가(원)", "수정고가(원)"],
            "volume": ["거래량(주)", "거래대금(원)"],
        }
        result = split_tables(sample_df, groups)
        assert len(result) == 2

        ohlcv = result["ohlcv"]
        assert set(ohlcv.columns) == {"코드", "코드명", "date", "수정시가(원)", "수정고가(원)"}

        volume = result["volume"]
        assert set(volume.columns) == {"코드", "코드명", "date", "거래량(주)", "거래대금(원)"}

    def test_groups_are_independent_copies(self, sample_df: pd.DataFrame):
        """Modifying one group's DataFrame should not affect the source."""
        groups = {
            "ohlcv": ["수정시가(원)"],
            "volume": ["거래량(주)"],
        }
        result = split_tables(sample_df, groups)
        result["ohlcv"].loc[result["ohlcv"].index[0], "수정시가(원)"] = -999
        assert sample_df["수정시가(원)"].iloc[0] == 100.0

    # -----------------------------------------------------------------
    # Key columns
    # -----------------------------------------------------------------

    def test_key_columns_always_present(self, sample_df: pd.DataFrame):
        """Default key columns (코드, 코드명, date) should always be included."""
        groups = {"prices": ["수정시가(원)"]}
        result = split_tables(sample_df, groups)
        df = result["prices"]
        assert "코드" in df.columns
        assert "코드명" in df.columns
        assert "date" in df.columns

    def test_custom_key_columns(self, sample_df: pd.DataFrame):
        """Custom key_columns override the defaults."""
        groups = {"prices": ["수정시가(원)"]}
        result = split_tables(sample_df, groups, key_columns=["코드"])
        df = result["prices"]
        assert "코드" in df.columns
        assert "코드명" not in df.columns  # not in custom key_columns
        assert "date" not in df.columns

    # -----------------------------------------------------------------
    # Missing columns
    # -----------------------------------------------------------------

    def test_missing_column_skipped_with_warning(self, sample_df: pd.DataFrame):
        """Columns not in DataFrame are silently skipped (with logging)."""
        groups = {
            "prices": ["수정시가(원)", "존재하지않는컬럼"],
        }
        result = split_tables(sample_df, groups)
        df = result["prices"]
        assert "수정시가(원)" in df.columns
        assert "존재하지않는컬럼" not in df.columns

    def test_all_columns_missing_skips_group(self, sample_df: pd.DataFrame):
        """If all columns in a group are missing, that group is skipped."""
        groups = {
            "ghost": ["없는1", "없는2"],
            "prices": ["수정시가(원)"],
        }
        result = split_tables(sample_df, groups)
        assert "ghost" not in result
        assert "prices" in result

    # -----------------------------------------------------------------
    # Edge cases
    # -----------------------------------------------------------------

    def test_empty_table_groups(self, sample_df: pd.DataFrame):
        """Empty table_groups dict returns empty result."""
        result = split_tables(sample_df, {})
        assert result == {}

    def test_misc_format_key_columns(self):
        """Misc format DataFrames may have different key columns."""
        df = pd.DataFrame({
            "date": ["2025-01-02", "2025-01-03"],
            "ETF코드": ["A069500", "A069500"],
            "ETF명": ["KODEX 200", "KODEX 200"],
            "금액": [9945243.0, 9900000.0],
        })
        groups = {"default": ["금액"]}
        result = split_tables(df, groups, key_columns=["date", "ETF코드", "ETF명"])
        assert set(result["default"].columns) == {"date", "ETF코드", "ETF명", "금액"}

    def test_row_count_preserved(self, sample_df: pd.DataFrame):
        """Splitting should not change the number of rows."""
        groups = {"prices": ["수정시가(원)"]}
        result = split_tables(sample_df, groups)
        assert len(result["prices"]) == len(sample_df)
