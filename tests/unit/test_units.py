"""
Unit tests for unit normalization (fn_dg6_ingest.transforms.units).

Tests unit suffix detection, scaling, and column renaming.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from fn_dg6_ingest.transforms.units import (
    detect_unit,
    normalize_column_name,
    normalize_units,
)


class TestDetectUnit:
    """Tests for detect_unit()."""

    def test_won_base_unit(self):
        """(원) is the base monetary unit -- multiplier 1."""
        unit, mult = detect_unit("수정시가(원)")
        assert unit == "원"
        assert mult == 1

    def test_cheonwon(self):
        """(천원) -> multiplier 1,000."""
        unit, mult = detect_unit("총매출(천원)")
        assert unit == "천원"
        assert mult == 1_000

    def test_baekmanwon(self):
        """(백만원) -> multiplier 1,000,000."""
        unit, mult = detect_unit("영업이익(백만원)")
        assert unit == "백만원"
        assert mult == 1_000_000

    def test_eokwon(self):
        """(억원) -> multiplier 100,000,000."""
        unit, mult = detect_unit("매출액(억원)")
        assert unit == "억원"
        assert mult == 100_000_000

    def test_sipeoukwon(self):
        """(십억원) -> multiplier 1,000,000,000."""
        unit, mult = detect_unit("자산총계(십억원)")
        assert unit == "십억원"
        assert mult == 1_000_000_000

    def test_non_monetary_ju(self):
        """(주) is non-monetary -- no scaling."""
        unit, mult = detect_unit("거래량(주)")
        assert unit is None
        assert mult == 1

    def test_non_monetary_percent(self):
        """(%) is non-monetary -- no scaling."""
        unit, mult = detect_unit("금액기준 구성비중(%)")
        assert unit is None
        assert mult == 1

    def test_non_monetary_gyeyaksu(self):
        """(계약수) is non-monetary -- no scaling."""
        unit, mult = detect_unit("주식수(계약수)")
        assert unit is None
        assert mult == 1

    def test_no_suffix(self):
        """No parenthesized suffix at all."""
        unit, mult = detect_unit("추정기관수")
        assert unit is None
        assert mult == 1

    def test_no_suffix_date_column(self):
        """Date column has no suffix."""
        unit, mult = detect_unit("date")
        assert unit is None
        assert mult == 1

    def test_nested_parentheses_eokwon(self):
        """Nested parentheses like 매출액(최고)(억원) -- last suffix wins."""
        unit, mult = detect_unit("매출액(최고)(억원)")
        assert unit == "억원"
        assert mult == 100_000_000

    def test_fwd_with_eokwon(self):
        """Items like 매출액(Fwd.12M)(억원)."""
        unit, mult = detect_unit("매출액(Fwd.12M)(억원)")
        assert unit == "억원"
        assert mult == 100_000_000


class TestNormalizeColumnName:
    """Tests for normalize_column_name()."""

    def test_rename_cheonwon_to_won(self):
        assert normalize_column_name("총매출(천원)", "천원") == "총매출(원)"

    def test_rename_eokwon_to_won(self):
        assert normalize_column_name("매출액(억원)", "억원") == "매출액(원)"

    def test_rename_nested_parentheses(self):
        assert normalize_column_name("매출액(최고)(억원)", "억원") == "매출액(최고)(원)"

    def test_rename_sipeoukwon(self):
        assert normalize_column_name("자산총계(십억원)", "십억원") == "자산총계(원)"


class TestNormalizeUnits:
    """Tests for normalize_units()."""

    def test_scales_eokwon_column(self):
        """매출액(억원) values should be multiplied by 100,000,000."""
        df = pd.DataFrame({
            "코드": ["A005930"],
            "매출액(억원)": [100.0],
        })
        result_df, unit_info = normalize_units(df)
        # 100 * 100,000,000 = 10,000,000,000
        assert result_df["매출액(원)"].iloc[0] == 10_000_000_000
        assert "매출액(억원)" not in result_df.columns
        assert "매출액(억원)" in unit_info
        assert unit_info["매출액(억원)"] == ("억원", 100_000_000)

    def test_base_unit_not_scaled(self):
        """(원) columns are recorded in unit_info but not scaled."""
        df = pd.DataFrame({
            "코드": ["A005930"],
            "수정시가(원)": [25200.0],
        })
        result_df, unit_info = normalize_units(df)
        assert result_df["수정시가(원)"].iloc[0] == 25200.0
        assert "수정시가(원)" in unit_info
        assert unit_info["수정시가(원)"] == ("원", 1)

    def test_non_monetary_untouched(self):
        """(주) columns should not appear in unit_info."""
        df = pd.DataFrame({
            "코드": ["A005930"],
            "거래량(주)": [306939.0],
        })
        result_df, unit_info = normalize_units(df)
        assert result_df["거래량(주)"].iloc[0] == 306939.0
        assert "거래량(주)" not in unit_info

    def test_mixed_columns(self):
        """Mix of monetary and non-monetary columns."""
        df = pd.DataFrame({
            "코드": ["A005930"],
            "매출액(억원)": [100.0],
            "수정시가(원)": [25200.0],
            "거래량(주)": [306939.0],
        })
        result_df, unit_info = normalize_units(df)
        # 매출액 should be scaled and renamed
        assert "매출액(원)" in result_df.columns
        assert "매출액(억원)" not in result_df.columns
        # 수정시가 should stay as-is
        assert result_df["수정시가(원)"].iloc[0] == 25200.0
        # 거래량 should stay as-is, not in unit_info
        assert result_df["거래량(주)"].iloc[0] == 306939.0
        assert "거래량(주)" not in unit_info

    def test_nan_values_preserved(self):
        """NaN values should remain NaN after scaling."""
        df = pd.DataFrame({
            "매출액(억원)": [100.0, float("nan")],
        })
        result_df, _ = normalize_units(df)
        assert result_df["매출액(원)"].iloc[0] == 10_000_000_000
        assert pd.isna(result_df["매출액(원)"].iloc[1])

    def test_does_not_mutate_input(self):
        """normalize_units should return a copy."""
        df = pd.DataFrame({
            "매출액(억원)": [100.0],
        })
        original = df["매출액(억원)"].iloc[0]
        _, _ = normalize_units(df)
        assert df["매출액(억원)"].iloc[0] == original

    def test_no_monetary_columns(self):
        """DataFrame with no monetary columns returns empty unit_info."""
        df = pd.DataFrame({
            "코드": ["A005930"],
            "추정기관수": [10.0],
        })
        result_df, unit_info = normalize_units(df)
        assert len(unit_info) == 0
        assert result_df["추정기관수"].iloc[0] == 10.0

    def test_multiple_eokwon_columns(self):
        """Multiple (억원) columns should each be independently scaled."""
        df = pd.DataFrame({
            "매출액(억원)": [100.0],
            "매출액(최고)(억원)": [120.0],
        })
        result_df, unit_info = normalize_units(df)
        assert "매출액(원)" in result_df.columns
        assert "매출액(최고)(원)" in result_df.columns
        assert result_df["매출액(원)"].iloc[0] == 10_000_000_000
        assert result_df["매출액(최고)(원)"].iloc[0] == 12_000_000_000
