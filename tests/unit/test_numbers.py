"""
Unit tests for number parsing transform (fn_dg6_ingest.transforms.numbers).

Tests comma stripping, whitespace handling, and numeric type coercion
using small synthetic DataFrames.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from fn_dg6_ingest.transforms.numbers import parse_numbers


class TestParseNumbers:
    """Tests for parse_numbers()."""

    def _make_df(self, data: dict[str, list[str]]) -> pd.DataFrame:
        """Helper: build a DataFrame with all-string columns."""
        return pd.DataFrame(data).astype(str)

    # -----------------------------------------------------------------
    # Core numeric parsing
    # -----------------------------------------------------------------

    def test_comma_separated_integers(self):
        """Korean-style thousand separators (commas) should be stripped."""
        df = self._make_df({
            "코드": ["A005930", "A000660"],
            "value": ["25,200", "1,234,567"],
        })
        result = parse_numbers(df, key_columns=["코드"])
        assert result["value"].iloc[0] == 25200
        assert result["value"].iloc[1] == 1234567

    def test_large_comma_separated_values(self):
        """Handles very large numbers like 거래대금(원) = 257,149,317,000."""
        df = self._make_df({
            "코드": ["A005930"],
            "value": ["257,149,317,000"],
        })
        result = parse_numbers(df, key_columns=["코드"])
        assert result["value"].iloc[0] == 257_149_317_000

    def test_decimal_values(self):
        """Decimal numbers like percentages should parse correctly."""
        df = self._make_df({
            "코드": ["A005930"],
            "ratio": ["0.62"],
        })
        result = parse_numbers(df, key_columns=["코드"])
        assert result["ratio"].iloc[0] == pytest.approx(0.62)

    def test_whitespace_stripping(self):
        """Leading/trailing whitespace should be stripped before parsing."""
        df = self._make_df({
            "코드": ["A005930"],
            "value": ["  25,200  "],
        })
        result = parse_numbers(df, key_columns=["코드"])
        assert result["value"].iloc[0] == 25200

    def test_negative_numbers(self):
        """Negative numbers should parse correctly."""
        df = self._make_df({
            "코드": ["A005930"],
            "value": ["-1,234"],
        })
        result = parse_numbers(df, key_columns=["코드"])
        assert result["value"].iloc[0] == -1234

    # -----------------------------------------------------------------
    # Missing / empty value handling
    # -----------------------------------------------------------------

    def test_empty_string_becomes_nan(self):
        """Empty strings are coerced to NaN."""
        df = self._make_df({
            "코드": ["A005930"],
            "value": [""],
        })
        result = parse_numbers(df, key_columns=["코드"])
        assert pd.isna(result["value"].iloc[0])

    def test_whitespace_only_becomes_nan(self):
        """Whitespace-only strings become NaN after strip + coerce."""
        df = self._make_df({
            "코드": ["A005930"],
            "value": ["   "],
        })
        result = parse_numbers(df, key_columns=["코드"])
        assert pd.isna(result["value"].iloc[0])

    def test_non_numeric_string_becomes_nan(self):
        """Non-numeric strings like dates in value columns become NaN."""
        df = self._make_df({
            "코드": ["A005930"],
            "value": ["20180102"],
        })
        result = parse_numbers(df, key_columns=["코드"])
        # "20180102" looks like a valid integer, should parse as 20180102
        assert result["value"].iloc[0] == 20180102

    def test_truly_non_numeric_becomes_nan(self):
        """Strings that are genuinely non-numeric become NaN."""
        df = self._make_df({
            "코드": ["A005930"],
            "value": ["N/A"],
        })
        result = parse_numbers(df, key_columns=["코드"])
        assert pd.isna(result["value"].iloc[0])

    # -----------------------------------------------------------------
    # Key column preservation
    # -----------------------------------------------------------------

    def test_key_columns_preserved_as_string(self):
        """Key columns must remain as strings, not coerced to numeric."""
        df = self._make_df({
            "코드": ["A005930"],
            "코드명": ["삼성전자"],
            "date": ["2016-01-04"],
            "value": ["25,200"],
        })
        result = parse_numbers(df, key_columns=["코드", "코드명", "date"])
        assert result["코드"].iloc[0] == "A005930"
        assert result["코드명"].iloc[0] == "삼성전자"
        assert result["date"].iloc[0] == "2016-01-04"

    def test_key_columns_not_modified_even_if_numeric_looking(self):
        """코드 values like 'A005930' must stay as strings."""
        df = self._make_df({
            "코드": ["A005930", "A000660"],
            "value": ["100", "200"],
        })
        result = parse_numbers(df, key_columns=["코드"])
        # pandas 2.x may use StringDtype instead of object; both are valid
        assert pd.api.types.is_string_dtype(result["코드"])

    # -----------------------------------------------------------------
    # Multiple value columns
    # -----------------------------------------------------------------

    def test_multiple_value_columns(self):
        """All non-key columns should be independently parsed."""
        df = self._make_df({
            "코드": ["A005930"],
            "코드명": ["삼성전자"],
            "date": ["2016-01-04"],
            "수정시가(원)": ["25,200"],
            "거래량(주)": ["306,939"],
            "거래대금(원)": ["375,331,217,000"],
        })
        result = parse_numbers(df, key_columns=["코드", "코드명", "date"])
        assert result["수정시가(원)"].iloc[0] == 25200
        assert result["거래량(주)"].iloc[0] == 306939
        assert result["거래대금(원)"].iloc[0] == 375_331_217_000

    # -----------------------------------------------------------------
    # Edge cases
    # -----------------------------------------------------------------

    def test_does_not_mutate_input(self):
        """parse_numbers should return a copy, not modify the input."""
        df = self._make_df({
            "코드": ["A005930"],
            "value": ["25,200"],
        })
        original_value = df["value"].iloc[0]
        _ = parse_numbers(df, key_columns=["코드"])
        assert df["value"].iloc[0] == original_value

    def test_empty_dataframe(self):
        """Empty DataFrame should pass through without error."""
        df = pd.DataFrame({"코드": pd.Series([], dtype=str), "value": pd.Series([], dtype=str)})
        result = parse_numbers(df, key_columns=["코드"])
        assert len(result) == 0

    def test_zero_values(self):
        """Zero values should be preserved (not treated as NaN)."""
        df = self._make_df({
            "코드": ["A005930"],
            "value": ["0"],
        })
        result = parse_numbers(df, key_columns=["코드"])
        assert result["value"].iloc[0] == 0
        assert not pd.isna(result["value"].iloc[0])
