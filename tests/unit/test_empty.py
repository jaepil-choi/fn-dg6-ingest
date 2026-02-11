"""
Unit tests for empty entity dropping (fn_dg6_ingest.transforms.empty).

Tests that entities with all-null data are correctly identified and dropped,
and that entity counts are accurate.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from fn_dg6_ingest.transforms.empty import DropResult, drop_empty_entities


class TestDropEmptyEntities:
    """Tests for drop_empty_entities()."""

    # -----------------------------------------------------------------
    # Basic drop / keep logic
    # -----------------------------------------------------------------

    def test_entity_with_all_nan_is_dropped(self):
        """An entity where every value column is NaN should be dropped."""
        df = pd.DataFrame({
            "코드": ["A001", "A001", "A002", "A002"],
            "date": ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"],
            "price": [100.0, 200.0, np.nan, np.nan],
            "volume": [10.0, 20.0, np.nan, np.nan],
        })
        result = drop_empty_entities(df)
        assert result.entities_total == 2
        assert result.entities_dropped == 1
        assert set(result.df["코드"].unique()) == {"A001"}

    def test_entity_with_some_data_is_kept(self):
        """An entity with at least one non-null value should be kept."""
        df = pd.DataFrame({
            "코드": ["A001", "A001", "A002", "A002"],
            "date": ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"],
            "price": [100.0, np.nan, np.nan, 50.0],
            "volume": [np.nan, np.nan, np.nan, np.nan],
        })
        result = drop_empty_entities(df)
        assert result.entities_total == 2
        assert result.entities_dropped == 0
        assert set(result.df["코드"].unique()) == {"A001", "A002"}

    def test_all_entities_empty(self):
        """If all entities are empty, result has zero rows."""
        df = pd.DataFrame({
            "코드": ["A001", "A002"],
            "date": ["2024-01-01", "2024-01-01"],
            "price": [np.nan, np.nan],
        })
        result = drop_empty_entities(df)
        assert result.entities_total == 2
        assert result.entities_dropped == 2
        assert len(result.df) == 0

    def test_no_entities_dropped(self):
        """When every entity has data, nothing is dropped."""
        df = pd.DataFrame({
            "코드": ["A001", "A002"],
            "date": ["2024-01-01", "2024-01-01"],
            "price": [100.0, 200.0],
        })
        result = drop_empty_entities(df)
        assert result.entities_total == 2
        assert result.entities_dropped == 0
        assert len(result.df) == 2

    # -----------------------------------------------------------------
    # Entity counts
    # -----------------------------------------------------------------

    def test_entity_counts_correct(self):
        """DropResult should report accurate total and dropped counts."""
        df = pd.DataFrame({
            "코드": ["A001"] * 3 + ["A002"] * 3 + ["A003"] * 3,
            "date": ["d1", "d2", "d3"] * 3,
            "price": [1, 2, 3, np.nan, np.nan, np.nan, np.nan, np.nan, 10],
        })
        result = drop_empty_entities(df)
        assert result.entities_total == 3
        assert result.entities_dropped == 1  # A002 dropped
        assert len(result.df) == 6  # A001 (3) + A003 (3)

    # -----------------------------------------------------------------
    # Custom parameters
    # -----------------------------------------------------------------

    def test_custom_entity_column(self):
        """Should work with a different entity column name."""
        df = pd.DataFrame({
            "ticker": ["AAPL", "AAPL", "GOOG", "GOOG"],
            "date": ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"],
            "price": [150.0, 151.0, np.nan, np.nan],
        })
        result = drop_empty_entities(df, entity_column="ticker")
        assert result.entities_total == 2
        assert result.entities_dropped == 1
        assert set(result.df["ticker"].unique()) == {"AAPL"}

    def test_explicit_value_columns(self):
        """When value_columns is specified, only those columns are checked."""
        df = pd.DataFrame({
            "코드": ["A001", "A002"],
            "date": ["2024-01-01", "2024-01-01"],
            "price": [np.nan, np.nan],
            "volume": [100.0, np.nan],
        })
        # Only check 'price' -- both are NaN, so both would be dropped
        result = drop_empty_entities(df, value_columns=["price"])
        assert result.entities_dropped == 2

        # Check 'volume' -- A001 has data
        result2 = drop_empty_entities(df, value_columns=["volume"])
        assert result2.entities_dropped == 1

    # -----------------------------------------------------------------
    # Edge cases
    # -----------------------------------------------------------------

    def test_missing_entity_column(self):
        """If entity_column is not in df, returns as-is with zero counts."""
        df = pd.DataFrame({
            "ticker": ["AAPL"],
            "price": [100.0],
        })
        result = drop_empty_entities(df, entity_column="코드")
        assert result.entities_total == 0
        assert result.entities_dropped == 0
        assert len(result.df) == 1

    def test_no_numeric_columns(self):
        """If there are no numeric columns and value_columns=None, no drop."""
        df = pd.DataFrame({
            "코드": ["A001", "A002"],
            "name": ["삼성전자", "SK하이닉스"],
        })
        result = drop_empty_entities(df)
        assert result.entities_total == 2
        assert result.entities_dropped == 0

    def test_resets_index(self):
        """The returned DataFrame should have a clean integer index."""
        df = pd.DataFrame({
            "코드": ["A001", "A001", "A002", "A002"],
            "date": ["d1", "d2", "d1", "d2"],
            "price": [1.0, 2.0, np.nan, np.nan],
        })
        result = drop_empty_entities(df)
        assert list(result.df.index) == [0, 1]
