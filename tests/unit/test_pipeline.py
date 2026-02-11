"""
Unit tests for TransformPipeline (fn_dg6_ingest.transforms.pipeline).

Tests the orchestration of all transform steps end-to-end using small
synthetic DataFrames and IngestConfig objects.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from fn_dg6_ingest.config import IngestConfig, MetadataConfig, OutputConfig, SourceConfig
from fn_dg6_ingest.transforms.pipeline import PipelineResult, TransformPipeline


def _make_config(
    *,
    tables: dict[str, list[str]] | None = None,
    normalize_units: bool = True,
    drop_empty_entities: bool = True,
) -> IngestConfig:
    """Build a minimal IngestConfig for testing."""
    if tables is None:
        tables = {"default": ["price"]}
    return IngestConfig(
        source=SourceConfig(input_path="test.csv", detected_format="timeseries_wide"),
        metadata=MetadataConfig(),
        output=OutputConfig(
            normalize_units=normalize_units,
            drop_empty_entities=drop_empty_entities,
        ),
        tables=tables,
    )


def _make_string_df() -> pd.DataFrame:
    """Build a synthetic wide-pivoted DataFrame with string values.

    Simulates what TimeSeriesWideParser produces (all dtypes are str).
    """
    return pd.DataFrame({
        "코드": ["A001", "A001", "A002", "A002", "A003", "A003"],
        "코드명": ["삼성전자", "삼성전자", "SK하이닉스", "SK하이닉스", "유령회사", "유령회사"],
        "date": ["2024-01-01", "2024-01-02"] * 3,
        "수정시가(원)": ["25,200", "25,400", "100,000", "101,000", "", ""],
        "거래량(주)": ["306,939", "310,000", "50,000", "51,000", "", ""],
    }).astype(str)


class TestTransformPipeline:
    """Tests for TransformPipeline.run()."""

    # -----------------------------------------------------------------
    # Full pipeline (all steps enabled)
    # -----------------------------------------------------------------

    def test_full_pipeline(self):
        """Full pipeline: parse numbers, normalize units, drop empty, split."""
        config = _make_config(
            tables={"default": ["수정시가(원)", "거래량(주)"]},
        )
        pipeline = TransformPipeline(config)
        df = _make_string_df()
        result = pipeline.run(df)

        assert isinstance(result, PipelineResult)
        assert "default" in result.tables
        tbl = result.tables["default"]

        # A003 (유령회사) should be dropped (all empty -> NaN)
        assert set(tbl["코드"].unique()) == {"A001", "A002"}

        # Numbers should be parsed
        assert tbl.loc[tbl["코드"] == "A001", "수정시가(원)"].iloc[0] == 25200.0

        # Entity counts
        assert result.drop_result is not None
        assert result.drop_result.entities_total == 3
        assert result.drop_result.entities_dropped == 1

    # -----------------------------------------------------------------
    # Selective step execution
    # -----------------------------------------------------------------

    def test_unit_normalization_disabled(self):
        """When normalize_units=False, columns are not renamed or scaled."""
        df = pd.DataFrame({
            "코드": ["A001"],
            "코드명": ["삼성전자"],
            "date": ["2024-01-01"],
            "매출액(억원)": ["100"],
        }).astype(str)

        config = _make_config(
            tables={"default": ["매출액(억원)"]},
            normalize_units=False,
        )
        result = TransformPipeline(config).run(df)
        tbl = result.tables["default"]
        assert "매출액(억원)" in tbl.columns
        assert "매출액(원)" not in tbl.columns
        assert tbl["매출액(억원)"].iloc[0] == 100.0
        assert result.unit_info == {}

    def test_empty_drop_disabled(self):
        """When drop_empty_entities=False, empty entities are kept."""
        config = _make_config(
            tables={"default": ["수정시가(원)", "거래량(주)"]},
            drop_empty_entities=False,
        )
        df = _make_string_df()
        result = TransformPipeline(config).run(df)
        tbl = result.tables["default"]
        # A003 should still be there
        assert "A003" in set(tbl["코드"].unique())
        assert result.drop_result is None

    # -----------------------------------------------------------------
    # Unit normalization + table group resolution
    # -----------------------------------------------------------------

    def test_unit_normalization_with_table_groups(self):
        """Table groups should be updated to reflect renamed columns."""
        df = pd.DataFrame({
            "코드": ["A001"],
            "코드명": ["삼성전자"],
            "date": ["2024-01-01"],
            "매출액(억원)": ["100"],
            "거래량(주)": ["1,000"],
        }).astype(str)

        config = _make_config(
            tables={
                "financials": ["매출액(억원)"],
                "volume": ["거래량(주)"],
            },
        )
        result = TransformPipeline(config).run(df)

        # The financials table should have 매출액(원) (renamed from 억원)
        fin = result.tables["financials"]
        assert "매출액(원)" in fin.columns
        assert "매출액(억원)" not in fin.columns
        assert fin["매출액(원)"].iloc[0] == 10_000_000_000

        # Volume table should be unaffected (주 is non-monetary)
        vol = result.tables["volume"]
        assert "거래량(주)" in vol.columns
        assert vol["거래량(주)"].iloc[0] == 1000.0

    # -----------------------------------------------------------------
    # Return type and structure
    # -----------------------------------------------------------------

    def test_returns_pipeline_result(self):
        """run() should return a PipelineResult dataclass."""
        config = _make_config(tables={"default": ["수정시가(원)"]})
        df = pd.DataFrame({
            "코드": ["A001"],
            "코드명": ["삼성전자"],
            "date": ["2024-01-01"],
            "수정시가(원)": ["100"],
        }).astype(str)
        result = TransformPipeline(config).run(df)
        assert isinstance(result, PipelineResult)
        assert isinstance(result.tables, dict)
        assert isinstance(result.unit_info, dict)

    def test_key_columns_in_output(self):
        """Key columns should always be present in every output table."""
        config = _make_config(
            tables={
                "t1": ["수정시가(원)"],
                "t2": ["거래량(주)"],
            },
        )
        df = _make_string_df()
        result = TransformPipeline(config).run(df)
        for name, tbl in result.tables.items():
            assert "코드" in tbl.columns
            assert "코드명" in tbl.columns
            assert "date" in tbl.columns

    # -----------------------------------------------------------------
    # Edge cases
    # -----------------------------------------------------------------

    def test_custom_key_columns(self):
        """Pipeline should accept custom key_columns for misc formats."""
        df = pd.DataFrame({
            "date": ["2025-01-02"],
            "ETF코드": ["A069500"],
            "금액": ["9,945,243"],
        }).astype(str)

        config = _make_config(tables={"default": ["금액"]})
        result = TransformPipeline(config).run(
            df, key_columns=["date", "ETF코드"]
        )
        tbl = result.tables["default"]
        assert "date" in tbl.columns
        assert "ETF코드" in tbl.columns
        assert tbl["금액"].iloc[0] == 9_945_243
