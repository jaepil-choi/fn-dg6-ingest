"""
Unit tests for fn_dg6_ingest.dataset.

Tests use synthetic Parquet/CSV files written to ``tmp_path`` and
a minimal ``IngestConfig`` -- no real input files required.

Covers:
- Dataset construction and property access
- load() single table, multi-table, with filters
- load() error on bad table name
- load_meta() delegation
- describe() structure and field validation
- save_config() round-trip
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from fn_dg6_ingest.config import (
    IngestConfig,
    MetadataConfig,
    OutputConfig,
    SourceConfig,
    save_config,
)
from fn_dg6_ingest.dataset import Dataset, DatasetInfo


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_config(
    output_dir: str,
    tables: dict[str, list[str]] | None = None,
    output_format: str = "parquet",
    detected_format: str = "timeseries_wide",
) -> IngestConfig:
    """Build a minimal IngestConfig for testing."""
    if tables is None:
        tables = {"default": ["수정주가(원)", "거래량(주)"]}
    return IngestConfig(
        source=SourceConfig(
            input_path="inputs/test.csv",
            detected_format=detected_format,
        ),
        metadata=MetadataConfig(
            frequency="일간",
            period_start="20240101",
            period_end="20250602",
        ),
        output=OutputConfig(
            output_dir=output_dir,
            output_format=output_format,
        ),
        tables=tables,
    )


@pytest.fixture()
def sample_df() -> pd.DataFrame:
    """Synthetic time-series DataFrame."""
    return pd.DataFrame({
        "코드": ["A005930"] * 4 + ["A000660"] * 4,
        "코드명": ["삼성전자"] * 4 + ["SK하이닉스"] * 4,
        "date": ["2024-01-02", "2024-06-03", "2025-01-02", "2025-06-02"] * 2,
        "수정주가(원)": [71000, 78000, 56000, 62000, 132000, 195000, 185000, 210000],
        "거래량(주)": [10_000_000, 12_000_000, 15_000_000, 11_000_000,
                      5_000_000, 8_000_000, 7_000_000, 6_000_000],
    })


@pytest.fixture()
def sample_meta_df() -> pd.DataFrame:
    """Synthetic _meta DataFrame."""
    return pd.DataFrame({
        "table_name": ["default", "default"],
        "source_file": ["test.csv", "test.csv"],
        "source_hash": ["abc", "abc"],
        "source_last_updated": ["2026-02-07", "2026-02-07"],
        "detected_format": ["timeseries_wide", "timeseries_wide"],
        "아이템코드": ["S1", "S2"],
        "아이템명": ["수정주가(원)", "거래량(주)"],
        "아이템명_normalized": ["수정주가(원)", "거래량(주)"],
        "유형": ["SSC", "SSC"],
        "집계주기": ["일간", "일간"],
        "frequency": ["일간", "일간"],
        "period_start": ["20240101", "20240101"],
        "period_end": ["20250602", "20250602"],
        "unit_original": ["원", "주"],
        "unit_multiplier": [1, 1],
        "non_business_days": ["제외", "제외"],
        "include_weekends": ["제외", "제외"],
        "entities_total": [2, 2],
        "entities_dropped": [0, 0],
        "processed_at": ["2026-02-11T00:00:00+00:00", "2026-02-11T00:00:00+00:00"],
    })


@pytest.fixture()
def dataset_parquet(tmp_path, sample_df, sample_meta_df) -> Dataset:
    """A Dataset backed by synthetic Parquet files."""
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    sample_df.to_parquet(out_dir / "default.parquet", index=False)
    sample_meta_df.to_parquet(out_dir / "_meta.parquet", index=False)

    config = _make_config(output_dir=str(out_dir))
    config_path = tmp_path / "fnconfig.yaml"
    save_config(config, config_path)
    return Dataset(config, config_path)


@pytest.fixture()
def dataset_csv(tmp_path, sample_df, sample_meta_df) -> Dataset:
    """A Dataset backed by synthetic CSV files."""
    out_dir = tmp_path / "csv_out"
    out_dir.mkdir()
    sample_df.to_csv(out_dir / "default.csv", index=False, encoding="utf-8-sig")
    sample_meta_df.to_csv(out_dir / "_meta.csv", index=False, encoding="utf-8-sig")

    config = _make_config(output_dir=str(out_dir), output_format="csv")
    config_path = tmp_path / "fnconfig.yaml"
    save_config(config, config_path)
    return Dataset(config, config_path)


@pytest.fixture()
def dataset_multi_table(tmp_path, sample_df, sample_meta_df) -> Dataset:
    """A Dataset with two tables: ohlcv and volume."""
    out_dir = tmp_path / "multi_out"
    out_dir.mkdir()

    ohlcv_df = sample_df[["코드", "코드명", "date", "수정주가(원)"]].copy()
    volume_df = sample_df[["코드", "코드명", "date", "거래량(주)"]].copy()

    ohlcv_df.to_parquet(out_dir / "ohlcv.parquet", index=False)
    volume_df.to_parquet(out_dir / "volume.parquet", index=False)
    sample_meta_df.to_parquet(out_dir / "_meta.parquet", index=False)

    config = _make_config(
        output_dir=str(out_dir),
        tables={
            "ohlcv": ["수정주가(원)"],
            "volume": ["거래량(주)"],
        },
    )
    config_path = tmp_path / "fnconfig.yaml"
    save_config(config, config_path)
    return Dataset(config, config_path)


# ---------------------------------------------------------------------------
# Tests: Construction + properties
# ---------------------------------------------------------------------------

class TestDatasetConstruction:

    def test_config_and_path(self, dataset_parquet):
        """Dataset stores config and config_path."""
        assert isinstance(dataset_parquet.config, IngestConfig)
        assert dataset_parquet.config_path.exists()

    def test_output_dir_property(self, dataset_parquet):
        """output_dir is derived from config."""
        assert dataset_parquet.output_dir == Path(
            dataset_parquet.config.output.output_dir
        )

    def test_repr(self, dataset_parquet):
        """__repr__ is informative."""
        r = repr(dataset_parquet)
        assert "timeseries_wide" in r
        assert "default" in r


# ---------------------------------------------------------------------------
# Tests: load()
# ---------------------------------------------------------------------------

class TestDatasetLoad:

    def test_load_full(self, dataset_parquet):
        """load() with no args returns full DataFrame."""
        df = dataset_parquet.load()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 8
        assert "코드" in df.columns
        assert "수정주가(원)" in df.columns

    def test_load_specific_table(self, dataset_multi_table):
        """load(table='ohlcv') returns only that table."""
        df = dataset_multi_table.load(table="ohlcv")
        assert isinstance(df, pd.DataFrame)
        assert "수정주가(원)" in df.columns
        assert "거래량(주)" not in df.columns

    def test_load_all_multi_table_returns_dict(self, dataset_multi_table):
        """load() with multiple tables returns dict."""
        result = dataset_multi_table.load()
        assert isinstance(result, dict)
        assert set(result.keys()) == {"ohlcv", "volume"}
        assert "수정주가(원)" in result["ohlcv"].columns
        assert "거래량(주)" in result["volume"].columns

    def test_load_filter_codes(self, dataset_parquet):
        """load(codes=...) filters rows by entity code."""
        df = dataset_parquet.load(codes=["A005930"])
        assert len(df) == 4
        assert set(df["코드"].unique()) == {"A005930"}

    def test_load_filter_date_range(self, dataset_parquet):
        """load(date_from=...) filters by date."""
        df = dataset_parquet.load(date_from="2025-01-01")
        assert len(df) == 4
        assert all(d >= "2025-01-01" for d in df["date"])

    def test_load_filter_items(self, dataset_parquet):
        """load(items=...) selects specific value columns."""
        df = dataset_parquet.load(items=["수정주가(원)"])
        assert "수정주가(원)" in df.columns
        assert "거래량(주)" not in df.columns

    def test_load_bad_table_name_raises(self, dataset_parquet):
        """load(table='nonexistent') raises ValueError."""
        with pytest.raises(ValueError, match="not found in config"):
            dataset_parquet.load(table="nonexistent")

    def test_load_csv_format(self, dataset_csv):
        """load() works with CSV output format."""
        df = dataset_csv.load()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 8


# ---------------------------------------------------------------------------
# Tests: load_meta()
# ---------------------------------------------------------------------------

class TestDatasetLoadMeta:

    def test_load_meta(self, dataset_parquet, sample_meta_df):
        """load_meta() returns the _meta DataFrame."""
        meta = dataset_parquet.load_meta()
        assert isinstance(meta, pd.DataFrame)
        assert len(meta) == len(sample_meta_df)
        assert "table_name" in meta.columns
        assert "아이템명" in meta.columns

    def test_load_meta_file_not_found(self, tmp_path):
        """load_meta() raises FileNotFoundError if _meta doesn't exist."""
        config = _make_config(output_dir=str(tmp_path))
        ds = Dataset(config, tmp_path / "fnconfig.yaml")
        with pytest.raises(FileNotFoundError):
            ds.load_meta()


# ---------------------------------------------------------------------------
# Tests: describe()
# ---------------------------------------------------------------------------

class TestDatasetDescribe:

    def test_describe_returns_datasetinfo(self, dataset_parquet):
        """describe() returns a DatasetInfo instance."""
        info = dataset_parquet.describe()
        assert isinstance(info, DatasetInfo)

    def test_describe_fields(self, dataset_parquet):
        """describe() populates all fields correctly."""
        info = dataset_parquet.describe()
        assert info.format_name == "timeseries_wide"
        assert info.tables == ["default"]
        assert "default" in info.shape
        assert info.shape["default"] == (8, 5)
        assert info.output_format == "parquet"
        assert info.date_range is not None
        assert info.entities == 2

    def test_describe_items(self, dataset_parquet):
        """describe() lists value columns (not key columns)."""
        info = dataset_parquet.describe()
        assert "default" in info.items
        # Items should exclude key columns
        assert "코드" not in info.items["default"]
        assert "수정주가(원)" in info.items["default"]

    def test_describe_multi_table(self, dataset_multi_table):
        """describe() handles multiple tables."""
        info = dataset_multi_table.describe()
        assert set(info.tables) == {"ohlcv", "volume"}
        assert "ohlcv" in info.shape
        assert "volume" in info.shape


# ---------------------------------------------------------------------------
# Tests: save_config()
# ---------------------------------------------------------------------------

class TestDatasetSaveConfig:

    def test_save_config_roundtrip(self, dataset_parquet):
        """save_config() writes config that can be loaded back."""
        # Modify config
        dataset_parquet.config.tables = {
            "ohlcv": ["수정주가(원)"],
            "volume": ["거래량(주)"],
        }
        dataset_parquet.save_config()

        # Reload and verify
        from fn_dg6_ingest.config import load_config

        reloaded = load_config(dataset_parquet.config_path)
        assert set(reloaded.tables.keys()) == {"ohlcv", "volume"}
        assert reloaded.tables["ohlcv"] == ["수정주가(원)"]
