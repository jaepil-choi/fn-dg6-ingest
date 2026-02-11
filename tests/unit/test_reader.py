"""
Unit tests for fn_dg6_ingest.reader.

All tests use synthetic data written to ``tmp_path`` -- no real input
files required.  The tests verify:

- Parquet: column pruning, predicate pushdown (codes, date range),
  combined filters, and full-load baseline.
- CSV: same filtering interface with post-load pandas filtering.
- read_meta: loads the ``_meta`` table in both formats.
- read_table_info: schema + row count without data scan (Parquet footer
  vs CSV line count).
- Error handling: missing files, unsupported formats.
"""

from __future__ import annotations

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fn_dg6_ingest.reader import read_meta, read_table, read_table_info


# ---------------------------------------------------------------------------
# Fixtures -- synthetic data
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_df() -> pd.DataFrame:
    """A small time-series-style DataFrame for testing.

    Shape: 12 rows (3 codes x 4 dates), 5 columns.
    Key columns: 코드, 코드명, date
    Value columns: 수정주가(원), 거래량(주)
    """
    codes = ["A005930", "A005930", "A005930", "A005930",
             "A000660", "A000660", "A000660", "A000660",
             "A035420", "A035420", "A035420", "A035420"]
    names = ["삼성전자"] * 4 + ["SK하이닉스"] * 4 + ["NAVER"] * 4
    dates = ["2024-01-02", "2024-06-03", "2025-01-02", "2025-06-02"] * 3
    prices = [71000, 78000, 56000, 62000,
              132000, 195000, 185000, 210000,
              210000, 180000, 195000, 220000]
    volumes = [10_000_000, 12_000_000, 15_000_000, 11_000_000,
               5_000_000, 8_000_000, 7_000_000, 6_000_000,
               2_000_000, 3_000_000, 2_500_000, 2_800_000]

    return pd.DataFrame({
        "코드": codes,
        "코드명": names,
        "date": dates,
        "수정주가(원)": prices,
        "거래량(주)": volumes,
    })


@pytest.fixture()
def sample_meta_df() -> pd.DataFrame:
    """A tiny _meta DataFrame matching the FR-9 schema."""
    return pd.DataFrame({
        "table_name": ["default", "default"],
        "source_file": ["test.csv", "test.csv"],
        "source_hash": ["abc123", "abc123"],
        "source_last_updated": ["2026-02-07 15:00:00", "2026-02-07 15:00:00"],
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
        "entities_total": [3, 3],
        "entities_dropped": [0, 0],
        "processed_at": ["2026-02-11T00:00:00+00:00", "2026-02-11T00:00:00+00:00"],
    })


@pytest.fixture()
def parquet_dir(tmp_path, sample_df, sample_meta_df):
    """Write sample data as Parquet to a temp directory."""
    sample_df.to_parquet(tmp_path / "default.parquet", index=False)
    sample_meta_df.to_parquet(tmp_path / "_meta.parquet", index=False)
    return tmp_path


@pytest.fixture()
def csv_dir(tmp_path, sample_df, sample_meta_df):
    """Write sample data as CSV to a temp directory."""
    out = tmp_path / "csv_out"
    out.mkdir()
    sample_df.to_csv(out / "default.csv", index=False, encoding="utf-8-sig")
    sample_meta_df.to_csv(out / "_meta.csv", index=False, encoding="utf-8-sig")
    return out


# ---------------------------------------------------------------------------
# Tests: read_table -- Parquet
# ---------------------------------------------------------------------------

class TestReadTableParquet:
    """Parquet reading with PyArrow-native filtering."""

    def test_full_load(self, parquet_dir, sample_df):
        """No filters -> returns all 12 rows, 5 columns."""
        df = read_table(parquet_dir, "default", "parquet")
        assert len(df) == 12
        assert list(df.columns) == list(sample_df.columns)

    def test_column_pruning(self, parquet_dir):
        """items=['수정주가(원)'] -> only key columns + that item."""
        df = read_table(
            parquet_dir, "default", "parquet",
            items=["수정주가(원)"],
        )
        assert set(df.columns) == {"코드", "코드명", "date", "수정주가(원)"}
        assert "거래량(주)" not in df.columns
        assert len(df) == 12  # all rows, just fewer columns

    def test_filter_codes(self, parquet_dir):
        """codes=['A005930'] -> only 삼성전자 rows (4 rows)."""
        df = read_table(
            parquet_dir, "default", "parquet",
            codes=["A005930"],
        )
        assert len(df) == 4
        assert set(df["코드"].unique()) == {"A005930"}

    def test_filter_codes_multiple(self, parquet_dir):
        """codes with two tickers -> 8 rows."""
        df = read_table(
            parquet_dir, "default", "parquet",
            codes=["A005930", "A000660"],
        )
        assert len(df) == 8
        assert set(df["코드"].unique()) == {"A005930", "A000660"}

    def test_filter_date_range(self, parquet_dir):
        """date_from='2025-01-01' -> only 2025 dates (6 rows)."""
        df = read_table(
            parquet_dir, "default", "parquet",
            date_from="2025-01-01",
        )
        assert len(df) == 6
        assert all(d >= "2025-01-01" for d in df["date"])

    def test_filter_date_range_both_bounds(self, parquet_dir):
        """date_from + date_to narrows to mid-range."""
        df = read_table(
            parquet_dir, "default", "parquet",
            date_from="2024-06-01",
            date_to="2025-01-31",
        )
        assert len(df) == 6  # 2024-06-03 + 2025-01-02 for each of 3 codes
        assert all("2024-06-01" <= d <= "2025-01-31" for d in df["date"])

    def test_combined_filters(self, parquet_dir):
        """codes + date + items all applied together."""
        df = read_table(
            parquet_dir, "default", "parquet",
            codes=["A005930"],
            date_from="2025-01-01",
            items=["수정주가(원)"],
        )
        assert len(df) == 2  # 삼성전자, 2025-01-02 + 2025-06-02
        assert set(df.columns) == {"코드", "코드명", "date", "수정주가(원)"}
        assert set(df["코드"].unique()) == {"A005930"}

    def test_file_not_found(self, tmp_path):
        """Missing file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="Table file not found"):
            read_table(tmp_path, "nonexistent", "parquet")


# ---------------------------------------------------------------------------
# Tests: read_table -- CSV
# ---------------------------------------------------------------------------

class TestReadTableCsv:
    """CSV reading with post-load pandas filtering."""

    def test_full_load(self, csv_dir, sample_df):
        """No filters -> all 12 rows."""
        df = read_table(csv_dir, "default", "csv")
        assert len(df) == 12
        assert set(df.columns) == set(sample_df.columns)

    def test_column_pruning(self, csv_dir):
        """items parameter limits columns."""
        df = read_table(
            csv_dir, "default", "csv",
            items=["수정주가(원)"],
        )
        assert set(df.columns) == {"코드", "코드명", "date", "수정주가(원)"}

    def test_filter_codes(self, csv_dir):
        """codes filtering in CSV."""
        df = read_table(
            csv_dir, "default", "csv",
            codes=["A005930"],
        )
        assert len(df) == 4
        assert set(df["코드"].unique()) == {"A005930"}

    def test_filter_date_range(self, csv_dir):
        """Date range filtering in CSV."""
        df = read_table(
            csv_dir, "default", "csv",
            date_from="2025-01-01",
        )
        assert len(df) == 6

    def test_combined_filters(self, csv_dir):
        """Combined codes + date + items in CSV."""
        df = read_table(
            csv_dir, "default", "csv",
            codes=["A005930"],
            date_from="2025-01-01",
            items=["수정주가(원)"],
        )
        assert len(df) == 2
        assert set(df.columns) == {"코드", "코드명", "date", "수정주가(원)"}


# ---------------------------------------------------------------------------
# Tests: read_table -- unsupported format
# ---------------------------------------------------------------------------

class TestReadTableEdgeCases:

    def test_unsupported_format_raises(self, tmp_path):
        """Unsupported format raises ValueError."""
        # Create a dummy file so path resolution passes
        (tmp_path / "default.json").write_text("{}")
        with pytest.raises(ValueError, match="Unsupported output format"):
            read_table(tmp_path, "default", "json")


# ---------------------------------------------------------------------------
# Tests: read_meta
# ---------------------------------------------------------------------------

class TestReadMeta:

    def test_read_meta_parquet(self, parquet_dir, sample_meta_df):
        """read_meta loads _meta.parquet correctly."""
        meta = read_meta(parquet_dir, "parquet")
        assert len(meta) == len(sample_meta_df)
        assert set(meta.columns) == set(sample_meta_df.columns)

    def test_read_meta_csv(self, csv_dir, sample_meta_df):
        """read_meta loads _meta.csv correctly."""
        meta = read_meta(csv_dir, "csv")
        assert len(meta) == len(sample_meta_df)
        assert set(meta.columns) == set(sample_meta_df.columns)

    def test_read_meta_file_not_found(self, tmp_path):
        """Missing _meta file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="Table file not found"):
            read_meta(tmp_path, "parquet")


# ---------------------------------------------------------------------------
# Tests: read_table_info
# ---------------------------------------------------------------------------

class TestReadTableInfo:

    def test_info_parquet(self, parquet_dir):
        """Parquet info reads schema + row count from footer."""
        info = read_table_info(parquet_dir, "default", "parquet")
        assert info["num_rows"] == 12
        assert info["num_columns"] == 5
        assert "코드" in info["columns"]
        assert "수정주가(원)" in info["columns"]
        assert isinstance(info["dtypes"], dict)
        assert len(info["dtypes"]) == 5

    def test_info_csv(self, csv_dir):
        """CSV info reads schema from header + line count."""
        info = read_table_info(csv_dir, "default", "csv")
        assert info["num_rows"] == 12
        assert info["num_columns"] == 5
        assert "코드" in info["columns"]

    def test_info_file_not_found(self, tmp_path):
        """Missing file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            read_table_info(tmp_path, "nonexistent", "parquet")


# ---------------------------------------------------------------------------
# Tests: custom key_columns (misc format)
# ---------------------------------------------------------------------------

class TestCustomKeyColumns:
    """Verify that non-default key columns (e.g., ETF format) work."""

    @pytest.fixture()
    def etf_dir(self, tmp_path):
        """Create a synthetic ETF-style dataset with different key columns."""
        df = pd.DataFrame({
            "날짜": ["2025-01-02", "2025-01-02", "2025-01-03", "2025-01-03"],
            "ETF코드": ["A069500", "A069500", "A069500", "A069500"],
            "ETF명": ["KODEX 200"] * 4,
            "구성종목코드": ["A005930", "A000660", "A005930", "A000660"],
            "구성종목": ["삼성전자", "SK하이닉스", "삼성전자", "SK하이닉스"],
            "금액": [1000000, 500000, 1100000, 520000],
        })
        df.to_parquet(tmp_path / "default.parquet", index=False)
        return tmp_path

    def test_filter_with_custom_key_columns(self, etf_dir):
        """Filtering with ETF-style key columns uses 날짜 for date."""
        key_cols = ["날짜", "ETF코드", "ETF명", "구성종목코드", "구성종목"]
        df = read_table(
            etf_dir, "default", "parquet",
            date_from="2025-01-03",
            key_columns=key_cols,
        )
        assert len(df) == 2
        assert all(d >= "2025-01-03" for d in df["날짜"])

    def test_codes_filter_with_custom_key_columns(self, etf_dir):
        """codes filter picks the first '코드'-containing column."""
        key_cols = ["날짜", "ETF코드", "ETF명", "구성종목코드", "구성종목"]
        df = read_table(
            etf_dir, "default", "parquet",
            codes=["A069500"],
            key_columns=key_cols,
        )
        # All rows have ETF코드=A069500, so all 4 rows should match
        assert len(df) == 4
