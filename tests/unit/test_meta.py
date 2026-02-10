"""
Unit tests for the _meta table builder (fn_dg6_ingest.meta).

Tests build_meta_table() with synthetic IngestConfig objects, ItemInfo
lists, unit_info dicts, and entity_stats -- no real file I/O.
Also tests _compute_file_hash() against a temp file.
"""

from __future__ import annotations

import pandas as pd
import pytest

from fn_dg6_ingest.config import IngestConfig, MetadataConfig, OutputConfig, SourceConfig
from fn_dg6_ingest.meta import _compute_file_hash, _resolve_normalized_name, build_meta_table
from fn_dg6_ingest.parsers.base import ItemInfo


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(
    *,
    input_path: str = "test_data.csv",
    detected_format: str = "timeseries_wide",
    frequency: str | None = "일간",
    period_start: str | None = "20160101",
    period_end: str | None = "20260206",
    non_business_days: str | None = "제외",
    include_weekends: str | None = "제외",
) -> IngestConfig:
    """Build a minimal IngestConfig for testing."""
    return IngestConfig(
        source=SourceConfig(input_path=input_path, detected_format=detected_format),
        metadata=MetadataConfig(
            frequency=frequency,
            period_start=period_start,
            period_end=period_end,
            non_business_days=non_business_days,
            include_weekends=include_weekends,
        ),
        output=OutputConfig(),
        tables={"default": ["수정시가(원)", "거래량(주)"]},
    )


def _make_items() -> list[ItemInfo]:
    """Build a standard 2-item list for testing."""
    return [
        ItemInfo(아이템코드="S410000650", 아이템명="수정시가(원)", 유형="SSC", 집계주기="일간"),
        ItemInfo(아이템코드="S410001200", 아이템명="거래량(주)", 유형="SSC", 집계주기="일간"),
    ]


# All 20 columns per the PRD FR-9 schema
META_COLUMNS = [
    "table_name", "source_file", "source_hash", "source_last_updated",
    "detected_format", "아이템코드", "아이템명", "아이템명_normalized",
    "유형", "집계주기", "frequency", "period_start", "period_end",
    "unit_original", "unit_multiplier", "non_business_days", "include_weekends",
    "entities_total", "entities_dropped", "processed_at",
]


# ---------------------------------------------------------------------------
# Tests for _compute_file_hash
# ---------------------------------------------------------------------------

class TestComputeFileHash:
    """Tests for the SHA-256 file hashing helper."""

    def test_deterministic(self, tmp_path):
        """Same file content produces the same hash on repeated calls."""
        f = tmp_path / "data.csv"
        f.write_bytes(b"hello world")
        h1 = _compute_file_hash(f)
        h2 = _compute_file_hash(f)
        assert h1 == h2
        assert len(h1) == 64  # SHA-256 hex digest length

    def test_different_content_different_hash(self, tmp_path):
        """Different file contents produce different hashes."""
        f1 = tmp_path / "a.csv"
        f2 = tmp_path / "b.csv"
        f1.write_bytes(b"content A")
        f2.write_bytes(b"content B")
        assert _compute_file_hash(f1) != _compute_file_hash(f2)

    def test_missing_file_raises(self, tmp_path):
        """Non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            _compute_file_hash(tmp_path / "nonexistent.csv")


# ---------------------------------------------------------------------------
# Tests for _resolve_normalized_name
# ---------------------------------------------------------------------------

class TestResolveNormalizedName:
    """Tests for the item name normalization helper."""

    def test_no_unit_info(self):
        """Item not in unit_info returns the original name."""
        assert _resolve_normalized_name("수정시가(원)", {}) == "수정시가(원)"

    def test_unit_multiplier_1(self):
        """Item with multiplier=1 (already base unit) keeps original name."""
        unit_info = {"수정시가(원)": ("원", 1)}
        assert _resolve_normalized_name("수정시가(원)", unit_info) == "수정시가(원)"

    def test_unit_multiplier_gt_1(self):
        """Item with multiplier>1 gets renamed to (원)."""
        unit_info = {"매출액(억원)": ("억원", 100_000_000)}
        assert _resolve_normalized_name("매출액(억원)", unit_info) == "매출액(원)"


# ---------------------------------------------------------------------------
# Tests for build_meta_table
# ---------------------------------------------------------------------------

class TestBuildMetaTable:
    """Tests for build_meta_table()."""

    # -----------------------------------------------------------------
    # Basic structure
    # -----------------------------------------------------------------

    def test_column_completeness(self):
        """The meta table must have all 20 columns per FR-9."""
        config = _make_config()
        items = _make_items()
        table_assignment = {"수정시가(원)": "default", "거래량(주)": "default"}
        df = build_meta_table(
            config=config,
            items=items,
            source_last_updated="2026-02-07 15:46:56",
            table_assignment=table_assignment,
            unit_info={},
            entity_stats={"default": (2400, 12)},
        )
        assert list(df.columns) == META_COLUMNS

    def test_one_row_per_item(self):
        """Output should have exactly one row per ItemInfo."""
        config = _make_config()
        items = _make_items()
        table_assignment = {"수정시가(원)": "default", "거래량(주)": "default"}
        df = build_meta_table(
            config=config,
            items=items,
            source_last_updated=None,
            table_assignment=table_assignment,
            unit_info={},
            entity_stats={},
        )
        assert len(df) == 2

    def test_empty_items(self):
        """Empty items list produces an empty DataFrame with correct columns."""
        config = _make_config()
        df = build_meta_table(
            config=config,
            items=[],
            source_last_updated=None,
            table_assignment={},
            unit_info={},
            entity_stats={},
        )
        assert len(df) == 0
        assert list(df.columns) == META_COLUMNS

    # -----------------------------------------------------------------
    # Source-level columns
    # -----------------------------------------------------------------

    def test_source_file_is_basename(self):
        """source_file should be the filename only, not the full path."""
        config = _make_config(input_path="inputs/dataguide_ohlcv.csv")
        items = [ItemInfo(아이템명="수정시가(원)")]
        table_assignment = {"수정시가(원)": "default"}
        df = build_meta_table(
            config=config,
            items=items,
            source_last_updated=None,
            table_assignment=table_assignment,
            unit_info={},
            entity_stats={},
        )
        assert df["source_file"].iloc[0] == "dataguide_ohlcv.csv"

    def test_source_hash_empty_when_file_missing(self):
        """When the source file doesn't exist, hash should be empty string."""
        config = _make_config(input_path="nonexistent_file.csv")
        items = [ItemInfo(아이템명="수정시가(원)")]
        table_assignment = {"수정시가(원)": "default"}
        df = build_meta_table(
            config=config,
            items=items,
            source_last_updated=None,
            table_assignment=table_assignment,
            unit_info={},
            entity_stats={},
        )
        assert df["source_hash"].iloc[0] == ""

    def test_source_last_updated_propagation(self):
        """source_last_updated value is propagated to all rows."""
        config = _make_config()
        items = _make_items()
        table_assignment = {"수정시가(원)": "default", "거래량(주)": "default"}
        df = build_meta_table(
            config=config,
            items=items,
            source_last_updated="2026-02-07 15:46:56",
            table_assignment=table_assignment,
            unit_info={},
            entity_stats={},
        )
        assert all(df["source_last_updated"] == "2026-02-07 15:46:56")

    def test_source_last_updated_none(self):
        """None source_last_updated is preserved as NaN/None in DataFrame."""
        config = _make_config()
        items = [ItemInfo(아이템명="수정시가(원)")]
        table_assignment = {"수정시가(원)": "default"}
        df = build_meta_table(
            config=config,
            items=items,
            source_last_updated=None,
            table_assignment=table_assignment,
            unit_info={},
            entity_stats={},
        )
        assert pd.isna(df["source_last_updated"].iloc[0])

    def test_detected_format(self):
        """detected_format should come from config.source.detected_format."""
        config = _make_config(detected_format="misc_etf")
        items = [ItemInfo(아이템명="금액")]
        table_assignment = {"금액": "default"}
        df = build_meta_table(
            config=config,
            items=items,
            source_last_updated=None,
            table_assignment=table_assignment,
            unit_info={},
            entity_stats={},
        )
        assert df["detected_format"].iloc[0] == "misc_etf"

    # -----------------------------------------------------------------
    # Item-level columns
    # -----------------------------------------------------------------

    def test_item_attributes(self):
        """Item-level attributes (아이템코드, 유형, 집계주기) are correct."""
        config = _make_config()
        items = [ItemInfo(아이템코드="S410000650", 아이템명="수정시가(원)", 유형="SSC", 집계주기="일간")]
        table_assignment = {"수정시가(원)": "default"}
        df = build_meta_table(
            config=config,
            items=items,
            source_last_updated=None,
            table_assignment=table_assignment,
            unit_info={},
            entity_stats={},
        )
        row = df.iloc[0]
        assert row["아이템코드"] == "S410000650"
        assert row["아이템명"] == "수정시가(원)"
        assert row["유형"] == "SSC"
        assert row["집계주기"] == "일간"

    def test_item_optional_fields_none(self):
        """Items with None optional fields produce NaN in the DataFrame."""
        config = _make_config()
        items = [ItemInfo(아이템명="금액")]  # 아이템코드, 유형, 집계주기 all None
        table_assignment = {"금액": "default"}
        df = build_meta_table(
            config=config,
            items=items,
            source_last_updated=None,
            table_assignment=table_assignment,
            unit_info={},
            entity_stats={},
        )
        row = df.iloc[0]
        assert pd.isna(row["아이템코드"])
        assert pd.isna(row["유형"])
        assert pd.isna(row["집계주기"])

    # -----------------------------------------------------------------
    # Unit normalization columns
    # -----------------------------------------------------------------

    def test_unit_info_present(self):
        """unit_original and unit_multiplier are set from unit_info."""
        config = _make_config()
        items = [ItemInfo(아이템명="매출액(억원)")]
        table_assignment = {"매출액(억원)": "default"}
        unit_info = {"매출액(억원)": ("억원", 100_000_000)}
        df = build_meta_table(
            config=config,
            items=items,
            source_last_updated=None,
            table_assignment=table_assignment,
            unit_info=unit_info,
            entity_stats={},
        )
        row = df.iloc[0]
        assert row["unit_original"] == "억원"
        assert row["unit_multiplier"] == 100_000_000
        assert row["아이템명_normalized"] == "매출액(원)"

    def test_unit_info_absent(self):
        """Items not in unit_info get None/1 defaults."""
        config = _make_config()
        items = [ItemInfo(아이템명="수정시가(원)")]
        table_assignment = {"수정시가(원)": "default"}
        df = build_meta_table(
            config=config,
            items=items,
            source_last_updated=None,
            table_assignment=table_assignment,
            unit_info={},
            entity_stats={},
        )
        row = df.iloc[0]
        assert pd.isna(row["unit_original"])
        assert row["unit_multiplier"] == 1
        assert row["아이템명_normalized"] == "수정시가(원)"  # unchanged

    # -----------------------------------------------------------------
    # Table assignment and entity stats
    # -----------------------------------------------------------------

    def test_table_assignment(self):
        """table_name column reflects the table_assignment mapping."""
        config = _make_config()
        items = [
            ItemInfo(아이템명="수정시가(원)"),
            ItemInfo(아이템명="거래량(주)"),
        ]
        table_assignment = {"수정시가(원)": "ohlcv", "거래량(주)": "volume"}
        df = build_meta_table(
            config=config,
            items=items,
            source_last_updated=None,
            table_assignment=table_assignment,
            unit_info={},
            entity_stats={},
        )
        assert df.iloc[0]["table_name"] == "ohlcv"
        assert df.iloc[1]["table_name"] == "volume"

    def test_entity_stats_per_table(self):
        """entities_total/dropped are derived from entity_stats per table."""
        config = _make_config()
        items = [
            ItemInfo(아이템명="수정시가(원)"),
            ItemInfo(아이템명="거래량(주)"),
        ]
        table_assignment = {"수정시가(원)": "ohlcv", "거래량(주)": "volume"}
        entity_stats = {"ohlcv": (2400, 12), "volume": (2400, 5)}
        df = build_meta_table(
            config=config,
            items=items,
            source_last_updated=None,
            table_assignment=table_assignment,
            unit_info={},
            entity_stats=entity_stats,
        )
        assert df.iloc[0]["entities_total"] == 2400
        assert df.iloc[0]["entities_dropped"] == 12
        assert df.iloc[1]["entities_total"] == 2400
        assert df.iloc[1]["entities_dropped"] == 5

    def test_entity_stats_missing_table(self):
        """Missing table in entity_stats defaults to (0, 0)."""
        config = _make_config()
        items = [ItemInfo(아이템명="수정시가(원)")]
        table_assignment = {"수정시가(원)": "unknown_table"}
        df = build_meta_table(
            config=config,
            items=items,
            source_last_updated=None,
            table_assignment=table_assignment,
            unit_info={},
            entity_stats={},
        )
        assert df.iloc[0]["entities_total"] == 0
        assert df.iloc[0]["entities_dropped"] == 0

    # -----------------------------------------------------------------
    # Metadata-level columns
    # -----------------------------------------------------------------

    def test_metadata_propagation(self):
        """Config metadata fields propagate to every row."""
        config = _make_config(
            frequency="일간",
            period_start="20160101",
            period_end="20260206",
            non_business_days="제외",
            include_weekends="포함",
        )
        items = _make_items()
        table_assignment = {"수정시가(원)": "default", "거래량(주)": "default"}
        df = build_meta_table(
            config=config,
            items=items,
            source_last_updated=None,
            table_assignment=table_assignment,
            unit_info={},
            entity_stats={},
        )
        for _, row in df.iterrows():
            assert row["frequency"] == "일간"
            assert row["period_start"] == "20160101"
            assert row["period_end"] == "20260206"
            assert row["non_business_days"] == "제외"
            assert row["include_weekends"] == "포함"

    # -----------------------------------------------------------------
    # Processing column
    # -----------------------------------------------------------------

    def test_processed_at_iso_format(self):
        """processed_at should be an ISO-8601 UTC timestamp."""
        config = _make_config()
        items = [ItemInfo(아이템명="수정시가(원)")]
        table_assignment = {"수정시가(원)": "default"}
        df = build_meta_table(
            config=config,
            items=items,
            source_last_updated=None,
            table_assignment=table_assignment,
            unit_info={},
            entity_stats={},
        )
        ts = df["processed_at"].iloc[0]
        # Should be parseable and contain timezone info
        assert "T" in ts
        assert "+" in ts or "Z" in ts  # UTC offset

    def test_table_assignment_missing_item(self):
        """Item not in table_assignment gets empty string for table_name."""
        config = _make_config()
        items = [ItemInfo(아이템명="orphan_item")]
        df = build_meta_table(
            config=config,
            items=items,
            source_last_updated=None,
            table_assignment={},  # deliberately empty
            unit_info={},
            entity_stats={},
        )
        assert df.iloc[0]["table_name"] == ""
