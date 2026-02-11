"""
Unit tests for FormatDetector (fn_dg6_ingest.detect).

Tests layout-based coordinate detection using small inline CSV strings
written to temporary files. Also includes smoke tests against
real input files (if available).
"""

import pytest

from fn_dg6_ingest.detect import detect_format, _read_rows, _check_layout
from fn_dg6_ingest.exceptions import UnknownFormatError
from fn_dg6_ingest.layout_registry import load_all_layouts
from fn_dg6_ingest.parsers.timeseries import TimeSeriesWideParser
from fn_dg6_ingest.parsers.misc import MiscParser

# ---------------------------------------------------------------------------
# Synthetic time series wide-format sample (minimal, matches layout coords)
# ---------------------------------------------------------------------------
TIMESERIES_WIDE_SAMPLE = """\
Refresh,Last Updated: 2026-02-07 15:46:56,,,
달력기준,,,
코드 포트폴리오,all,기본설정,
아이템 포트폴리오,,,
출력주기,일간,원화,
비영업일,제외,오름차순,
주말포함,제외,,
기간,20160101,최근일자(20260206),
코드,코드명,유형,아이템코드,아이템명,집계주기,2016-01-04,2016-01-05
A005930,삼성전자,SSC,S410000650,수정시가(원),일간,25200,24040
A005930,삼성전자,SSC,S410000700,수정주가(원),일간,24100,24160
"""

# ---------------------------------------------------------------------------
# Synthetic misc ETF-format sample (minimal, matches layout coords)
# ---------------------------------------------------------------------------
MISC_ETF_SAMPLE = """\
Refresh,Last Updated: 2026-02-07 16:41:37,,,,,,
ETF 구성종목,,,,,,,
ETF,A069500,KODEX 200,,,,,
출력주기,일간,오름차순,,,,,
조회기간,20250101,최근일자(20260206),,,,,
날짜,ETF코드,ETF명,구성종목코드,구성종목,주식수(계약수),금액,금액기준 구성비중(%)
2025-01-02,A069500,KODEX 200,A000080,하이트진로,47,914620,0.06
"""

# ---------------------------------------------------------------------------
# Unknown format sample
# ---------------------------------------------------------------------------
UNKNOWN_SAMPLE = """\
col_a,col_b,col_c
1,2,3
4,5,6
"""


class TestReadRows:
    """Tests for _read_rows() helper."""

    def test_reads_correct_number(self, tmp_path):
        f = tmp_path / "test.csv"
        f.write_text("a,b\nc,d\ne,f\ng,h\n", encoding="utf-8")
        rows = _read_rows(f, n_rows=2)
        assert len(rows) == 2
        assert rows[0] == ["a", "b"]

    def test_handles_short_file(self, tmp_path):
        f = tmp_path / "test.csv"
        f.write_text("only,one\n", encoding="utf-8")
        rows = _read_rows(f, n_rows=20)
        assert len(rows) == 1


class TestDetectFormat:
    """Tests for detect_format() with synthetic data."""

    def test_detects_timeseries_wide(self, tmp_path):
        f = tmp_path / "ts_wide.csv"
        f.write_text(TIMESERIES_WIDE_SAMPLE, encoding="utf-8")
        parser_cls, layout = detect_format(f)
        assert parser_cls is TimeSeriesWideParser
        assert layout.format_name == "timeseries_wide"
        assert layout.format_category == "timeseries"

    def test_detects_misc_etf(self, tmp_path):
        f = tmp_path / "misc_etf.csv"
        f.write_text(MISC_ETF_SAMPLE, encoding="utf-8")
        parser_cls, layout = detect_format(f)
        assert parser_cls is MiscParser
        assert layout.format_name == "misc_etf"
        assert layout.format_category == "misc"

    def test_unknown_raises(self, tmp_path):
        f = tmp_path / "unknown.csv"
        f.write_text(UNKNOWN_SAMPLE, encoding="utf-8")
        with pytest.raises(UnknownFormatError, match="Could not detect format"):
            detect_format(f)

    def test_empty_file_raises(self, tmp_path):
        f = tmp_path / "empty.csv"
        f.write_text("", encoding="utf-8")
        with pytest.raises(UnknownFormatError, match="empty"):
            detect_format(f)

    def test_timeseries_priority_over_misc(self, tmp_path):
        """Time series layouts should be checked before misc layouts."""
        layouts = load_all_layouts()
        priorities = [l.priority for l in layouts]
        ts_layouts = [l for l in layouts if l.format_category == "timeseries"]
        misc_layouts = [l for l in layouts if l.format_category == "misc"]
        if ts_layouts and misc_layouts:
            assert ts_layouts[0].priority < misc_layouts[0].priority


class TestCheckLayout:
    """Tests for _check_layout() internal function."""

    def test_matching_layout(self):
        layouts = load_all_layouts()
        ts_layout = next(l for l in layouts if l.format_name == "timeseries_wide")
        rows = [r.split(",") for r in TIMESERIES_WIDE_SAMPLE.strip().split("\n")]
        # Strip whitespace from cells
        rows = [[c.strip() for c in row] for row in rows]
        assert _check_layout(ts_layout, rows) is True

    def test_non_matching_layout(self):
        layouts = load_all_layouts()
        ts_layout = next(l for l in layouts if l.format_name == "timeseries_wide")
        rows = [r.split(",") for r in UNKNOWN_SAMPLE.strip().split("\n")]
        rows = [[c.strip() for c in row] for row in rows]
        assert _check_layout(ts_layout, rows) is False


class TestDetectFormatRealFiles:
    """Smoke tests against real input files (skipped if files don't exist)."""

    def test_ohlcv_csv(self):
        from tests.conftest import WIDE_OHLCV_CSV
        if not WIDE_OHLCV_CSV.exists():
            pytest.skip(f"File not found: {WIDE_OHLCV_CSV}")
        parser_cls, layout = detect_format(WIDE_OHLCV_CSV)
        assert parser_cls is TimeSeriesWideParser
        assert layout.format_name == "timeseries_wide"

    def test_etf_csv(self):
        from tests.conftest import LONG_ETF_CSV
        if not LONG_ETF_CSV.exists():
            pytest.skip(f"File not found: {LONG_ETF_CSV}")
        parser_cls, layout = detect_format(LONG_ETF_CSV)
        assert parser_cls is MiscParser
        assert layout.format_name == "misc_etf"

    def test_sales_consensus_csv(self):
        from tests.conftest import WIDE_SALES_CSV
        if not WIDE_SALES_CSV.exists():
            pytest.skip(f"File not found: {WIDE_SALES_CSV}")
        parser_cls, layout = detect_format(WIDE_SALES_CSV)
        assert parser_cls is TimeSeriesWideParser
