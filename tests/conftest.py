"""
Shared test fixtures and path constants for fn-dg6-ingest tests.

All input file paths are defined here as module-level constants for
easy discovery and modification. If input files move or new ones are
added, update this file.
"""

from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Input file paths -- edit here if files move or new ones are added
# ---------------------------------------------------------------------------
INPUT_DIR = Path(__file__).resolve().parent.parent / "inputs"

WIDE_OHLCV_CSV = INPUT_DIR / "dataguide_kse+kosdaq_ohlcv_from(20160101)_to(20260207).csv"
WIDE_OHLCV_XLSX = INPUT_DIR / "dataguide_kse+kosdaq_ohlcv_from(20160101)_to(20260207).xlsx"
LONG_ETF_CSV = INPUT_DIR / "dataguide_etfconst(kodex200)_from(20250101)_to(20260207).csv"
LONG_ETF_XLSX = INPUT_DIR / "dataguide_etfconst(kodex200)_from(20250101)_to(20260207).xlsx"
WIDE_SALES_CSV = INPUT_DIR / "dataguide_kse+kosdaq_sales-consensus_from(20180101)_to(20260207).csv"
EVENT_STUDY_XLSX = INPUT_DIR / "dataguide_eventstudy_earningsurprise_from(20160101)_to(20260207).xlsx"


# ---------------------------------------------------------------------------
# Pytest markers
# ---------------------------------------------------------------------------
def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "integration: mark test as integration test (runs against real input files)",
    )
