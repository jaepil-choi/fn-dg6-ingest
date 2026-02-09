"""
Integration tests: time series data end-to-end.

Runs the full init() -> ingest() pipeline against real time series
CSV/XLSX files (OHLCV, sales-consensus).
Uses tmp_path for output to avoid polluting the workspace.
"""

import pytest


@pytest.mark.integration
class TestTimeSeriesPipeline:
    """End-to-end tests for time series wide data."""

    # TODO: Test init() generates fnconfig.yaml with correct items
    # TODO: Test ingest() produces expected output tables
    # TODO: Test _meta table is produced with correct schema
    # TODO: Test table splitting (edit config, re-run ingest)
    pass
