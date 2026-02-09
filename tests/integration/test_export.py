"""
Integration tests: export verification.

Tests that output files (CSV and Parquet) are correctly written,
loadable, and contain expected data.
"""

import pytest


@pytest.mark.integration
class TestExport:
    """Tests for CSV and Parquet export."""

    # TODO: Test Parquet output is loadable and has correct dtypes
    # TODO: Test CSV output is loadable and has correct content
    # TODO: Test _meta table is written alongside data tables
    pass
