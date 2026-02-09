"""
Unit tests for table splitter (fn_dg6_ingest.transforms.splitter).

Tests that DataFrames are correctly split into groups, key columns
are always included, and single-group returns the full DataFrame.
"""

import pytest


class TestSplitTables:
    """Tests for split_tables()."""

    # TODO: Test single default group -> returns full DataFrame
    # TODO: Test two groups -> each has correct columns + key columns
    # TODO: Test missing 아이템명 in group -> expected behavior
    pass
