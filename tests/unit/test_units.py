"""
Unit tests for unit normalization (fn_dg6_ingest.transforms.units).

Tests unit suffix detection, scaling, and column renaming.
"""

import pytest


class TestDetectUnit:
    """Tests for detect_unit()."""

    # TODO: Test (원) -> multiplier 1
    # TODO: Test (천원) -> multiplier 1000
    # TODO: Test (백만원) -> multiplier 1_000_000
    # TODO: Test (억원) -> multiplier 100_000_000
    # TODO: Test (십억원) -> multiplier 1_000_000_000
    # TODO: Test (주) -> no scaling (non-monetary)
    # TODO: Test (%) -> no scaling
    # TODO: Test no suffix -> no scaling
    pass


class TestNormalizeColumnName:
    """Tests for normalize_column_name()."""

    # TODO: Test renaming "매출액(천원)" -> "매출액(원)"
    pass
