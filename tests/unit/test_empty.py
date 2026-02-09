"""
Unit tests for empty entity dropping (fn_dg6_ingest.transforms.empty).

Tests that entities with all-null data are correctly identified and dropped,
and that entity counts are accurate.
"""

import pytest


class TestDropEmptyEntities:
    """Tests for drop_empty_entities()."""

    # TODO: Test entity with all NaN -> dropped
    # TODO: Test entity with some data -> kept
    # TODO: Test entity counts in DropResult
    # TODO: Test with custom entity_column
    pass
