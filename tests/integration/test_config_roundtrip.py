"""
Integration tests: config round-trip workflow.

Tests the full cycle: init() -> edit config -> ingest() rebuild.
Verifies that config modifications (e.g., splitting tables) are
correctly reflected in the rebuilt output.
"""

import pytest


@pytest.mark.integration
class TestConfigRoundtrip:
    """Tests for the config-first workflow."""

    # TODO: Test init() -> save config -> load config -> validate round-trip
    # TODO: Test edit table groups -> ingest() produces split tables
    # TODO: Test toggle normalize_units -> verify output values change
    pass
