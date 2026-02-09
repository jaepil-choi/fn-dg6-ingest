"""
Integration tests: misc/custom ETF constituent data end-to-end.

Runs the full init() -> ingest() pipeline against real ETF constituent
CSV/XLSX files. Uses tmp_path for output to avoid polluting the workspace.
"""

import pytest


@pytest.mark.integration
class TestMiscEtfPipeline:
    """End-to-end tests for misc ETF constituent data."""

    # TODO: Test init() generates fnconfig.yaml
    # TODO: Test ingest() produces expected output table
    # TODO: Test _meta table contents
    pass
