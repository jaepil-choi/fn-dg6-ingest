"""
Demo script: process all three test DataGuide 6 files via the public API.

Usage:
    uv run python scripts/run_ingest.py

Each input file gets its own output subdirectory and fnconfig.yaml under outputs/.
After the first run, you can edit the generated YAML configs and re-run to
rebuild with custom table groupings.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

INPUT_FILES = [
    "inputs/dataguide_etfconst(kodex200)_from(20250101)_to(20260207).csv",
    "inputs/dataguide_kse+kosdaq_ohlcv_from(20160101)_to(20260207).csv",
    "inputs/dataguide_kse+kosdaq_sales-consensus_from(20180101)_to(20260207).csv",
]

OUTPUT_ROOT = Path("outputs")

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("run_ingest")


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _derive_name(input_path: str) -> str:
    """Derive a short directory/config name from the input filename."""
    stem = Path(input_path).stem  # e.g. dataguide_kse+kosdaq_ohlcv_from(...)_to(...)
    # Take the descriptive middle part between 'dataguide_' and '_from('
    name = stem
    if name.startswith("dataguide_"):
        name = name[len("dataguide_"):]
    idx = name.find("_from(")
    if idx > 0:
        name = name[:idx]
    return name


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    from fn_dg6_ingest import init, ingest

    for input_path in INPUT_FILES:
        if not Path(input_path).exists():
            log.warning("SKIP  %s  (file not found)", input_path)
            continue

        name = _derive_name(input_path)
        output_dir = OUTPUT_ROOT / name
        config_path = OUTPUT_ROOT / f"{name}.yaml"

        log.info("=" * 70)
        log.info("Processing: %s", input_path)
        log.info("  output_dir  : %s", output_dir)
        log.info("  config_path : %s", config_path)
        log.info("=" * 70)

        if config_path.exists():
            # Subsequent run: rebuild from existing config
            log.info("Config exists -- running ingest()")
            written = ingest(config_path=str(config_path))
        else:
            # First run: detect, generate config, build
            log.info("No config -- running init()")
            init(
                input_path=input_path,
                output_dir=str(output_dir),
                config_path=str(config_path),
                run_immediately=True,
            )
            written = []  # init logs its own output

        if written:
            log.info("Written files:")
            for p in written:
                log.info("  %s", p)

        log.info("Done: %s\n", name)

    log.info("All files processed.")


if __name__ == "__main__":
    main()
