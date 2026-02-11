"""
Parsers sub-package for fn-dg6-ingest.

Contains format-specific parsers that convert raw DataGuide 6 exports
into a standardized intermediate representation (DataFrame + metadata).

Design: Strategy Pattern
- base.py defines the BaseParser ABC (protocol).
- timeseries.py implements TimeSeriesWideParser for pivot-style time series data.
- snapshot.py implements SnapshotParser (placeholder) for snapshot-style data.
- misc.py implements MiscParser for custom/miscellaneous formats (e.g., ETF constituents).

Parser taxonomy mirrors DataGuide 6's top-level categories:
  timeseries -> snapshot -> misc

Each parser receives a Layout object from the detector, which provides
cell coordinates for metadata extraction (no heuristic scanning).

The FormatDetector (detect.py) selects the appropriate parser + layout at runtime.
"""
