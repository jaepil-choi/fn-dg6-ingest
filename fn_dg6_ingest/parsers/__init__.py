"""
Parsers sub-package for fn-dg6-ingest.

Contains format-specific parsers that convert raw DataGuide 6 exports
into a standardized intermediate representation (DataFrame + metadata).

Design: Strategy Pattern
- base.py defines the BaseParser ABC (protocol).
- wide.py implements WideFormatParser for pivot-style data (dates as columns).
- long.py implements LongFormatParser for normal-form data (dates as rows).

The FormatDetector (detect.py) selects the appropriate parser at runtime.
"""
