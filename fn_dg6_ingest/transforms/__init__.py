"""
Transforms sub-package for fn-dg6-ingest.

Contains composable transformation steps that form the processing pipeline.
Each transform is a callable that takes a DataFrame (+ config) and returns
a transformed DataFrame.

Design: Pipeline Pattern
- pipeline.py orchestrates the sequence of transforms.
- Individual transforms are in separate modules for testability:
  - numbers.py: Strip commas, parse numeric strings.
  - units.py: Detect unit suffixes, scale values, rename columns.
  - empty.py: Drop entities (코드) with all-null data.
  - splitter.py: Split a single DataFrame into multiple tables by 아이템명 groups.

Why composable steps:
- Each step is independently testable.
- Steps can be reordered, added, or skipped based on config.
- New transforms (e.g., currency conversion) can be added without modifying existing code.
"""
