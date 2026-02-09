"""
Custom exception hierarchy for fn-dg6-ingest.

Why a custom hierarchy:
- Callers can catch specific exceptions (e.g., UnknownFormatError vs
  ConfigValidationError) without relying on generic ValueError/RuntimeError.
- Error messages are tailored to DataGuide 6 ingestion context, making
  debugging easier for users unfamiliar with the file formats.
"""


class FnDg6IngestError(Exception):
    """Base exception for all fn-dg6-ingest errors."""


class UnknownFormatError(FnDg6IngestError):
    """Raised when the input file does not match any known DataGuide 6 format.

    Typically includes a snippet of the file's first few lines to aid debugging.
    """


class ConfigValidationError(FnDg6IngestError):
    """Raised when fnconfig.yaml fails validation.

    This can happen if:
    - Required fields are missing or have wrong types.
    - Referenced 아이템명 do not exist in the source data.
    - output_format is not a supported value.
    """


class ParsingError(FnDg6IngestError):
    """Raised when a parser encounters unexpected data structure.

    For example, if the wide-format parser cannot find the expected
    data header row, or if column counts are inconsistent.
    """


class ExportError(FnDg6IngestError):
    """Raised when the exporter fails to write output files.

    For example, permission errors, disk full, or unsupported format.
    """
