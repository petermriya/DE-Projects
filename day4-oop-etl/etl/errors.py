"""
Custom exception hierarchy for the ETL framework.
"""


class ETLError(Exception):
    """Base class for every error raised by the ETL framework."""


class ExtractionError(ETLError):
    """Raised when a source cannot be read."""


class LoadingError(ETLError):
    """Raised when the output destination cannot be written, or when a
    destination uses an unsupported file extension."""


class ValidationError(ETLError):
    """Reserved for record-level validation failures."""
