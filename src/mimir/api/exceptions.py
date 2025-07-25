"""
Custom exception types for the Mimir application.
"""


class MimirError(Exception):
    """Base class for all Mimir-specific exceptions."""

    pass


class MimirConfigError(MimirError):
    """Raised for errors related to configuration."""

    pass


class MimirQueryError(MimirError):
    """Raised for errors related to query construction or validation."""

    pass


class MimirConnectionError(MimirError):
    """Raised for errors related to data source connections."""

    pass


class MimirNotImplementedError(MimirError, NotImplementedError):
    """Raised for features that are not yet implemented."""

    pass
