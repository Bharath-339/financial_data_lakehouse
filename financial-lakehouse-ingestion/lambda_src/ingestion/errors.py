class IngestionError(Exception):
    """Base class for expected ingestion failures."""


class ConfigurationError(IngestionError):
    """The invocation or runtime configuration is invalid."""


class SourceAPIError(IngestionError):
    """An external data provider returned an unsuccessful response."""


class ConcurrentInvocationError(IngestionError):
    """Another invocation currently owns the same ingestion checkpoint."""


class PaginationLimitError(IngestionError):
    """A paginated source exceeded the configured safety limit."""
