"""Exception hierarchy for the cTrader transport.

Deliberately free of protobuf imports so that callers can catch these without depending on
the generated bindings.
"""

from __future__ import annotations


class CTraderError(Exception):
    """Base class for every error raised by this adapter."""


class CTraderConnectionError(CTraderError):
    """The connection is down, was lost, or was never established."""


class CTraderProtocolError(CTraderError):
    """A frame or payload violated the wire protocol."""


class CTraderTimeoutError(CTraderError):
    """No response arrived for a request within its timeout."""


class CTraderAuthError(CTraderError):
    """Application or account authentication was rejected, or a token was invalidated."""


class CTraderRequestError(CTraderError):
    """The venue rejected a request.

    Carries the fields a caller needs to decide whether retrying makes sense.
    `retry_after_secs` is set when the venue reports a rate-limit breach.
    """

    def __init__(
        self,
        error_code: str,
        description: str | None = None,
        *,
        maintenance_end_secs: int | None = None,
        retry_after_secs: int | None = None,
    ) -> None:
        super().__init__(f"{error_code}: {description}" if description else error_code)
        self.error_code = error_code
        self.description = description
        self.maintenance_end_secs = maintenance_end_secs
        self.retry_after_secs = retry_after_secs
