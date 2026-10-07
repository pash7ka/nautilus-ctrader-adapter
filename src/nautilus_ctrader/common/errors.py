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


# The OAuth errors below have fixed messages: no token, code, client id or secret ever reaches
# their text or arguments. What varies is carried in fields, already sanitised for a terminal.


class CTraderAuthorizationDenied(CTraderAuthError):
    """The authorization redirect carried an error instead of a code.

    `error_code` is the redirect's `error` value.
    """

    def __init__(self, error_code: str) -> None:
        super().__init__("the authorization redirect carried an error instead of a code")
        self.error_code = error_code


class CTraderAuthorizationTimeout(CTraderAuthError):
    """No authorization redirect arrived within `timeout_secs`."""

    def __init__(self, timeout_secs: float) -> None:
        super().__init__(f"no authorization redirect received within {timeout_secs:g}s")
        self.timeout_secs = timeout_secs


class CTraderTokenExchangeError(CTraderAuthError):
    """The token endpoint refused the code, could not be reached, or answered unusably.

    - `error_code`, `description`: the endpoint's own refusal fields, when it sent them;
    - `http_status`: set when the endpoint answered with an HTTP error.
    """

    def __init__(
        self,
        message: str,
        *,
        error_code: str | None = None,
        description: str | None = None,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.description = description
        self.http_status = http_status


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


class CTraderAccountError(CTraderError):
    """The account cannot be traded through this adapter: its type or rights do not allow it."""
