"""Configuration for the cTrader clients.

The application passes a typed config object; nothing here reads a file. A secret left out of
the config is read from its environment variable when the account client is built, so a config
Nautilus writes to disk carries only the secrets the application put in it. Credentials are
never logged.
"""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from typing import Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from nautilus_trader.config import (
    InstrumentProviderConfig,
    LiveDataClientConfig,
    LiveExecClientConfig,
)
from nautilus_trader.model.enums import AssetClass

from nautilus_ctrader.common.account import ENVIRONMENTS, AccountCredentials, Environment
from nautilus_ctrader.common.rate_limit import Pacing
from nautilus_ctrader.constants import (
    RATE_LIMIT_MAX_PAUSE_SECS,
    RATE_LIMIT_RETRIES,
    REQUEST_QUEUE_TIMEOUT_SECS,
    REQUEST_WINDOW_SECS,
    VENUE_HISTORICAL_REQUESTS_PER_SEC,
    VENUE_REQUESTS_PER_SEC,
)


def parse_asset_class_overrides(overrides: Mapping[str, str]) -> dict[str, AssetClass]:
    """`AssetClass` per broker symbol name, from the configured class names.

    Raises `ValueError` if a value is not the name of a Nautilus `AssetClass`.
    """
    parsed = {}
    for symbol, name in overrides.items():
        try:
            parsed[symbol] = AssetClass[name]
        except KeyError:
            valid = ", ".join(sorted(AssetClass.__members__))
            raise ValueError(
                f"asset_class_overrides[{symbol!r}]: {name!r} is not an AssetClass ({valid})",
            ) from None
    return parsed


class _SecretSettings(Protocol):
    client_id: str | None
    client_secret: str | None
    access_token: str | None
    refresh_token: str | None
    token_expires_at: float | None


def _from_environment(value: str | None, variable: str) -> str | None:
    if value is not None:
        return value
    # An empty variable counts as unset: no credential is the empty string.
    return os.environ.get(variable) or None


def _required(value: str | None, setting: str, variable: str) -> str:
    resolved = _from_environment(value, variable)
    if resolved is None:
        raise ValueError(f"{setting} is not set: set it in the config or in {variable}")
    return resolved


def _expiry(value: float | None) -> float | None:
    if value is not None:
        return value
    text = _from_environment(None, "CTRADER_TOKEN_EXPIRES_AT")
    if text is None:
        return None
    unreadable = ValueError(
        "token_expires_at: CTRADER_TOKEN_EXPIRES_AT is not a number of Unix seconds",
    )
    try:
        expires_at = float(text)
    except ValueError:
        raise unreadable from None
    if not math.isfinite(expires_at):
        raise unreadable
    return expires_at


def _resolve_credentials(config: _SecretSettings) -> AccountCredentials:
    """The config's credentials, each one it leaves `None` read from its environment variable.

    The refresh token and the expiry are read from the environment only when the access token
    is: an inline grant is never completed with another's.

    Raises `ValueError`, naming the setting and the variable but never a value, if the client
    id, client secret or access token is set in neither, or `CTRADER_TOKEN_EXPIRES_AT` is not
    a finite number.
    """
    grant_from_environment = config.access_token is None
    return AccountCredentials(
        client_id=_required(config.client_id, "client_id", "CTRADER_CLIENT_ID"),
        client_secret=_required(config.client_secret, "client_secret", "CTRADER_CLIENT_SECRET"),
        access_token=_required(config.access_token, "access_token", "CTRADER_ACCESS_TOKEN"),
        refresh_token=(
            _from_environment(config.refresh_token, "CTRADER_REFRESH_TOKEN")
            if grant_from_environment
            else config.refresh_token
        ),
        token_expires_at=(
            _expiry(config.token_expires_at) if grant_from_environment else config.token_expires_at
        ),
    )


def _require_positive(config: object, *names: str) -> None:
    """Raises `ValueError` for the first of the `names` settings that is not positive, NaN too."""
    for name in names:
        value = getattr(config, name)
        if not (value > 0):
            raise ValueError(f"{name} must be positive, got {value}")


class CTraderDataClientConfig(LiveDataClientConfig, kw_only=True, frozen=True):
    """
    Configuration for `CTraderDataClient` instances.

    Parameters
    ----------
    client_id, client_secret : str, optional
        The registered application's credentials.
    access_token : str, optional
        An access token granting this application the account.
    trader_login : int
        The account number the broker gave you, the one the cTrader interface shows. It is
        not the `ctidTraderAccountId` the protocol addresses internally; the adapter looks
        that one up from the accounts the token grants.
    refresh_token : str, optional
        Used to renew `access_token`; without it an expired token ends the session.
    token_expires_at : float, optional
        Unix seconds the access token expires at. Without it no proactive refresh happens.
    environment : str, default "auto"
        `"auto"` reads the account's own live flag and picks the host; `"demo"`/`"live"` force it.
    subscribe_conversion_quotes : bool, default True
        Subscribe the chain converting each instrument's quote currency into the deposit
        currency, so Nautilus can price positions in the account currency.
    fail_on_instrument_error : bool, default False
        Fail `connect` if any requested instrument cannot be loaded or converted, instead of
        carrying on without it.
    asset_class_overrides : dict[str, str], default {}
        `AssetClass` name per broker symbol name, e.g. `{"US100.cash": "INDEX"}`. Indices need
        one to be anything but `ALTERNATIVE`; ignored for symbols that resolve to a currency
        pair.
    synthetic_quote_size : float, optional
        The size to put on both sides of every quote tick. The venue's spot events carry no
        size; without this they are emitted as zero.
    bar_close_grace_secs : float, default 1.0
        How long after a bar's end to wait for the stream to close it before asking history.
    bar_close_history_retries : int, default 3
        How many times to re-ask history for a bar it has not served yet.
    history_page_size : int, default 500
        Bars per historical request; at least 1, since it also sizes the window each request
        covers.
    history_request_timeout_secs : float, default 30.0
        Response timeout for a historical request.
    connect_timeout_secs : float, default 60.0
        Bound on the whole account bring-up.
    restore_retry_interval_secs : float, default 30.0
        How often to retry subscriptions whose restore failed, and a conversion chain refused
        during a subscribe.
    request_window_secs : float, default 1.25
        The sliding window the request limits count within.
    historical_requests_per_window : int, default 5
        The most historical requests sent within one window.
    requests_per_window : int, default 50
        The most other requests sent within one window.
    rate_limit_retries : int, default 3
        Resends of a historical request the venue refused for its rate limit.
    request_queue_timeout_secs : float, default 60.0
        The longest a request waits in the queue, retries included, before it fails unsent.
    rate_limit_max_pause_secs : float, default 10.0
        The longest pause after a rate-limit refusal. A longer `retryAfter` is cut to it and
        logged as an error.

    Raises
    ------
    ValueError
        If `history_page_size` is below 1, `environment` is not one of "auto", "demo",
        "live", a duration is not positive, `bar_close_history_retries` is negative, or the
        request pacing is impossible (a count below 1, a window, queue timeout or pause cap
        that is not positive, negative retries).

    Notes
    -----
    Every client of one account shares one connection: the connection settings of the first
    config built for the account win, the request pacing included.

    Each of the five credentials left `None` is read when the account client is built, from
    `CTRADER_CLIENT_ID`, `CTRADER_CLIENT_SECRET`, `CTRADER_ACCESS_TOKEN`,
    `CTRADER_REFRESH_TOKEN` or `CTRADER_TOKEN_EXPIRES_AT`. A value in the config wins. The
    last two are read only when the access token is too, never to complete an inline one.

    """

    client_id: str | None = None
    client_secret: str | None = None
    access_token: str | None = None
    trader_login: int
    refresh_token: str | None = None
    token_expires_at: float | None = None
    environment: Environment = "auto"
    subscribe_conversion_quotes: bool = True
    fail_on_instrument_error: bool = False
    asset_class_overrides: dict[str, str] = {}  # noqa: RUF012
    synthetic_quote_size: float | None = None
    bar_close_grace_secs: float = 1.0
    bar_close_history_retries: int = 3
    # TODO(verify): the venue's maximum bars per historical request.
    history_page_size: int = 500
    history_request_timeout_secs: float = 30.0
    connect_timeout_secs: float = 60.0
    restore_retry_interval_secs: float = 30.0
    request_window_secs: float = REQUEST_WINDOW_SECS
    historical_requests_per_window: int = VENUE_HISTORICAL_REQUESTS_PER_SEC
    requests_per_window: int = VENUE_REQUESTS_PER_SEC
    rate_limit_retries: int = RATE_LIMIT_RETRIES
    request_queue_timeout_secs: float = REQUEST_QUEUE_TIMEOUT_SECS
    rate_limit_max_pause_secs: float = RATE_LIMIT_MAX_PAUSE_SECS

    def __post_init__(self) -> None:
        self.pacing()
        if self.history_page_size < 1:
            raise ValueError(
                f"history_page_size must be at least 1, got {self.history_page_size}",
            )
        _require_positive(
            self,
            "bar_close_grace_secs",
            "history_request_timeout_secs",
            "connect_timeout_secs",
            "restore_retry_interval_secs",
        )
        if self.bar_close_history_retries < 0:
            raise ValueError(
                "bar_close_history_retries must not be negative, "
                f"got {self.bar_close_history_retries}",
            )
        # msgspec does not enforce the `Literal` on direct construction, and the account
        # client checks it only when it is the one building the client.
        if self.environment not in ENVIRONMENTS:
            raise ValueError(
                f"environment must be one of {ENVIRONMENTS}, got {self.environment!r}",
            )

    def credentials(self) -> AccountCredentials:
        """The credentials, those left `None` read from the environment.

        Raises `ValueError` if a required one is set in neither.
        """
        return _resolve_credentials(self)

    def pacing(self) -> Pacing:
        """The request pacing settings; raises `ValueError` if they are impossible."""
        return Pacing(
            requests_per_window=self.requests_per_window,
            historical_requests_per_window=self.historical_requests_per_window,
            window_secs=self.request_window_secs,
            retries=self.rate_limit_retries,
            queue_timeout_secs=self.request_queue_timeout_secs,
            max_pause_secs=self.rate_limit_max_pause_secs,
        )


class CTraderExecClientConfig(LiveExecClientConfig, kw_only=True, frozen=True):
    """
    Configuration for `CTraderExecutionClient` instances.

    It carries no instrument settings: the execution client trades the instruments of the
    account's provider, which the data client's configuration builds.

    Parameters
    ----------
    client_id, client_secret : str, optional
        The registered application's credentials.
    access_token : str, optional
        An access token granting this application the account.
    trader_login : int
        The account number the broker gave you, as for `CTraderDataClientConfig`. The Nautilus
        account id is built from it: `CTRADER-<trader_login>`.
    refresh_token : str, optional
        Used to renew `access_token`.
    token_expires_at : float, optional
        Unix seconds the access token expires at.
    environment : str, default "auto"
        `"auto"` reads the account's own live flag and picks the host; `"demo"`/`"live"` force it.
    connect_timeout_secs : float, default 60.0
        Bound on the whole account bring-up.
    restore_retry_interval_secs : float, default 30.0
        How often to retry what a reconnect failed to restore.
    request_window_secs : float, default 1.25
        The sliding window the request limits count within.
    historical_requests_per_window : int, default 5
        The most historical requests sent within one window.
    requests_per_window : int, default 50
        The most other requests sent within one window.
    rate_limit_retries : int, default 3
        Resends of a historical request the venue refused for its rate limit.
    request_queue_timeout_secs : float, default 60.0
        The longest a request waits in the queue, retries included, before it fails unsent.
    rate_limit_max_pause_secs : float, default 10.0
        The longest pause after a rate-limit refusal. A longer `retryAfter` is cut to it and
        logged as an error.
    reference_price_max_age_secs : float, default 10.0
        The oldest spot a bracket's levels may be measured from, by the time since it arrived.
        The venue takes a market order's levels only as distances, which it applies to the fill.
    protective_order_timeout_secs : float, default 2.0
        How long after a bracket's fill to wait for the broker's protective order before
        setting the levels by an amend.
    order_request_timeout_secs : float, default 30.0
        Response timeout for an order, a close or a level amend. An order or a close with no
        answer by then is never resent, so a slow answer must not be taken for a lost one.
    reconciliation_default_lookback_mins : int, default 1440
        How far back reconciliation reads fills and closed orders when Nautilus passes no
        lookback of its own.
    balance_checkpoint_hour : int, optional
        The hour of day, 0-23, at which the balance is kept in the cache key
        `ctrader.balance_checkpoint`, rebuilt from the broker's history. Unset, the key says
        `"off"`.
    balance_checkpoint_timezone : str, default "UTC"
        The IANA time zone `balance_checkpoint_hour` is in.

    Raises
    ------
    ValueError
        If `instrument_provider` is set, `environment` is not one of "auto", "demo", "live", a
        duration or `reconciliation_default_lookback_mins` is not positive,
        `balance_checkpoint_hour` is outside 0-23, `balance_checkpoint_timezone` is not a
        known time zone, or the request pacing is impossible (a count below 1, a window,
        queue timeout or pause cap that is not positive, negative retries).

    Notes
    -----
    Every client of one account shares one connection: the connection settings of the first
    config built for the account win, the request pacing included.

    Credentials left `None` are read from the environment as for `CTraderDataClientConfig`.

    """

    client_id: str | None = None
    client_secret: str | None = None
    access_token: str | None = None
    trader_login: int
    refresh_token: str | None = None
    token_expires_at: float | None = None
    environment: Environment = "auto"
    connect_timeout_secs: float = 60.0
    restore_retry_interval_secs: float = 30.0
    request_window_secs: float = REQUEST_WINDOW_SECS
    historical_requests_per_window: int = VENUE_HISTORICAL_REQUESTS_PER_SEC
    requests_per_window: int = VENUE_REQUESTS_PER_SEC
    rate_limit_retries: int = RATE_LIMIT_RETRIES
    request_queue_timeout_secs: float = REQUEST_QUEUE_TIMEOUT_SECS
    rate_limit_max_pause_secs: float = RATE_LIMIT_MAX_PAUSE_SECS
    reference_price_max_age_secs: float = 10.0
    protective_order_timeout_secs: float = 2.0
    order_request_timeout_secs: float = 30.0
    reconciliation_default_lookback_mins: int = 1440
    balance_checkpoint_hour: int | None = None
    balance_checkpoint_timezone: str = "UTC"

    def __post_init__(self) -> None:
        self.pacing()
        if self.instrument_provider != InstrumentProviderConfig():
            raise ValueError(
                "instrument_provider is set on the execution config; the execution client "
                "trades the instruments of the account's provider, which the data client's "
                "config builds",
            )
        if self.environment not in ENVIRONMENTS:
            raise ValueError(
                f"environment must be one of {ENVIRONMENTS}, got {self.environment!r}",
            )
        _require_positive(
            self,
            "connect_timeout_secs",
            "restore_retry_interval_secs",
            "reference_price_max_age_secs",
            "protective_order_timeout_secs",
            "order_request_timeout_secs",
            "reconciliation_default_lookback_mins",
        )
        hour = self.balance_checkpoint_hour
        if hour is not None and not 0 <= hour <= 23:
            raise ValueError(f"balance_checkpoint_hour must be 0-23, got {hour}")
        self.checkpoint_zone()

    def checkpoint_zone(self) -> ZoneInfo:
        """`balance_checkpoint_timezone` as a zone; raises `ValueError` for an unknown name."""
        name = self.balance_checkpoint_timezone
        try:
            return ZoneInfo(name)
        except (ZoneInfoNotFoundError, ValueError) as e:
            raise ValueError(
                f"balance_checkpoint_timezone: {name!r} is not a known time zone"
            ) from e

    def credentials(self) -> AccountCredentials:
        """The credentials, those left `None` read from the environment.

        Raises `ValueError` if a required one is set in neither.
        """
        return _resolve_credentials(self)

    def pacing(self) -> Pacing:
        """The request pacing settings; raises `ValueError` if they are impossible."""
        return Pacing(
            requests_per_window=self.requests_per_window,
            historical_requests_per_window=self.historical_requests_per_window,
            window_secs=self.request_window_secs,
            retries=self.rate_limit_retries,
            queue_timeout_secs=self.request_queue_timeout_secs,
            max_pause_secs=self.rate_limit_max_pause_secs,
        )
