"""Configuration for the cTrader clients.

The application passes a typed config object; nothing here reads a file or an environment
variable. Credentials live in the config only to be handed to the account client, and are
never logged.
"""

from __future__ import annotations

from collections.abc import Mapping

from nautilus_trader.config import (
    InstrumentProviderConfig,
    LiveDataClientConfig,
    LiveExecClientConfig,
)
from nautilus_trader.model.enums import AssetClass

from nautilus_ctrader.common.account import ENVIRONMENTS, AccountCredentials, Environment


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


class CTraderDataClientConfig(LiveDataClientConfig, kw_only=True, frozen=True):
    """
    Configuration for `CTraderDataClient` instances.

    Parameters
    ----------
    client_id, client_secret : str
        The registered application's credentials.
    access_token : str
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
        How often to retry subscriptions whose restore failed.

    Raises
    ------
    ValueError
        If `history_page_size` is below 1, or `environment` is not one of "auto", "demo",
        "live".

    """

    client_id: str
    client_secret: str
    access_token: str
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

    def __post_init__(self) -> None:
        if self.history_page_size < 1:
            raise ValueError(
                f"history_page_size must be at least 1, got {self.history_page_size}",
            )
        # msgspec does not enforce the `Literal` on direct construction, and the account
        # client checks it only when it is the one building the client.
        if self.environment not in ENVIRONMENTS:
            raise ValueError(
                f"environment must be one of {ENVIRONMENTS}, got {self.environment!r}",
            )

    def credentials(self) -> AccountCredentials:
        return AccountCredentials(
            client_id=self.client_id,
            client_secret=self.client_secret,
            access_token=self.access_token,
            refresh_token=self.refresh_token,
            token_expires_at=self.token_expires_at,
        )


class CTraderExecClientConfig(LiveExecClientConfig, kw_only=True, frozen=True):
    """
    Configuration for `CTraderExecutionClient` instances.

    It carries no instrument settings: the execution client trades the instruments of the
    account's provider, which the data client's configuration builds.

    Parameters
    ----------
    client_id, client_secret : str
        The registered application's credentials.
    access_token : str
        An access token granting this application the account.
    trader_login : int
        The account number the broker gave you, as for `CTraderDataClientConfig`.
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
    reference_price_max_age_secs : float, default 10.0
        The oldest spot a bracket's levels may be measured from. The venue takes a market
        order's levels only as distances, which it applies to the fill.
    protective_order_timeout_secs : float, default 2.0
        How long after a bracket's fill to wait for the broker's protective order before
        setting the levels by an amend.
    order_request_timeout_secs : float, default 30.0
        Response timeout for an order, a close or a level amend. An order or a close with no
        answer by then is never resent, so a slow answer must not be taken for a lost one.

    Raises
    ------
    ValueError
        If `instrument_provider` is set, `environment` is not one of "auto", "demo", "live", or
        a duration is not positive.

    Notes
    -----
    Every client of one account shares one connection: the connection settings of the first
    config built for the account win.

    """

    client_id: str
    client_secret: str
    access_token: str
    trader_login: int
    refresh_token: str | None = None
    token_expires_at: float | None = None
    environment: Environment = "auto"
    connect_timeout_secs: float = 60.0
    restore_retry_interval_secs: float = 30.0
    reference_price_max_age_secs: float = 10.0
    protective_order_timeout_secs: float = 2.0
    order_request_timeout_secs: float = 30.0

    def __post_init__(self) -> None:
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
        for name in (
            "reference_price_max_age_secs",
            "protective_order_timeout_secs",
            "order_request_timeout_secs",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive, got {getattr(self, name)}")

    def credentials(self) -> AccountCredentials:
        return AccountCredentials(
            client_id=self.client_id,
            client_secret=self.client_secret,
            access_token=self.access_token,
            refresh_token=self.refresh_token,
            token_expires_at=self.token_expires_at,
        )
