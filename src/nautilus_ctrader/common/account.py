"""The per-account client: one session per trading account, shared by every Nautilus client.

It picks the host, owns the only `CTraderSession` for the account (and with it the only party
allowed to refresh tokens), and caches the account's reference data. Nothing about the account
it resolves - its id, login or broker - is ever logged or put into an error message.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import ssl
import time
from collections.abc import Callable, Hashable, Sequence
from dataclasses import dataclass, field
from typing import Literal, Protocol, get_args

from google.protobuf.message import Message
from nautilus_trader.common.component import Logger

from nautilus_ctrader.common.connection import CTraderConnection
from nautilus_ctrader.common.errors import (
    CTraderAuthError,
    CTraderConnectionError,
    CTraderProtocolError,
    CTraderRequestError,
    CTraderTimeoutError,
)
from nautilus_ctrader.common.session import CTraderSession
from nautilus_ctrader.common.subscriptions import SubscriptionRegistry
from nautilus_ctrader.constants import (
    DEMO_HOST,
    LIVE_HOST,
    PROTOBUF_PORT,
    SYMBOL_BY_ID_BATCH,
    TOKEN_ERROR_CODES,
)
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om

TokenListener = Callable[[str, str, float], None]

Environment = Literal["auto", "demo", "live"]
ENVIRONMENTS: tuple[Environment, ...] = get_args(Environment)
_READY_POLL_SECS = 0.5
# A restore still failing after this many background retries is reported once at ERROR.
_RESTORE_RETRY_ERROR_ATTEMPTS = 3


@dataclass(frozen=True)
class AccountCredentials:
    client_id: str
    client_secret: str = field(repr=False)
    access_token: str = field(repr=False)
    refresh_token: str | None = field(repr=False)
    # Unix seconds; `None` disables proactive refresh.
    token_expires_at: float | None


class CTraderAccountClient:
    """
    One trading account's connection, shared by reference count.

    - The account is named by its `trader_login`. Every bring-up lists the accounts the token
      grants over a short pre-connection to `demo_host` - the list is served on either host -
      and reads the `ctidTraderAccountId` the protocol needs off the matching entry.
    - `environment="auto"` takes the host from that entry's `isLive` flag; `"demo"`/`"live"`
      force it.
    - `connect()` returns once the session is ready and the reference data is loaded; bad
      credentials fail it at once rather than after `connect_timeout_secs`.
    """

    def __init__(
        self,
        *,
        trader_login: int,
        credentials: AccountCredentials,
        environment: Environment,
        logger: Logger,
        connect_timeout_secs: float = 60.0,
        restore_retry_interval_secs: float = 30.0,
        demo_host: str = DEMO_HOST,
        live_host: str = LIVE_HOST,
        port: int = PROTOBUF_PORT,
        tls: ssl.SSLContext | bool = True,
    ) -> None:
        if environment not in ENVIRONMENTS:
            raise ValueError(f"environment must be one of {ENVIRONMENTS}, got {environment!r}")
        self.trader_login = trader_login
        self._account_id: int | None = None
        self._credentials = credentials
        self._environment = environment
        self._log = logger
        self._connect_timeout_secs = connect_timeout_secs
        self._restore_retry_interval_secs = restore_retry_interval_secs
        self._demo_host = demo_host
        self._live_host = live_host
        self._port = port
        self._tls = tls

        self.session: CTraderSession | None = None
        self._token_listeners: list[TokenListener] = []
        self._users = 0
        # Serialises `connect()` and `disconnect()`, so at most one session is ever brought up.
        self._lifecycle_lock = asyncio.Lock()
        self._retry_task: asyncio.Task | None = None
        # Outlives sessions, so live subscriptions are restored on every later `connect()`.
        self.subscriptions = SubscriptionRegistry(self, logger)

        self._deposit_asset: om.ProtoOAAsset | None = None
        self._money_digits: int | None = None
        self.assets: dict[int, om.ProtoOAAsset] = {}
        self.light_symbols: dict[str, om.ProtoOALightSymbol] = {}
        self._symbol_specs: dict[int, om.ProtoOASymbol] = {}

    @property
    def account_id(self) -> int:
        """The `ctidTraderAccountId` every request carries, resolved from the trader login.

        A property rather than an attribute so a request built before the account list has
        been read fails here, instead of going out addressed to nothing.
        """
        if self._account_id is None:
            raise CTraderConnectionError("account id not resolved yet; connect() first")
        return self._account_id

    @property
    def deposit_asset(self) -> om.ProtoOAAsset:
        if self._deposit_asset is None:
            raise CTraderConnectionError("reference data not loaded; connect() first")
        return self._deposit_asset

    @property
    def money_digits(self) -> int:
        """Decimal places of every monetary value the venue reports for this account."""
        if self._money_digits is None:
            raise CTraderConnectionError("reference data not loaded; connect() first")
        return self._money_digits

    def add_token_listener(self, callback: TokenListener) -> None:
        """Register `callback(access_token, refresh_token, expires_at_secs)` for every refresh.

        The new pair exists nowhere else: the application must persist it, because the old
        refresh token may no longer work.
        """
        self._token_listeners.append(callback)

    async def connect(self) -> None:
        """Become a user of the account's session, bringing it up if this is the first user.

        Calls are serialised: a caller queued behind a successful attempt only counts itself,
        and one queued behind a failed attempt tries again. A failed or cancelled attempt
        stops what it started and leaves the user count unchanged.
        """
        async with self._lifecycle_lock:
            if self._users > 0:
                self._users += 1
                return
            await self._bring_up()
            # No await between a successful bring-up and this, so a cancellation cannot leave
            # a running session without a user.
            self._users += 1

    async def disconnect(self) -> None:
        """Release one user; the last one stops the session."""
        async with self._lifecycle_lock:
            if self._users == 0:
                return
            self._users -= 1
            if self._users > 0:
                return
            session, self.session = self.session, None
            await self._stop(session)
            self._log.info("Account session closed")

    async def request(self, payload: Message, *, timeout_secs: float | None = None) -> Message:
        if self.session is None:
            raise CTraderConnectionError("account client not connected")
        return await self.session.request(payload, timeout_secs=timeout_secs)

    async def symbol_specs(
        self,
        symbol_ids: Sequence[int],
        *,
        refresh: bool = False,
    ) -> dict[int, om.ProtoOASymbol]:
        """Full symbol entities by id, fetched once each and cached.

        `refresh` bypasses the cache for every id in `symbol_ids`, re-fetching and replacing
        the cached entry. Ids the venue does not return are absent from the result.
        """
        ids = list(dict.fromkeys(symbol_ids))
        missing = ids if refresh else [i for i in ids if i not in self._symbol_specs]
        for start in range(0, len(missing), SYMBOL_BY_ID_BATCH):
            response = await self.request(
                oa.ProtoOASymbolByIdReq(
                    ctidTraderAccountId=self.account_id,
                    symbolId=missing[start : start + SYMBOL_BY_ID_BATCH],
                ),
            )
            for symbol in response.symbol:
                self._symbol_specs[symbol.symbolId] = symbol
        return {i: self._symbol_specs[i] for i in symbol_ids if i in self._symbol_specs}

    async def conversion_chain(
        self,
        from_asset_id: int,
        to_asset_id: int,
    ) -> list[om.ProtoOALightSymbol]:
        """The symbols that convert `from_asset_id` into `to_asset_id`, in venue order."""
        response = await self.request(
            oa.ProtoOASymbolsForConversionReq(
                ctidTraderAccountId=self.account_id,
                firstAssetId=from_asset_id,
                lastAssetId=to_asset_id,
            ),
        )
        return list(response.symbol)

    async def _bring_up(self) -> None:
        # One bound for the whole bring-up, so a `disconnect()` queued behind a hanging
        # `connect()` waits no longer than `connect_timeout_secs`.
        deadline = asyncio.timeout(self._connect_timeout_secs)
        session: CTraderSession | None = None
        try:
            async with deadline:
                host = await self._resolve_account()
                session = self._build_session(host)
                # Before `start()`, so the first bring-up already restores live subscriptions.
                self.subscriptions.attach(session)
                self.session = session
                await session.start()
                await self._wait_ready(session)
                if self._deposit_asset is None:
                    await self._load_reference_data()
        except BaseException as e:
            self.session = None
            if session is not None:
                self.subscriptions.detach()
                await session.stop()
            if isinstance(e, TimeoutError) and deadline.expired():
                cause = session.last_error if session is not None else None
                raise CTraderTimeoutError(
                    f"connect did not complete within {self._connect_timeout_secs:g}s",
                ) from (cause or e)
            raise
        self._retry_task = asyncio.create_task(self._retry_restores_loop(session))
        self._log.info("Account session ready")

    async def _stop(self, session: CTraderSession | None) -> None:
        self.subscriptions.detach()
        retry_task, self._retry_task = self._retry_task, None
        if retry_task is not None:
            retry_task.cancel()
        # The session is stopped even if this is cancelled while the retry task winds down;
        # otherwise it would run on with nothing left to reach it.
        try:
            if retry_task is not None:
                await asyncio.wait({retry_task})
        finally:
            if session is not None:
                await session.stop()

    async def _resolve_account(self) -> str:
        """Set `self._account_id` from the granted account list, and return the host to use.

        The list is fetched whatever `environment` says, because it is the only thing that
        maps a trader login to the `ctidTraderAccountId` the protocol addresses.
        """
        connection = CTraderConnection(
            self._demo_host,
            self._port,
            logger=self._log,
            tls=self._tls,
        )
        try:
            await connection.connect()
            try:
                await connection.request(
                    oa.ProtoOAApplicationAuthReq(
                        clientId=self._credentials.client_id,
                        clientSecret=self._credentials.client_secret,
                    ),
                )
            except CTraderRequestError as e:
                raise CTraderAuthError(f"application auth rejected: {e.error_code}") from e
            accounts = await self._list_accounts(connection)
        finally:
            await connection.close()

        # The list also carries logins and broker names: it is read here and never logged.
        # TODO(verify): the venue populates `traderLogin` on every listed account. An entry
        # without it can never be matched, and this reports it as a login not granted.
        matched = [
            entry
            for entry in accounts.ctidTraderAccount
            if entry.HasField("traderLogin") and entry.traderLogin == self.trader_login
        ]
        if not matched:
            raise CTraderAuthError("no account with that trader login is granted to this token")
        if len(matched) > 1:
            # A login is unique per broker server, but one token can grant accounts on several
            # servers, so picking one here would be a guess at which account to trade.
            raise CTraderAuthError(
                "more than one granted account has that trader login; the account is ambiguous",
            )
        entry = matched[0]
        self._account_id = entry.ctidTraderAccountId
        if self._environment == "demo":
            return self._demo_host
        if self._environment == "live":
            return self._live_host
        return self._live_host if entry.isLive else self._demo_host

    async def _list_accounts(
        self,
        connection: CTraderConnection,
    ) -> oa.ProtoOAGetAccountListByAccessTokenRes:
        try:
            return await connection.request(
                oa.ProtoOAGetAccountListByAccessTokenReq(
                    accessToken=self._credentials.access_token,
                ),
            )
        except CTraderRequestError as e:
            if e.error_code not in TOKEN_ERROR_CODES:
                raise CTraderAuthError(f"account list rejected: {e.error_code}") from e
            if self._credentials.refresh_token is None:
                raise CTraderAuthError(
                    f"access token rejected ({e.error_code}) and no refresh token is configured",
                ) from e
            self._log.warning(f"Access token rejected ({e.error_code}), refreshing token")

        await self._refresh_over(connection)
        try:
            return await connection.request(
                oa.ProtoOAGetAccountListByAccessTokenReq(
                    accessToken=self._credentials.access_token,
                ),
            )
        except CTraderRequestError as e:
            raise CTraderAuthError(f"account list rejected after refresh: {e.error_code}") from e

    async def _refresh_over(self, connection: CTraderConnection) -> None:
        # TODO(verify): ProtoOARefreshTokenReq over an app-only-authenticated pre-connection on
        # the demo host works for a live account's token.
        try:
            response = await connection.request(
                oa.ProtoOARefreshTokenReq(refreshToken=self._credentials.refresh_token),
            )
        except CTraderRequestError as e:
            self._log.error(f"Token refresh rejected: {e.error_code}")
            raise CTraderAuthError(f"token refresh rejected: {e.error_code}") from e
        self._log.info("Access token refreshed")
        self._on_tokens_refreshed(
            response.accessToken,
            response.refreshToken,
            time.time() + response.expiresIn,
        )

    def _on_tokens_refreshed(self, access: str, refresh: str, expires_at_secs: float) -> None:
        # Kept current so a session built later starts from the newest pair.
        self._credentials = dataclasses.replace(
            self._credentials,
            access_token=access,
            refresh_token=refresh,
            token_expires_at=expires_at_secs,
        )
        for listener in tuple(self._token_listeners):
            try:
                listener(access, refresh, expires_at_secs)
            except Exception as e:
                # Only the type: the application's message could contain the tokens.
                self._log.error(
                    f"Token listener raised {type(e).__name__}; new tokens not saved",
                )

    def _build_session(self, host: str) -> CTraderSession:
        credentials = self._credentials
        if credentials.token_expires_at is None:
            self._log.warning("token_expires_at not set: proactive token refresh is disabled")
        return CTraderSession(
            host=host,
            port=self._port,
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
            account_id=self.account_id,
            access_token=credentials.access_token,
            refresh_token=credentials.refresh_token,
            expires_at_secs=credentials.token_expires_at,
            on_tokens_refreshed=self._on_tokens_refreshed,
            logger=self._log,
            tls=self._tls,
        )

    async def _wait_ready(self, session: CTraderSession) -> None:
        # Polled rather than awaited in one go, so a rejected authentication - which the
        # session would otherwise retry forever - fails the connect at once. The caller bounds
        # the total wait.
        while True:
            with contextlib.suppress(TimeoutError):
                await session.wait_ready(timeout_secs=_READY_POLL_SECS)
                return
            error = session.last_error
            if isinstance(error, CTraderAuthError):
                if self._environment != "auto" and _is_cant_route(error):
                    raise CTraderAuthError(
                        "configured environment does not match the account; use 'auto'",
                    ) from error
                raise CTraderAuthError(str(error)) from error

    async def _load_reference_data(self) -> None:
        trader = (
            await self.request(oa.ProtoOATraderReq(ctidTraderAccountId=self.account_id))
        ).trader
        if not trader.HasField("moneyDigits"):
            # A silent default of 0 would misscale every monetary value.
            raise CTraderProtocolError("trader response carries no moneyDigits")
        asset_list = await self.request(oa.ProtoOAAssetListReq(ctidTraderAccountId=self.account_id))
        assets = {asset.assetId: asset for asset in asset_list.asset}
        deposit_asset = assets.get(trader.depositAssetId)
        if deposit_asset is None:
            raise CTraderProtocolError("deposit asset missing from the asset list")
        symbol_list = await self.request(
            oa.ProtoOASymbolsListReq(ctidTraderAccountId=self.account_id),
        )

        self.assets = assets
        self.light_symbols = {s.symbolName: s for s in symbol_list.symbol}
        self._money_digits = trader.moneyDigits
        self._deposit_asset = deposit_asset

    async def _retry_restores_loop(self, session: CTraderSession) -> None:
        failures: dict[Hashable, int] = {}
        while True:
            await asyncio.sleep(self._restore_retry_interval_secs)
            if not session.is_ready:
                continue
            try:
                await session.retry_failed_restores()
            except Exception as e:
                self._log.exception("Restore retry failed", e)
                continue
            # A key that recovers drops out and restarts from zero, so a key that keeps
            # flapping can log the ERROR again.
            failures = {key: failures.get(key, 0) + 1 for key in session.failed_restores}
            for key, attempts in failures.items():
                if attempts == _RESTORE_RETRY_ERROR_ATTEMPTS:
                    self._log.error(f"Restore {key!r} still failing after {attempts} retries")


def _is_cant_route(error: BaseException) -> bool:
    cause: BaseException | None = error
    while cause is not None:
        if isinstance(cause, CTraderRequestError) and cause.error_code == "CANT_ROUTE_REQUEST":
            return True
        cause = cause.__cause__
    return False


# The credentials are kept beside the client so a later call is compared with what the
# client was built from, not with a token it has since refreshed for itself.
_ACCOUNT_CLIENTS: dict[tuple[int, str], tuple[CTraderAccountClient, AccountCredentials]] = {}


def get_cached_ctrader_account_client(
    *,
    trader_login: int,
    credentials: AccountCredentials,
    environment: str,
    logger: Logger,
    **kwargs,
) -> CTraderAccountClient:
    """The one client per `(trader_login, credentials.client_id)`.

    The key assumes a login names one account across everything the token grants, which the
    duplicate check in `_resolve_account()` enforces at connect time.

    Later calls with the same key return the first instance; their other arguments are ignored.
    A differing `environment` or credential is reported in one WARNING: the first client's
    host and tokens win, and the second config would otherwise be ignored in silence. The
    comparison is against the credentials the client was built from, so a token the client
    refreshed on its own is not reported as the caller's disagreement.
    """
    key = (trader_login, credentials.client_id)
    entry = _ACCOUNT_CLIENTS.get(key)
    if entry is None:
        client = CTraderAccountClient(
            trader_login=trader_login,
            credentials=credentials,
            environment=environment,
            logger=logger,
            **kwargs,
        )
        _ACCOUNT_CLIENTS[key] = (client, credentials)
        return client

    client, cached = entry
    differences: list[str] = []
    if environment != client._environment:
        differences.append(
            f"environment (built {client._environment!r}, requested {environment!r})",
        )
    # Field names only: a credential value, or any part or measure of one, never reaches a log.
    changed = [
        label
        for label, old_value, new_value in (
            ("client secret", cached.client_secret, credentials.client_secret),
            ("access token", cached.access_token, credentials.access_token),
            ("refresh token", cached.refresh_token, credentials.refresh_token),
            ("token expiry", cached.token_expires_at, credentials.token_expires_at),
        )
        if old_value != new_value
    ]
    if changed:
        differences.append(f"credentials ({', '.join(changed)})")
    if differences:
        logger.warning(
            "Account client already built; these differing settings are ignored: "
            + "; ".join(differences),
        )
    return client


class AccountClientConfig(Protocol):
    """What `account_client_from_config` reads from a client config.

    Structural, so every client config satisfies it without importing this module's types.
    """

    trader_login: int
    environment: Environment
    connect_timeout_secs: float
    restore_retry_interval_secs: float

    def credentials(self) -> AccountCredentials: ...


def account_client_from_config(
    config: AccountClientConfig,
    logger: Logger,
) -> CTraderAccountClient:
    """The cached account client a client config asks for.

    An application that persists refreshed tokens calls this with the same config object the
    node config holds, before building the node, and registers its listener on the result.
    """
    return get_cached_ctrader_account_client(
        trader_login=config.trader_login,
        credentials=config.credentials(),
        environment=config.environment,
        logger=logger,
        connect_timeout_secs=config.connect_timeout_secs,
        restore_retry_interval_secs=config.restore_retry_interval_secs,
    )


def _clear_account_cache() -> None:
    """Test helper: forget every cached client."""
    _ACCOUNT_CLIENTS.clear()
