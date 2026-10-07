"""The per-account client: one session per trading account, shared by every Nautilus client.

It picks the host, owns the only `CTraderSession` for the account (and with it the only party
allowed to refresh tokens) and the only instrument provider, and caches the account's reference
data. Its log lines and error messages say what went wrong, not which account it resolved.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import ssl
import time
from collections.abc import Callable, Hashable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal, Protocol, get_args

from google.protobuf.message import Message
from nautilus_trader.common.component import Logger
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.instruments import Instrument

from nautilus_ctrader.common.connection import CTraderConnection
from nautilus_ctrader.common.errors import (
    CTraderAuthError,
    CTraderConnectionError,
    CTraderError,
    CTraderProtocolError,
    CTraderRequestError,
    CTraderTimeoutError,
)
from nautilus_ctrader.common.session import CTraderSession
from nautilus_ctrader.common.subscriptions import SubscriptionRegistry
from nautilus_ctrader.constants import (
    DEFAULT_REQUEST_TIMEOUT_SECS,
    DEMO_HOST,
    LATE_REFRESH_WAIT_SECS,
    LIVE_HOST,
    PROTOBUF_PORT,
    SYMBOL_BY_ID_BATCH,
    TOKEN_ERROR_CODES,
)
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from nautilus_ctrader.providers import CTraderInstrumentProvider, InstrumentLoadError

TokenListener = Callable[[str, str, float], None]
ReloadListener = Callable[[Instrument], None]

Environment = Literal["auto", "demo", "live"]
ENVIRONMENTS: tuple[Environment, ...] = get_args(Environment)
_READY_POLL_SECS = 0.5
# A restore still failing after this many background retries is reported once at ERROR.
_RESTORE_RETRY_ERROR_ATTEMPTS = 3


@dataclass(frozen=True)
class AccountRecord:
    """One entry of the account list an access token grants; absent fields are `None`."""

    ctid_trader_account_id: int
    is_live: bool | None
    trader_login: int | None
    broker_title_short: str | None


@dataclass(frozen=True)
class AccountsResult:
    permission_scope: int
    accounts: list[AccountRecord]


class NullLogger:
    """Discards everything; for callers of the helpers here that have no logger to give."""

    def debug(self, message: str, color=None) -> None:
        pass

    def info(self, message: str, color=None) -> None:
        pass

    def warning(self, message: str, color=None) -> None:
        pass

    def error(self, message: str, color=None) -> None:
        pass

    def exception(self, message: str, ex: BaseException) -> None:
        pass


def account_host(
    is_live: bool | None,
    *,
    demo_host: str = DEMO_HOST,
    live_host: str = LIVE_HOST,
) -> str:
    """The host an account authenticates on: `live_host` for a live account, else `demo_host`.

    An unknown flag goes to `demo_host`.
    """
    return live_host if is_live else demo_host


async def request_granted_accounts(
    connection: CTraderConnection,
    access_token: str,
) -> AccountsResult:
    """The accounts `access_token` grants, over an application-authenticated `connection`.

    Raises the venue's `CTraderRequestError` as it is. The response echoes the access token;
    only the records and the permission scope are kept.
    """
    response = await connection.request(
        oa.ProtoOAGetAccountListByAccessTokenReq(accessToken=access_token),
    )
    accounts = [
        AccountRecord(
            ctid_trader_account_id=entry.ctidTraderAccountId,
            is_live=entry.isLive if entry.HasField("isLive") else None,
            trader_login=entry.traderLogin if entry.HasField("traderLogin") else None,
            broker_title_short=(
                entry.brokerTitleShort if entry.HasField("brokerTitleShort") else None
            ),
        )
        for entry in response.ctidTraderAccount
    ]
    return AccountsResult(permission_scope=response.permissionScope, accounts=accounts)


async def list_granted_accounts(
    client_id: str,
    client_secret: str,
    access_token: str,
    *,
    host: str,
    port: int = PROTOBUF_PORT,
    tls: ssl.SSLContext | bool = True,
    logger: Logger | None = None,
) -> AccountsResult:
    """Authenticate the application on `host`, then list the accounts `access_token` grants.

    The list is served on either host. A venue rejection is raised as its `CTraderRequestError`.
    """
    connection = CTraderConnection(
        host,
        port,
        logger=NullLogger() if logger is None else logger,
        tls=tls,
    )
    await connection.connect()
    try:
        await connection.request(
            oa.ProtoOAApplicationAuthReq(clientId=client_id, clientSecret=client_secret),
        )
        return await request_granted_accounts(connection, access_token)
    finally:
        await connection.close()


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
    - A loaded symbol the venue reports as changed is reloaded here, once for all clients;
      they learn of it through `add_reload_listener()`. One not loaded is left unloaded.
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
        self._reload_listeners: list[ReloadListener] = []
        self._reload_tasks: set[asyncio.Task] = set()
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

        self._instrument_provider: CTraderInstrumentProvider | None = None
        # What the provider was built from, keyed by the config field that carries each setting.
        self._instrument_settings: dict[str, object] = {}

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

    @property
    def instrument_provider(self) -> CTraderInstrumentProvider | None:
        """The account's provider, or `None` until `get_instrument_provider()` builds it."""
        return self._instrument_provider

    def get_instrument_provider(
        self,
        *,
        config: InstrumentProviderConfig,
        asset_class_overrides: Mapping[str, AssetClass],
        fail_on_instrument_error: bool,
        logger: Logger,
    ) -> CTraderInstrumentProvider:
        """The account's one instrument provider, built by the first call.

        Every client of the account shares it, so an instrument unloaded for one is unloaded
        for all. Its conversion chains are dropped on every bring-up of the account's session.

        Raises `ValueError` naming the settings that differ if a later call asks for other
        instrument settings than the provider was built from: one of the two would otherwise
        be ignored. A later call's `logger` is ignored.
        """
        settings = {
            "instrument_provider": config,
            "asset_class_overrides": dict(asset_class_overrides),
            "fail_on_instrument_error": fail_on_instrument_error,
        }
        if self._instrument_provider is None:
            self._instrument_provider = CTraderInstrumentProvider(
                self,
                config,
                asset_class_overrides,
                fail_on_instrument_error,
                logger,
            )
            self._instrument_settings = settings
            return self._instrument_provider

        # Names only: the caller holds both values, and a filter value could hold anything.
        differing = [
            name for name, value in settings.items() if value != self._instrument_settings[name]
        ]
        if differing:
            raise ValueError(
                "the instrument provider of this account is already built from other "
                f"instrument settings; differing: {', '.join(differing)}. Every client of one "
                "account must be given the same instrument settings",
            )
        return self._instrument_provider

    def add_token_listener(self, callback: TokenListener) -> None:
        """Register `callback(access_token, refresh_token, expires_at_secs)` for every refresh.

        The new pair exists nowhere else: the application must persist it, because the old
        refresh token may no longer work.
        """
        self._token_listeners.append(callback)

    def add_reload_listener(self, callback: ReloadListener) -> None:
        """Register `callback(instrument)` for every instrument reloaded after a symbol change.

        The account reloads each changed symbol once, however many clients it serves, and calls
        every listener with the rebuilt instrument, which the provider already holds. A reload
        that fails calls none. Adding a registered listener again changes nothing.
        """
        if callback not in self._reload_listeners:
            self._reload_listeners.append(callback)

    def remove_reload_listener(self, callback: ReloadListener) -> None:
        if callback in self._reload_listeners:
            self._reload_listeners.remove(callback)

    async def connect(self) -> None:
        """Become a user of the account's session, bringing it up if this is the first user.

        Returns once the session is ready, for every caller: a later user that finds it
        reconnecting waits, under the same `connect_timeout_secs` bound and failing at once on
        a rejected authentication.

        Calls are serialised: a caller queued behind a successful attempt only counts itself,
        and one queued behind a failed attempt tries again. A failed or cancelled attempt
        stops what it started and leaves the user count unchanged.
        """
        async with self._lifecycle_lock:
            if self._users > 0:
                # Waited on under the lock, so the session cannot be stopped or replaced
                # meanwhile. Its own reconnect never takes this lock.
                if not self.session.is_ready:
                    await self._join(self.session)
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
                # Before `start()`, so the first bring-up already restores live subscriptions,
                # and a symbol change arriving before the session is ready is not lost. No
                # instrument is loaded before the reference data, so no reload starts that early.
                self.subscriptions.attach(session)
                session.add_event_handler(oa.ProtoOASymbolChangedEvent, self._on_symbol_changed)
                self.session = session
                await session.start()
                await self._wait_ready(session)
                if self._deposit_asset is None:
                    await self._load_reference_data()
        except BaseException as e:
            self.session = None
            if session is not None:
                # Also cancels a reload an early symbol change started.
                await self._stop(session)
            if isinstance(e, TimeoutError) and deadline.expired():
                cause = session.last_error if session is not None else None
                raise self._connect_timeout_error() from (cause or e)
            raise
        if self._instrument_provider is not None:
            # A chain is venue data that can change, so it is re-queried once per bring-up;
            # a reconnect of the running session keeps it.
            self._instrument_provider.reset_conversion_cache()
        self._retry_task = asyncio.create_task(self._retry_restores_loop(session))
        self._log.info("Account session ready")

    async def _join(self, session: CTraderSession) -> None:
        """Wait for a running session that is not ready, as a later user's `connect()` does."""
        deadline = asyncio.timeout(self._connect_timeout_secs)
        try:
            async with deadline:
                await self._wait_ready(session)
        except TimeoutError as e:
            if not deadline.expired():
                raise
            raise self._connect_timeout_error() from (session.last_error or e)

    def _connect_timeout_error(self) -> CTraderTimeoutError:
        return CTraderTimeoutError(
            f"connect did not complete within {self._connect_timeout_secs:g}s",
        )

    async def _stop(self, session: CTraderSession | None) -> None:
        self.subscriptions.detach()
        if session is not None:
            session.remove_event_handler(oa.ProtoOASymbolChangedEvent, self._on_symbol_changed)
        tasks, self._reload_tasks = self._reload_tasks, set()
        retry_task, self._retry_task = self._retry_task, None
        if retry_task is not None:
            tasks.add(retry_task)
        for task in tasks:
            task.cancel()
        # The session is stopped even if this is cancelled while the tasks wind down;
        # otherwise it would run on with nothing left to reach it.
        try:
            if tasks:
                await asyncio.wait(tasks)
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

        # The list also carries logins and broker names; this module does not log them.
        # TODO(verify): the venue populates `traderLogin` on every listed account. An entry
        # without it can never be matched, and this reports it as a login not granted.
        matched = [entry for entry in accounts.accounts if entry.trader_login == self.trader_login]
        if not matched:
            raise CTraderAuthError("no account with that trader login is granted to this token")
        if len(matched) > 1:
            # A login is unique per broker server, but one token can grant accounts on several
            # servers, so picking one here would be a guess at which account to trade.
            raise CTraderAuthError(
                "more than one granted account has that trader login; the account is ambiguous",
            )
        entry = matched[0]
        self._account_id = entry.ctid_trader_account_id
        if self._environment == "demo":
            return self._demo_host
        if self._environment == "live":
            return self._live_host
        return account_host(entry.is_live, demo_host=self._demo_host, live_host=self._live_host)

    async def _list_accounts(self, connection: CTraderConnection) -> AccountsResult:
        try:
            return await request_granted_accounts(connection, self._credentials.access_token)
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
            return await request_granted_accounts(connection, self._credentials.access_token)
        except CTraderRequestError as e:
            raise CTraderAuthError(f"account list rejected after refresh: {e.error_code}") from e

    async def _refresh_over(self, connection: CTraderConnection) -> None:
        # TODO(verify): ProtoOARefreshTokenReq over an app-only-authenticated pre-connection on
        # the demo host works for a live account's token.
        late: asyncio.Future[oa.ProtoOARefreshTokenRes] = asyncio.get_running_loop().create_future()

        def catch_late_reply(payload: Message) -> None:
            if isinstance(payload, oa.ProtoOARefreshTokenRes) and not late.done():
                late.set_result(payload)

        # Set before the request, so a reply that misses its timeout is still caught.
        connection.set_event_handler(catch_late_reply)
        try:
            response = await connection.request(
                oa.ProtoOARefreshTokenReq(refreshToken=self._credentials.refresh_token),
                timeout_secs=DEFAULT_REQUEST_TIMEOUT_SECS,
            )
        except CTraderRequestError as e:
            self._log.error(f"Token refresh rejected: {e.error_code}")
            raise CTraderAuthError(f"token refresh rejected: {e.error_code}") from e
        except CTraderTimeoutError as timeout:
            # A refresh token is single-use, so once the request has reached the venue a late
            # reply holds the only working pair; closing this connection now would lose it.
            try:
                response = await asyncio.wait_for(late, LATE_REFRESH_WAIT_SECS)
            except TimeoutError:
                raise timeout from None
            self._log.warning("Adopted a late token refresh reply")
        else:
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

    def _on_symbol_changed(self, event: oa.ProtoOASymbolChangedEvent) -> None:
        provider = self._instrument_provider
        if provider is None:
            # No client has asked for instruments, so nothing is loaded or cached.
            return
        for symbol_id in event.symbolId:
            changed = provider.instrument_for_symbol_id(symbol_id)
            if changed is None:
                # Never requested, or dropped by `remove_failed`: which instruments exist is the
                # application's decision. Only the cached spec goes, so a later load fetches the
                # changed one.
                self._symbol_specs.pop(symbol_id, None)
                self._log.debug(f"Symbol changed at the venue: symbol id {symbol_id}; not loaded")
                continue
            name = changed.id.symbol.value
            self._log.warning(f"Symbol changed at the venue: {name}; reloading")
            # Before the reload's first await, so no conversion prepared meanwhile trusts a
            # chain through the changed symbol.
            provider.reset_conversion_cache(symbol_id)
            task = asyncio.create_task(self._reload_symbol(provider, symbol_id, name))
            self._reload_tasks.add(task)
            task.add_done_callback(self._on_reload_done)

    async def _reload_symbol(
        self,
        provider: CTraderInstrumentProvider,
        symbol_id: int,
        name: str,
    ) -> None:
        try:
            reloaded = await provider.reload(symbol_id)
        except (InstrumentLoadError, CTraderError) as e:
            self._log.error(f"Reload of {name} failed: {e}")
            return
        for listener in tuple(self._reload_listeners):
            try:
                listener(reloaded)
            except Exception as e:
                self._log.exception(f"Reload listener raised for {reloaded.id}", e)

    def _on_reload_done(self, task: asyncio.Task) -> None:
        self._reload_tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            self._log.exception("Symbol reload raised", task.exception())

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


def request_error_code(error: BaseException) -> str | None:
    """The venue's error code behind `error`: that of the first `CTraderRequestError` in its
    `__cause__` chain, or `None` if there is none."""
    cause: BaseException | None = error
    while cause is not None:
        if isinstance(cause, CTraderRequestError):
            return cause.error_code
        cause = cause.__cause__
    return None


def _is_cant_route(error: BaseException) -> bool:
    return request_error_code(error) == "CANT_ROUTE_REQUEST"


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
