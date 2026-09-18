"""The per-account client: one session per trading account, shared by every Nautilus client.

It picks the host, owns the only `CTraderSession` for the account (and with it the only party
allowed to refresh tokens), and caches the account's reference data. Nothing about the account
it resolves - its id, login or broker - is ever logged or put into an error message.
"""

from __future__ import annotations

import asyncio
import dataclasses
import ssl
import time
from collections.abc import Callable, Hashable, Sequence
from dataclasses import dataclass
from typing import Literal

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

_ENVIRONMENTS = ("auto", "demo", "live")
_READY_POLL_SECS = 0.5
# A restore still failing after this many background retries is reported once at ERROR.
_RESTORE_RETRY_ERROR_ATTEMPTS = 3


@dataclass(frozen=True)
class AccountCredentials:
    client_id: str
    client_secret: str
    access_token: str
    refresh_token: str | None
    # Unix seconds; `None` disables proactive refresh.
    token_expires_at: float | None


class CTraderAccountClient:
    """
    One trading account's connection, shared by reference count.

    - `environment="auto"` reads the account's `isLive` flag over a short pre-connection to
      `demo_host` and connects to the matching host; `"demo"`/`"live"` connect directly.
    - `connect()` returns once the session is ready and the reference data is loaded; bad
      credentials fail it at once rather than after `connect_timeout_secs`.
    """

    def __init__(
        self,
        *,
        account_id: int,
        credentials: AccountCredentials,
        environment: Literal["auto", "demo", "live"],
        logger: Logger,
        connect_timeout_secs: float = 60.0,
        restore_retry_interval_secs: float = 30.0,
        demo_host: str = DEMO_HOST,
        live_host: str = LIVE_HOST,
        port: int = PROTOBUF_PORT,
        tls: ssl.SSLContext | bool = True,
    ) -> None:
        if environment not in _ENVIRONMENTS:
            raise ValueError(f"environment must be one of {_ENVIRONMENTS}, got {environment!r}")
        self.account_id = account_id
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
        self._connect_task: asyncio.Task | None = None
        self._connect_waiters = 0
        self._retry_task: asyncio.Task | None = None

        self._deposit_asset: om.ProtoOAAsset | None = None
        self._money_digits: int | None = None
        self.assets: dict[int, om.ProtoOAAsset] = {}
        self.light_symbols: dict[str, om.ProtoOALightSymbol] = {}
        self._symbol_specs: dict[int, om.ProtoOASymbol] = {}

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

        Concurrent callers share one attempt. A failed attempt leaves the user count unchanged
        and the client reusable.
        """
        if self._users > 0:
            self._users += 1
            return
        if self._connect_task is None:
            task = asyncio.create_task(self._bring_up())
            task.add_done_callback(self._on_connect_done)
            self._connect_task = task
        task = self._connect_task
        self._connect_waiters += 1
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            # Only the last waiter to leave abandons the attempt; the others still want it.
            if self._connect_waiters == 1 and not task.done():
                task.cancel()
            raise
        finally:
            self._connect_waiters -= 1
        self._users += 1

    def _on_connect_done(self, task: asyncio.Task) -> None:
        if self._connect_task is task:
            self._connect_task = None
        if not task.cancelled():
            task.exception()

    async def disconnect(self) -> None:
        """Release one user; the last one stops the session."""
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

    async def symbol_specs(self, symbol_ids: Sequence[int]) -> dict[int, om.ProtoOASymbol]:
        """Full symbol entities by id, fetched once each and cached.

        Ids the venue does not return are absent from the result.
        """
        missing = [i for i in dict.fromkeys(symbol_ids) if i not in self._symbol_specs]
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
        host = await self._resolve_host()
        session = self._build_session(host)
        self.session = session
        try:
            await session.start()
            await self._wait_ready(session)
            if self._deposit_asset is None:
                await self._load_reference_data()
        except BaseException:
            self.session = None
            await session.stop()
            raise
        self._retry_task = asyncio.create_task(self._retry_restores_loop(session))
        self._log.info("Account session ready")

    async def _stop(self, session: CTraderSession | None) -> None:
        retry_task, self._retry_task = self._retry_task, None
        if retry_task is not None:
            retry_task.cancel()
            await asyncio.wait({retry_task})
        if session is not None:
            await session.stop()

    async def _resolve_host(self) -> str:
        if self._environment == "demo":
            return self._demo_host
        if self._environment == "live":
            return self._live_host

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
        for entry in accounts.ctidTraderAccount:
            if entry.ctidTraderAccountId == self.account_id:
                return self._live_host if entry.isLive else self._demo_host
        raise CTraderAuthError("account not granted to this token")

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
        # session would otherwise retry forever - fails the connect at once.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._connect_timeout_secs
        while True:
            remaining = deadline - loop.time()
            try:
                await session.wait_ready(timeout_secs=max(0.0, min(_READY_POLL_SECS, remaining)))
                return
            except TimeoutError:
                pass
            error = session.last_error
            if isinstance(error, CTraderAuthError):
                if self._environment != "auto" and _is_cant_route(error):
                    raise CTraderAuthError(
                        "configured environment does not match the account; use 'auto'",
                    ) from error
                raise error
            if loop.time() >= deadline:
                raise CTraderTimeoutError(
                    f"session not ready within {self._connect_timeout_secs:g}s",
                ) from error

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


_ACCOUNT_CLIENTS: dict[tuple[int, str], CTraderAccountClient] = {}


def get_cached_ctrader_account_client(
    *,
    account_id: int,
    credentials: AccountCredentials,
    environment: str,
    logger: Logger,
    **kwargs,
) -> CTraderAccountClient:
    """The one client per `(account_id, credentials.client_id)`.

    Later calls with the same key return the first instance; their other arguments are ignored.
    """
    key = (account_id, credentials.client_id)
    client = _ACCOUNT_CLIENTS.get(key)
    if client is None:
        client = CTraderAccountClient(
            account_id=account_id,
            credentials=credentials,
            environment=environment,
            logger=logger,
            **kwargs,
        )
        _ACCOUNT_CLIENTS[key] = client
    return client


def _clear_account_cache() -> None:
    """Test helper: forget every cached client."""
    _ACCOUNT_CLIENTS.clear()
