"""The cTrader session: authentication, reconnect, and subscription restore.

A state machine over a connection it owns. Upper layers register a restore action once and
never handle reconnection themselves, which is what keeps recovery behaviour identical on
every path rather than subtly different per client.
"""

from __future__ import annotations

import asyncio
import random
import ssl
import time
from collections.abc import Awaitable, Callable, Hashable
from enum import Enum, auto

from google.protobuf.message import Message
from nautilus_trader.common.component import Logger

from nautilus_ctrader.common.connection import CTraderConnection, retrieve_exceptions
from nautilus_ctrader.common.errors import (
    CTraderAuthError,
    CTraderConnectionError,
    CTraderRequestError,
)
from nautilus_ctrader.common.rate_limit import RateLimiter
from nautilus_ctrader.constants import (
    BACKOFF_BASE_SECS,
    BACKOFF_JITTER,
    BACKOFF_MAX_SECS,
    BUCKET_DEFAULT,
    BUCKET_HISTORICAL,
    CONNECT_TIMEOUT_SECS,
    DEFAULT_RATE_LIMIT_PER_SEC,
    DEFAULT_REQUEST_TIMEOUT_SECS,
    HEARTBEAT_IDLE_SECS,
    HISTORICAL_RATE_LIMIT_PER_SEC,
    INBOUND_SILENCE_SECS,
    MIN_TOKEN_REFRESH_INTERVAL_SECS,
    RECONNECT_FAILURE_THRESHOLD,
    STABLE_SESSION_SECS,
    TOKEN_ERROR_CODES,
    TOKEN_REFRESH_MARGIN_SECS,
)
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa


class SessionState(Enum):
    STOPPED = auto()
    CONNECTING = auto()
    AUTHENTICATING = auto()
    RESTORING = auto()
    READY = auto()


class CTraderSession:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        client_id: str,
        client_secret: str,
        account_id: int,
        access_token: str,
        logger: Logger,
        refresh_token: str | None = None,
        expires_at_secs: float | None = None,
        on_tokens_refreshed: Callable[[str, str, float], None] | None = None,
        # `True` (verified TLS) is the only safe choice against a real venue; `False` is for
        # tests against the local fake server.
        tls: ssl.SSLContext | bool = True,
        rate_limiter: RateLimiter | None = None,
        heartbeat_idle_secs: float = HEARTBEAT_IDLE_SECS,
        inbound_silence_secs: float = INBOUND_SILENCE_SECS,
        request_timeout_secs: float = DEFAULT_REQUEST_TIMEOUT_SECS,
        connect_timeout_secs: float = CONNECT_TIMEOUT_SECS,
        backoff_base_secs: float = BACKOFF_BASE_SECS,
        backoff_max_secs: float = BACKOFF_MAX_SECS,
        failure_threshold: int = RECONNECT_FAILURE_THRESHOLD,
    ) -> None:
        self._client_id = client_id
        self._client_secret = client_secret
        self._account_id = account_id
        self._access_token = access_token
        self._refresh_token = refresh_token
        self._expires_at_secs = expires_at_secs
        self._on_tokens_refreshed = on_tokens_refreshed
        self._log = logger

        self._backoff_base_secs = backoff_base_secs
        self._backoff_max_secs = backoff_max_secs
        self._failure_threshold = failure_threshold

        # Rate limiting is part of the transport, not an opt-in: a session built without a
        # limiter would send unthrottled. Callers override the rates, not their existence.
        if rate_limiter is None:
            rate_limiter = RateLimiter(
                {
                    BUCKET_DEFAULT: DEFAULT_RATE_LIMIT_PER_SEC,
                    BUCKET_HISTORICAL: HISTORICAL_RATE_LIMIT_PER_SEC,
                },
            )

        self._connection = CTraderConnection(
            host=host,
            port=port,
            logger=logger,
            rate_limiter=rate_limiter,
            heartbeat_idle_secs=heartbeat_idle_secs,
            inbound_silence_secs=inbound_silence_secs,
            request_timeout_secs=request_timeout_secs,
            connect_timeout_secs=connect_timeout_secs,
            tls=tls,
        )
        self._connection.set_event_handler(self._on_event)
        self._connection.set_disconnect_handler(self._on_disconnect)

        self._state = SessionState.STOPPED
        self._restores: dict[Hashable, Callable[[], Awaitable[None]]] = {}
        self._failed_restores: set[Hashable] = set()
        self._event_handler: Callable[[Message], None] | None = None
        self._ready = asyncio.Event()
        self._lost = asyncio.Event()
        self._supervisor: asyncio.Task | None = None
        self._refresh_task: asyncio.Task | None = None
        self._last_refresh_at: float | None = None
        self._reauth_requested = False
        self._stopping = False
        # Set while no `stop()` is running; `start()` waits on it.
        self._stop_idle = asyncio.Event()
        self._stop_idle.set()
        self._stops_in_progress = 0
        # Bumped by every `stop()`, so a `start()` that waited on `_stop_idle` can tell whether
        # a later `stop()` started after it and must win.
        self._stop_epoch = 0
        self.last_error: Exception | None = None
        # The cause of the current loss only, so a bring-up failure never chains to a stale one.
        self._loss_cause: Exception | None = None

    @property
    def state(self) -> SessionState:
        return self._state

    @property
    def is_ready(self) -> bool:
        return self._state is SessionState.READY

    @property
    def failed_restores(self) -> frozenset[Hashable]:
        """Keys whose restore failed in the most recent bring-up."""
        return frozenset(self._failed_restores)

    def set_event_handler(self, handler: Callable[[Message], None]) -> None:
        self._event_handler = handler

    def add_restore(self, key: Hashable, factory: Callable[[], Awaitable[None]]) -> None:
        """Register an action to replay after every successful authentication."""
        self._restores[key] = factory

    def remove_restore(self, key: Hashable) -> None:
        self._restores.pop(key, None)
        self._failed_restores.discard(key)

    async def wait_ready(self, timeout_secs: float | None = None) -> None:
        await asyncio.wait_for(self._ready.wait(), timeout_secs)

    async def start(self) -> None:
        """Start the supervisor. Returns before the first authentication completes.

        Waits for any `stop()` still in progress, so a restart never overlaps its teardown.
        """
        epoch = self._stop_epoch
        await self._stop_idle.wait()
        # A stop() issued while this start() was waiting takes precedence over it.
        if self._stop_epoch != epoch:
            return
        if self._supervisor is not None and not self._supervisor.done():
            return
        self._stopping = False
        self._reauth_requested = False
        self._supervisor = asyncio.create_task(self._supervise())

    async def stop(self) -> None:
        self._stops_in_progress += 1
        self._stop_epoch += 1
        self._stop_idle.clear()
        try:
            # State first, awaits last: repeated cancellation can only cut a wait short, never
            # leave a live, unsupervised connection behind a session that still looks ready.
            # Only what is captured here is awaited, so a restart begun meanwhile is never
            # touched.
            self._stopping = True
            self._lost.set()
            self._state = SessionState.STOPPED
            self._ready.clear()
            tasks = {t for t in (self._supervisor, self._refresh_task) if t is not None}
            self._supervisor = None
            self._refresh_task = None
            for task in tasks:
                task.cancel()
            handles = self._connection._begin_close()

            try:
                if tasks:
                    # Not a per-task suppress: that would also catch a cancellation of the
                    # caller of `stop()` itself and let it slip past unnoticed.
                    await asyncio.wait(tasks)
            finally:
                retrieve_exceptions(tasks)
                await self._connection._finish_close(handles)
        finally:
            self._stops_in_progress -= 1
            if self._stops_in_progress == 0:
                self._stop_idle.set()

    async def request(
        self,
        payload: Message,
        *,
        timeout_secs: float | None = None,
        bucket: str = BUCKET_DEFAULT,
    ) -> Message:
        """Issue a request, failing fast when the session is not ready.

        Also accepted while `RESTORING`, since restore actions issue their requests through
        here. Subscriptions survive a reconnect through the restore registry, so anything else
        is better refused than silently delayed.
        """
        if self._state not in (SessionState.READY, SessionState.RESTORING):
            raise CTraderConnectionError(f"session not ready (state={self._state.name})")
        return await self._connection.request(
            payload,
            timeout_secs=timeout_secs,
            bucket=bucket,
        )

    async def _supervise(self) -> None:
        loop = asyncio.get_running_loop()
        attempt = 0
        while not self._stopping:
            try:
                await self._bring_up()
                ready_at = loop.time()
                await self._lost.wait()
                if self._stopping:
                    return
                # The old socket may still be live (the venue dropped our authentication) or
                # half-dead (a failed read leaves the writer and heartbeat task running). Either
                # way it is closed before a new one opens.
                await self._teardown_connection()
                reauth_requested, self._reauth_requested = self._reauth_requested, False
                if reauth_requested or loop.time() - ready_at >= STABLE_SESSION_SECS:
                    attempt = 0
                    if reauth_requested:
                        self._log.info("Re-authenticating with the refreshed token")
                    else:
                        self._log.warning("Connection lost, reconnecting")
                else:
                    # A peer that drops every new session is failing, not recovering.
                    attempt += 1
                    await self._back_off(
                        attempt,
                        f"Connection lost soon after becoming ready (attempt {attempt})",
                    )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.last_error = e
                attempt += 1
                await self._teardown_connection()
                await self._back_off(attempt, f"Session bring-up failed (attempt {attempt}): {e}")

    async def _back_off(self, attempt: int, message: str) -> None:
        """Log `message` and sleep an exponential, jittered delay; ERROR from the threshold on."""
        delay = min(self._backoff_max_secs, self._backoff_base_secs * 2 ** (attempt - 1))
        delay *= 1.0 + random.random() * BACKOFF_JITTER
        if attempt >= self._failure_threshold:
            self._log.error(f"{message}; retrying in {delay:.1f}s")
        else:
            self._log.warning(f"{message}; retrying in {delay:.1f}s")
        await asyncio.sleep(delay)

    async def _bring_up(self) -> None:
        # Cleared before connecting, not after: a loss landing while bring-up is finishing
        # must survive to wake the supervisor, or it would wait forever on a dead socket.
        self._lost.clear()
        self._loss_cause = None
        self._ready.clear()
        self._failed_restores.clear()
        self._state = SessionState.CONNECTING
        await self._connection.connect()

        self._state = SessionState.AUTHENTICATING
        await self._authenticate()
        self._raise_if_lost()

        self._state = SessionState.RESTORING
        for key, factory in list(self._restores.items()):
            self._log.info(f"Restoring {key!r}")
            try:
                await factory()
            except CTraderConnectionError:
                # The connection itself is gone: a bring-up failure, not a bad restore.
                raise
            except Exception as e:
                # A loss can surface as another error type; it is already logged.
                self._raise_if_lost()
                # One rejected restore must not keep the whole session down. It is logged, and
                # the key stays registered so the next reconnect retries it.
                detail = e.error_code if isinstance(e, CTraderRequestError) else repr(e)
                self._log.error(f"Restore {key!r} failed: {detail}")
                self._failed_restores.add(key)

        self._raise_if_lost()

        if self._refresh_task is None or self._refresh_task.done():
            self._refresh_task = asyncio.create_task(self._refresh_loop())

        self._state = SessionState.READY
        self.last_error = None
        self._ready.set()
        self._log.info("Session ready")

    def _raise_if_lost(self) -> None:
        """Fail bring-up if the connection or authentication was lost mid-flight.

        Called right after authenticating and again after restoring, so a loss during either
        phase is caught before the following state would let `request()` through on a socket
        whose authentication just dropped. A loss can surface as any error type - a protocol
        error rejects pending requests with itself - so this decides by what happened to the
        connection, not by the exception.
        """
        if self._lost.is_set():
            raise CTraderConnectionError(
                "connection or authentication lost during bring-up",
            ) from self._loss_cause

    async def _authenticate(self) -> None:
        try:
            await self._connection.request(
                oa.ProtoOAApplicationAuthReq(
                    clientId=self._client_id,
                    clientSecret=self._client_secret,
                ),
            )
        except CTraderRequestError as e:
            raise CTraderAuthError(f"application auth rejected: {e.error_code}") from e

        try:
            await self._authenticate_account()
        except CTraderRequestError as e:
            if not self._may_refresh_for(e):
                raise CTraderAuthError(f"account auth rejected: {e.error_code}") from e
            # The access token may simply have expired. Refresh, then retry once; a second
            # rejection is a real authentication failure.
            # TODO(verify): that the venue accepts a refresh on a connection authenticated only
            # at application level.
            self._log.warning(f"Account auth rejected ({e.error_code}), refreshing token")
            await self.refresh_tokens()
            try:
                await self._authenticate_account()
            except CTraderRequestError as retry_error:
                raise CTraderAuthError(
                    f"account auth rejected after refresh: {retry_error.error_code}",
                ) from retry_error

    async def _teardown_connection(self) -> None:
        self._state = SessionState.CONNECTING
        self._ready.clear()
        await self._connection.close()

    def _on_disconnect(self, error: Exception) -> None:
        self.last_error = error
        self._loss_cause = error
        if self._stopping:
            # `stop()` has already settled the state; a loss reported while it waits must not
            # revive it.
            return
        self._state = SessionState.CONNECTING
        self._ready.clear()
        self._lost.set()

    async def refresh_tokens(self) -> None:
        """Exchange the refresh token for a new pair, over the existing connection.

        A rejected refresh stops trading, so it is logged at ERROR and raised rather than
        retried quietly - the operator has to learn this from the failure, not from the absence
        of activity.

        It does not re-authenticate by itself: the new access token takes effect at the next
        account authentication, which the proactive loop forces.
        """
        if self._refresh_token is None:
            raise CTraderAuthError("no refresh token available")

        self._last_refresh_at = time.time()
        try:
            response = await self._connection.request(
                oa.ProtoOARefreshTokenReq(refreshToken=self._refresh_token),
            )
        except CTraderRequestError as e:
            self._log.error(f"Token refresh rejected: {e.error_code}")
            raise CTraderAuthError(f"token refresh rejected: {e.error_code}") from e

        self._access_token = response.accessToken
        self._refresh_token = response.refreshToken
        self._expires_at_secs = time.time() + response.expiresIn
        self._log.info("Access token refreshed")

        if self._on_tokens_refreshed is not None:
            try:
                self._on_tokens_refreshed(
                    self._access_token,
                    self._refresh_token,
                    self._expires_at_secs,
                )
            except Exception as e:
                # The refresh itself succeeded, so carry on with the new tokens - but loudly:
                # they exist in memory only, and the old refresh token no longer works. Only the
                # type is logged, since the application's message could contain the tokens.
                self._log.error(
                    f"Token persistence callback raised {type(e).__name__}; new tokens not saved",
                )

    async def _authenticate_account(self) -> None:
        await self._connection.request(
            oa.ProtoOAAccountAuthReq(
                ctidTraderAccountId=self._account_id,
                accessToken=self._access_token,
            ),
        )

    def _may_refresh_for(self, error: CTraderRequestError) -> bool:
        """Whether a rejected account authentication is worth a token refresh.

        Only a token problem is fixed by a new token; refreshing for anything else - an unknown
        account, a blocked channel, wrong application credentials - would rotate tokens on every
        retry. The interval covers a venue that keeps reporting a revoked token as expired.
        """
        if self._refresh_token is None or error.error_code not in TOKEN_ERROR_CODES:
            return False
        if self._last_refresh_at is None:
            return True
        return time.time() - self._last_refresh_at >= MIN_TOKEN_REFRESH_INTERVAL_SECS

    async def _refresh_loop(self) -> None:
        while self._expires_at_secs is not None and self._refresh_token is not None:
            now = time.time()
            delay = self._expires_at_secs - TOKEN_REFRESH_MARGIN_SECS - now
            if self._last_refresh_at is not None:
                # TODO(verify): the lifetime a live venue grants. A lifetime shorter than this
                # interval leaves the session without a valid token until the interval passes -
                # chosen over a tight refresh loop.
                delay = max(delay, self._last_refresh_at + MIN_TOKEN_REFRESH_INTERVAL_SECS - now)
            if delay > 0:
                await asyncio.sleep(delay)
                # A refresh made elsewhere may have moved the expiry while this slept.
                continue
            if not self._ready.is_set():
                await self._ready.wait()
                # A reconnect may have refreshed the token itself.
                continue
            try:
                await self.refresh_tokens()
            except (CTraderAuthError, CTraderConnectionError):
                return
            except Exception as e:
                # A timeout or protocol error is transient. `refresh_tokens()` has recorded the
                # attempt, so the retry waits the minimum interval - a human must intervene only
                # if that retry would land too late to matter.
                next_attempt_at = self._last_refresh_at + MIN_TOKEN_REFRESH_INTERVAL_SECS
                if next_attempt_at < self._expires_at_secs:
                    self._log.warning(f"Proactive token refresh failed: {e!r}")
                else:
                    self._log.error(f"Proactive token refresh failed: {e!r}")
                continue
            if self._lost.is_set():
                # A real loss landed while the refresh completed; it keeps its backoff.
                continue
            # Re-authenticate with the new token through the ordinary reconnect path, rather
            # than relying on the venue to end the old session.
            # TODO(verify): whether the venue also sends ProtoOAAccountsTokenInvalidatedEvent
            # after our own refresh; if it does, that costs one extra, harmless reconnect.
            # _loss_cause stays None: no loss has happened since the last bring-up cleared it,
            # and this loop only refreshes while ready.
            self._reauth_requested = True
            self._lost.set()

    def _ends_our_authentication(self, payload: Message) -> bool:
        """Whether the venue has dropped this session's authentication on a live socket.

        All three cases recover the same way: the reconnect path re-authenticates with the
        current token and replays restores, which belong to the account session. A token
        invalidation deliberately does not trigger a refresh - the schema lists "token was
        refreshed" among its causes, so refreshing in response could feed itself forever.
        """
        # TODO(verify): that the venue's reason text never carries account identifiers.
        if isinstance(payload, oa.ProtoOAClientDisconnectEvent):
            reason = payload.reason or "no reason given"
            self._log.error(f"Venue cancelled the application connection: {reason}")
            return True
        if isinstance(payload, oa.ProtoOAAccountDisconnectEvent):
            if payload.ctidTraderAccountId != self._account_id:
                return False
            self._log.warning("Venue dropped the account session, re-authenticating")
            return True
        if isinstance(payload, oa.ProtoOAAccountsTokenInvalidatedEvent):
            ids = payload.ctidTraderAccountIds
            if ids and self._account_id not in ids:
                return False
            reason = payload.reason or "no reason given"
            self._log.warning(f"Access token invalidated ({reason}), re-authenticating")
            return True
        return False

    def _on_event(self, payload: Message) -> None:
        if isinstance(payload, oa.ProtoOARefreshTokenRes):
            # A refresh reply that arrived after its request timed out. It carries a token pair,
            # which must never travel to the application's event handler.
            # TODO(verify): whether the venue invalidates a refresh token once used; if it does,
            # the pair in a late reply is the only valid one and should be adopted, not dropped.
            self._log.warning("Dropped a late token refresh response")
            return
        if self._ends_our_authentication(payload) and not self._stopping:
            self._state = SessionState.CONNECTING
            self._ready.clear()
            self._loss_cause = None
            self._lost.set()
        if self._event_handler is not None:
            self._event_handler(payload)
