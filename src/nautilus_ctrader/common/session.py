"""The cTrader session: authentication, reconnect, and subscription restore.

A state machine over a connection it owns. Upper layers register a restore action once and
never handle reconnection themselves, which is what keeps recovery behaviour identical on
every path rather than subtly different per client.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import ssl
from collections.abc import Awaitable, Callable, Hashable
from enum import Enum, auto

from google.protobuf.message import Message
from nautilus_trader.common.component import Logger

from nautilus_ctrader.common.connection import CTraderConnection
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
    DEFAULT_RATE_LIMIT_PER_SEC,
    DEFAULT_REQUEST_TIMEOUT_SECS,
    HEARTBEAT_IDLE_SECS,
    HISTORICAL_RATE_LIMIT_PER_SEC,
    RECONNECT_FAILURE_THRESHOLD,
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
        ssl_context: ssl.SSLContext | None = None,
        rate_limiter: RateLimiter | None = None,
        heartbeat_idle_secs: float = HEARTBEAT_IDLE_SECS,
        request_timeout_secs: float = DEFAULT_REQUEST_TIMEOUT_SECS,
        backoff_base_secs: float = BACKOFF_BASE_SECS,
        backoff_max_secs: float = BACKOFF_MAX_SECS,
        failure_threshold: int = RECONNECT_FAILURE_THRESHOLD,
    ) -> None:
        self._host = host
        self._port = port
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
            request_timeout_secs=request_timeout_secs,
            ssl_context=ssl_context,
        )
        self._connection.set_event_handler(self._on_event)
        self._connection.set_disconnect_handler(self._on_disconnect)

        self._state = SessionState.STOPPED
        self._restores: dict[Hashable, Callable[[], Awaitable[None]]] = {}
        self._event_handler: Callable[[Message], None] | None = None
        self._ready = asyncio.Event()
        self._lost = asyncio.Event()
        self._supervisor: asyncio.Task | None = None
        self._stopping = False
        self.last_error: Exception | None = None

    @property
    def state(self) -> SessionState:
        return self._state

    @property
    def is_ready(self) -> bool:
        return self._state is SessionState.READY

    def set_event_handler(self, handler: Callable[[Message], None]) -> None:
        self._event_handler = handler

    def add_restore(self, key: Hashable, factory: Callable[[], Awaitable[None]]) -> None:
        """Register an action to replay after every successful authentication."""
        self._restores[key] = factory

    def remove_restore(self, key: Hashable) -> None:
        self._restores.pop(key, None)

    async def wait_ready(self, timeout_secs: float | None = None) -> None:
        await asyncio.wait_for(self._ready.wait(), timeout_secs)

    async def start(self) -> None:
        """Start the supervisor. Returns before the first authentication completes."""
        if self._supervisor is not None and not self._supervisor.done():
            return
        self._stopping = False
        self._supervisor = asyncio.create_task(self._supervise())

    async def stop(self) -> None:
        self._stopping = True
        self._lost.set()
        if self._supervisor is not None:
            self._supervisor.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._supervisor
            self._supervisor = None
        await self._connection.close()
        self._state = SessionState.STOPPED
        self._ready.clear()

    async def request(
        self,
        payload: Message,
        *,
        timeout_secs: float | None = None,
        bucket: str = BUCKET_DEFAULT,
    ) -> Message:
        """Issue a request, failing fast when the session is not ready.

        Subscriptions survive a reconnect through the restore registry, so anything else is
        better refused than silently delayed.
        """
        if self._state not in (SessionState.READY, SessionState.RESTORING):
            raise CTraderConnectionError(f"session not ready (state={self._state.name})")
        return await self._connection.request(
            payload,
            timeout_secs=timeout_secs,
            bucket=bucket,
        )

    async def _supervise(self) -> None:
        attempt = 0
        while not self._stopping:
            try:
                await self._bring_up()
                attempt = 0
                await self._lost.wait()
                if self._stopping:
                    return
                self._log.warning("Connection lost, reconnecting")
                # The old socket may still be live (the venue dropped our authentication) or
                # half-dead (a failed read leaves the writer and heartbeat task running). Either
                # way it is closed before a new one opens.
                await self._teardown_connection()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.last_error = e
                attempt += 1
                delay = min(
                    self._backoff_max_secs,
                    self._backoff_base_secs * 2 ** (attempt - 1),
                )
                delay *= 1.0 + random.random() * BACKOFF_JITTER
                message = f"Session bring-up failed (attempt {attempt}): {e}"
                if attempt >= self._failure_threshold:
                    self._log.error(f"{message}; retrying in {delay:.1f}s")
                else:
                    self._log.warning(f"{message}; retrying in {delay:.1f}s")
                await self._teardown_connection()
                await asyncio.sleep(delay)

    async def _bring_up(self) -> None:
        # Cleared before connecting, not after: a loss landing while bring-up is finishing
        # must survive to wake the supervisor, or it would wait forever on a dead socket.
        self._lost.clear()
        self._ready.clear()
        self._state = SessionState.CONNECTING
        await self._connection.connect()

        self._state = SessionState.AUTHENTICATING
        await self._authenticate()

        self._state = SessionState.RESTORING
        for key, factory in list(self._restores.items()):
            self._log.info(f"Restoring {key!r}")
            try:
                await factory()
            except CTraderConnectionError:
                # The connection itself is gone: a bring-up failure, not a bad restore.
                raise
            except Exception as e:
                # One rejected restore must not keep the whole session down. It is logged, and
                # the key stays registered so the next reconnect retries it.
                detail = e.error_code if isinstance(e, CTraderRequestError) else repr(e)
                self._log.error(f"Restore {key!r} failed: {detail}")

        if self._lost.is_set():
            # A loss can surface as any error type - a protocol error rejects pending requests
            # with itself - so decide by what happened to the connection, not by the exception.
            raise CTraderConnectionError(f"connection lost during bring-up: {self.last_error!r}")

        self._state = SessionState.READY
        self.last_error = None
        self._ready.set()
        self._log.info("Session ready")

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
            await self._connection.request(
                oa.ProtoOAAccountAuthReq(
                    ctidTraderAccountId=self._account_id,
                    accessToken=self._access_token,
                ),
            )
        except CTraderRequestError as e:
            raise CTraderAuthError(f"account auth rejected: {e.error_code}") from e

    async def _teardown_connection(self) -> None:
        self._state = SessionState.CONNECTING
        self._ready.clear()
        await self._connection.close()

    def _on_disconnect(self, error: Exception) -> None:
        self.last_error = error
        self._state = SessionState.CONNECTING
        self._ready.clear()
        self._lost.set()

    def _on_event(self, payload: Message) -> None:
        if self._event_handler is not None:
            self._event_handler(payload)
