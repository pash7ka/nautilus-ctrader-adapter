"""The cTrader socket connection: framing, correlation, heartbeat.

Owns exactly one socket and knows nothing about authentication or accounts. It reports a lost
connection and stops; reconnection belongs to the session. That split is what lets this layer
be tested against the fake server with no authentication in the picture.

Payload bytes are never logged. DEBUG carries payload type, correlation id and byte length,
which makes secret-masking a property of the layer rather than a rule to remember.
"""

from __future__ import annotations

import asyncio
import contextlib
import ssl
import uuid
from collections.abc import Callable

from google.protobuf.message import Message
from nautilus_trader.common.component import Logger

from nautilus_ctrader.common import codec
from nautilus_ctrader.common.errors import (
    CTraderConnectionError,
    CTraderProtocolError,
    CTraderRequestError,
    CTraderTimeoutError,
)
from nautilus_ctrader.common.rate_limit import RateLimiter
from nautilus_ctrader.constants import (
    BUCKET_DEFAULT,
    DEFAULT_REQUEST_TIMEOUT_SECS,
    HEARTBEAT_IDLE_SECS,
    LENGTH_PREFIX_BYTES,
)
from nautilus_ctrader.messages import OpenApiCommonMessages_pb2 as common
from nautilus_ctrader.messages import OpenApiCommonModelMessages_pb2 as common_model


class CTraderConnection:
    def __init__(
        self,
        host: str,
        port: int,
        *,
        logger: Logger,
        rate_limiter: RateLimiter | None = None,
        heartbeat_idle_secs: float = HEARTBEAT_IDLE_SECS,
        request_timeout_secs: float = DEFAULT_REQUEST_TIMEOUT_SECS,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._log = logger
        self._rate_limiter = rate_limiter
        self._heartbeat_idle_secs = heartbeat_idle_secs
        self._request_timeout_secs = request_timeout_secs
        self._ssl_context = ssl_context

        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._read_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._pending: dict[str, asyncio.Future[Message]] = {}
        self._last_send = 0.0
        self._connected = False

        self._event_handler: Callable[[Message], None] | None = None
        self._disconnect_handler: Callable[[Exception], None] | None = None

    @property
    def is_connected(self) -> bool:
        return self._connected

    def set_event_handler(self, handler: Callable[[Message], None]) -> None:
        self._event_handler = handler

    def set_disconnect_handler(self, handler: Callable[[Exception], None]) -> None:
        self._disconnect_handler = handler

    async def connect(self) -> None:
        try:
            self._reader, self._writer = await asyncio.open_connection(
                self._host,
                self._port,
                ssl=self._ssl_context,
            )
        except OSError as e:
            raise CTraderConnectionError(f"cannot connect to {self._host}:{self._port}") from e

        self._connected = True
        self._last_send = asyncio.get_running_loop().time()
        self._read_task = asyncio.create_task(self._read_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._log.info(f"Connected to {self._host}:{self._port}")

    async def close(self) -> None:
        self._connected = False
        for task in (self._heartbeat_task, self._read_task):
            if task is not None:
                task.cancel()
        for task in (self._heartbeat_task, self._read_task):
            if task is not None:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        self._heartbeat_task = None
        self._read_task = None

        if self._writer is not None:
            self._writer.close()
            with contextlib.suppress(ConnectionError, OSError):
                await self._writer.wait_closed()
        self._writer = None
        self._reader = None
        self._reject_pending(CTraderConnectionError("connection closed"))

    async def request(
        self,
        payload: Message,
        *,
        timeout_secs: float | None = None,
        bucket: str = BUCKET_DEFAULT,
    ) -> Message:
        """Send a request and await its correlated response."""
        if not self._connected:
            raise CTraderConnectionError("not connected")

        client_msg_id = str(uuid.uuid4())
        future: asyncio.Future[Message] = asyncio.get_running_loop().create_future()
        self._pending[client_msg_id] = future
        deadline = self._request_timeout_secs if timeout_secs is None else timeout_secs
        try:
            frame = codec.encode_frame(payload, client_msg_id)
            self._log_send(payload, client_msg_id, frame)
            await self._write(frame, bucket=bucket)
            return await asyncio.wait_for(future, deadline)
        except TimeoutError as e:
            raise CTraderTimeoutError(
                f"no response to payloadType={payload.payloadType} within {deadline}s",
            ) from e
        except CTraderRequestError as e:
            if e.retry_after_secs is not None and self._rate_limiter is not None:
                self._log.warning(
                    f"Rate limited on bucket '{bucket}', pausing {e.retry_after_secs}s",
                )
                self._rate_limiter.pause(bucket, e.retry_after_secs)
            raise
        finally:
            self._pending.pop(client_msg_id, None)

    async def send(
        self,
        payload: Message,
        *,
        client_msg_id: str | None = None,
        bucket: str = BUCKET_DEFAULT,
    ) -> None:
        """Send a message without awaiting a response."""
        if not self._connected:
            raise CTraderConnectionError("not connected")
        frame = codec.encode_frame(payload, client_msg_id)
        self._log_send(payload, client_msg_id, frame)
        await self._write(frame, bucket=bucket)

    def _log_send(self, payload: Message, client_msg_id: str | None, frame: bytes) -> None:
        self._log.debug(
            f"send payloadType={payload.payloadType} "
            f"clientMsgId={client_msg_id or '-'} bytes={len(frame)}",
        )

    async def _write(self, frame: bytes, *, bucket: str | None) -> None:
        if bucket is not None and self._rate_limiter is not None:
            await self._rate_limiter.acquire(bucket)
        if self._writer is None:
            raise CTraderConnectionError("not connected")
        self._writer.write(frame)
        self._last_send = asyncio.get_running_loop().time()
        try:
            await self._writer.drain()
        except (ConnectionError, OSError) as e:
            raise CTraderConnectionError(f"write failed: {e!r}") from e

    def _write_now(self, frame: bytes) -> None:
        """Write without awaiting the rate limiter or the drain.

        Used only for heartbeats: a paused bucket must never suppress a keep-alive, or a
        rate-limit breach would escalate into a dropped connection. The frame is a handful of
        bytes, so skipping `drain()` costs nothing and keeps the dispatch path synchronous.
        """
        if self._writer is None:
            return
        self._writer.write(frame)
        self._last_send = asyncio.get_running_loop().time()

    async def _read_loop(self) -> None:
        assert self._reader is not None
        try:
            while True:
                prefix = await self._reader.readexactly(LENGTH_PREFIX_BYTES)
                body = await self._reader.readexactly(codec.decode_length(prefix))
                self._dispatch(codec.decode_envelope(body))
        except asyncio.CancelledError:
            raise
        except asyncio.IncompleteReadError:
            self._fail(CTraderConnectionError("connection closed by peer"))
        except CTraderProtocolError as e:
            self._log.error(f"Protocol error, dropping connection: {e}")
            self._fail(e)
        except (ConnectionError, OSError) as e:
            self._fail(CTraderConnectionError(f"read failed: {e!r}"))

    def _dispatch(self, envelope: common.ProtoMessage) -> None:
        self._log.debug(
            f"recv payloadType={envelope.payloadType} "
            f"clientMsgId={envelope.clientMsgId or '-'} bytes={len(envelope.payload)}",
        )

        if envelope.payloadType == common_model.HEARTBEAT_EVENT:
            self._write_now(codec.encode_frame(common.ProtoHeartbeatEvent()))
            return

        try:
            payload = codec.parse_payload(envelope)
        except CTraderProtocolError as e:
            # An unrecognised message must not take the connection down: the schema grows.
            self._log.warning(f"Ignoring undecodable message: {e}")
            return

        future = self._pending.pop(envelope.clientMsgId, None) if envelope.clientMsgId else None
        if future is not None and not future.done():
            error = codec.as_request_error(payload)
            if error is not None:
                future.set_exception(error)
            else:
                future.set_result(payload)
            return

        if self._event_handler is not None:
            self._event_handler(payload)

    async def _heartbeat_loop(self) -> None:
        interval = max(self._heartbeat_idle_secs / 2.0, 0.01)
        while True:
            await asyncio.sleep(interval)
            if not self._connected or self._writer is None:
                return
            idle = asyncio.get_running_loop().time() - self._last_send
            if idle >= self._heartbeat_idle_secs:
                self._write_now(codec.encode_frame(common.ProtoHeartbeatEvent()))

    def _fail(self, error: Exception) -> None:
        was_connected = self._connected
        self._connected = False
        self._reject_pending(error)
        if was_connected and self._disconnect_handler is not None:
            self._disconnect_handler(error)

    def _reject_pending(self, error: Exception) -> None:
        pending, self._pending = self._pending, {}
        for future in pending.values():
            if not future.done():
                future.set_exception(error)
