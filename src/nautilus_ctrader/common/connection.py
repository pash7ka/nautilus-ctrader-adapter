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
    CONNECT_TIMEOUT_SECS,
    DEFAULT_REQUEST_TIMEOUT_SECS,
    HEARTBEAT_IDLE_SECS,
    INBOUND_SILENCE_SECS,
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
        inbound_silence_secs: float = INBOUND_SILENCE_SECS,
        request_timeout_secs: float = DEFAULT_REQUEST_TIMEOUT_SECS,
        connect_timeout_secs: float = CONNECT_TIMEOUT_SECS,
        tls: ssl.SSLContext | bool = True,
    ) -> None:
        """
        `tls` is passed to `asyncio.open_connection` as `ssl`: `True` (a verifying default
        context) is the only safe choice against a real venue; `False` (plaintext) is for tests
        against the local fake server.
        """
        self._host = host
        self._port = port
        self._log = logger
        self._rate_limiter = rate_limiter
        self._heartbeat_idle_secs = heartbeat_idle_secs
        self._inbound_silence_secs = inbound_silence_secs
        self._request_timeout_secs = request_timeout_secs
        self._connect_timeout_secs = connect_timeout_secs
        self._tls = tls

        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._read_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._pending: dict[str, asyncio.Future[Message]] = {}
        self._last_send = 0.0
        self._last_receive = 0.0
        self._connected = False
        self._generation = 0

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
            async with asyncio.timeout(self._connect_timeout_secs):
                self._reader, self._writer = await asyncio.open_connection(
                    self._host,
                    self._port,
                    ssl=self._tls,
                )
        except TimeoutError as e:
            # Caught before OSError, of which TimeoutError is a subclass.
            raise CTraderConnectionError(
                f"timed out connecting to {self._host}:{self._port}",
            ) from e
        except OSError as e:
            raise CTraderConnectionError(f"cannot connect to {self._host}:{self._port}") from e

        self._generation += 1
        self._connected = True
        now = asyncio.get_running_loop().time()
        self._last_send = now
        self._last_receive = now
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
            if future.done() and not future.cancelled():
                future.exception()

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
        generation = self._generation
        if bucket is not None and self._rate_limiter is not None:
            await self._rate_limiter.acquire(bucket)
        # The wait can outlast the connection it began on. A frame queued for a closed socket
        # must fail, never go out on the next one.
        if not self._connected or self._writer is None or self._generation != generation:
            raise CTraderConnectionError("connection lost while waiting to send")
        self._writer.write(frame)
        self._last_send = asyncio.get_running_loop().time()
        try:
            await self._writer.drain()
        except (ConnectionError, OSError) as e:
            raise CTraderConnectionError(f"write failed: {e!r}") from e

    def _write_now(self, frame: bytes) -> None:
        """Write without awaiting the rate limiter or the drain.

        Used only for idle heartbeats: a paused bucket must never suppress a keep-alive, or a
        rate-limit breach would escalate into a dropped connection. The frame is a handful of
        bytes, so skipping `drain()` costs nothing.
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
                self._last_receive = asyncio.get_running_loop().time()
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
        except Exception as e:
            # Anything unexpected must still mark the connection down, or it would keep
            # reporting connected with no reader behind it.
            self._log.error(f"Read loop failed: {e!r}")
            self._fail(CTraderConnectionError(f"read loop failed: {e!r}"))

    def _dispatch(self, envelope: common.ProtoMessage) -> None:
        self._log.debug(
            f"recv payloadType={envelope.payloadType} "
            f"clientMsgId={envelope.clientMsgId or '-'} bytes={len(envelope.payload)}",
        )

        if envelope.payloadType == common_model.HEARTBEAT_EVENT:
            # Not answered. Spotware's SDK replies to every inbound heartbeat, but the schema
            # describes heartbeats as keep-alive, not ping/pong. The idle timer keeps our side
            # alive well inside the server's 30 s tolerance, and never replying means no peer
            # can drive a heartbeat loop.
            # TODO(verify): confirm on a live connection that the venue expects no reply.
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

        self._call_handler("Event", self._event_handler, payload)

    async def _heartbeat_loop(self) -> None:
        """Watch both directions: send a heartbeat on outbound idle, fail on inbound silence.

        Ticks at half the shorter of the two bounds, so a long heartbeat interval can never
        delay noticing that the venue has gone quiet.
        """
        interval = max(min(self._heartbeat_idle_secs, self._inbound_silence_secs) / 2.0, 0.01)
        while True:
            await asyncio.sleep(interval)
            if not self._connected or self._writer is None:
                return
            now = asyncio.get_running_loop().time()
            if now - self._last_receive >= self._inbound_silence_secs:
                self._log.warning(
                    f"No data from the venue for {self._inbound_silence_secs:g}s, "
                    "treating the connection as lost",
                )
                self._fail(
                    CTraderConnectionError(
                        f"no inbound data for {self._inbound_silence_secs:g}s",
                    ),
                )
                return
            idle = now - self._last_send
            if idle >= self._heartbeat_idle_secs:
                self._write_now(codec.encode_frame(common.ProtoHeartbeatEvent()))

    def _call_handler(self, name: str, handler: Callable[..., None] | None, arg: object) -> None:
        """Run a caller-supplied handler so that a bug in it cannot take the connection down."""
        if handler is None:
            return
        try:
            handler(arg)
        except Exception as e:
            self._log.error(f"{name} handler raised {e!r}; continuing")

    def _fail(self, error: Exception) -> None:
        was_connected = self._connected
        self._connected = False
        self._reject_pending(error)
        if was_connected:
            self._call_handler("Disconnect", self._disconnect_handler, error)

    def _reject_pending(self, error: Exception) -> None:
        pending, self._pending = self._pending, {}
        for future in pending.values():
            if not future.done():
                future.set_exception(error)
