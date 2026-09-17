"""The session: authentication order, subscription restore, and reconnect."""

import asyncio
import contextlib
import struct

import pytest
from nautilus_trader.common.component import Logger

from nautilus_ctrader.common.connection import CTraderConnection
from nautilus_ctrader.common.errors import (
    CTraderAuthError,
    CTraderConnectionError,
    CTraderProtocolError,
)
from nautilus_ctrader.common.rate_limit import RateLimiter
from nautilus_ctrader.common.session import CTraderSession, SessionState
from nautilus_ctrader.constants import (
    BUCKET_DEFAULT,
    BUCKET_HISTORICAL,
    LENGTH_PREFIX_FORMAT,
    MAX_FRAME_BYTES,
)
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from tests.fake_server import FakeCTraderServer
from tests.polling import wait_until
from tests.recording_logger import RecordingLogger

ACCOUNT_ID = 1234567


def _authenticating_server() -> FakeCTraderServer:
    server = FakeCTraderServer()
    server.on(
        oa_model.PROTO_OA_APPLICATION_AUTH_REQ,
        lambda _r: oa.ProtoOAApplicationAuthRes(),
    )
    server.on(
        oa_model.PROTO_OA_ACCOUNT_AUTH_REQ,
        lambda _r: oa.ProtoOAAccountAuthRes(ctidTraderAccountId=ACCOUNT_ID),
    )
    return server


def _session(
    server: FakeCTraderServer,
    logger: Logger | RecordingLogger | None = None,
    **kwargs,
) -> CTraderSession:
    return CTraderSession(
        host=server.host,
        port=server.port,
        client_id="client-id",
        client_secret="client-secret",
        account_id=ACCOUNT_ID,
        access_token="access-token",
        logger=Logger("test") if logger is None else logger,
        tls=False,
        **kwargs,
    )


async def test_start_authenticates_application_then_account() -> None:
    server = _authenticating_server()
    await server.start()
    session = _session(server)
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        assert session.state is SessionState.READY
        assert [type(m).__name__ for m in server.received] == [
            "ProtoOAApplicationAuthReq",
            "ProtoOAAccountAuthReq",
        ]
    finally:
        await session.stop()
        await server.stop()


async def test_a_rejected_application_auth_never_becomes_ready() -> None:
    server = FakeCTraderServer()
    server.on(
        oa_model.PROTO_OA_APPLICATION_AUTH_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="INVALID_REQUEST", description="bad client"),
    )
    await server.start()
    session = _session(server, backoff_base_secs=0.05, failure_threshold=1)
    try:
        await session.start()
        with pytest.raises(TimeoutError):
            await session.wait_ready(timeout_secs=0.4)
        assert session.state is not SessionState.READY
    finally:
        await session.stop()
        await server.stop()


async def test_restore_actions_run_after_authentication() -> None:
    server = _authenticating_server()
    server.on(
        oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
        lambda _r: oa.ProtoOASubscribeSpotsRes(ctidTraderAccountId=ACCOUNT_ID),
    )
    await server.start()
    session = _session(server)

    async def subscribe() -> None:
        await session.request(
            oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=ACCOUNT_ID, symbolId=[1]),
        )

    try:
        session.add_restore(("spots", 1), subscribe)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        assert any(isinstance(m, oa.ProtoOASubscribeSpotsReq) for m in server.received)
    finally:
        await session.stop()
        await server.stop()


async def test_a_dropped_connection_reauthenticates_and_replays_restores() -> None:
    server = _authenticating_server()
    server.on(
        oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
        lambda _r: oa.ProtoOASubscribeSpotsRes(ctidTraderAccountId=ACCOUNT_ID),
    )
    await server.start()
    session = _session(server, backoff_base_secs=0.05)

    async def subscribe() -> None:
        await session.request(
            oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=ACCOUNT_ID, symbolId=[1]),
        )

    try:
        session.add_restore(("spots", 1), subscribe)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        server.received.clear()

        await server.drop_connections()
        await wait_until(lambda: server.connection_count >= 2)
        await session.wait_ready(timeout_secs=3.0)

        replayed = [type(m).__name__ for m in server.received]
        assert replayed == [
            "ProtoOAApplicationAuthReq",
            "ProtoOAAccountAuthReq",
            "ProtoOASubscribeSpotsReq",
        ]
        assert server.connection_count >= 2
    finally:
        await session.stop()
        await server.stop()


async def test_a_silent_venue_makes_the_session_reconnect() -> None:
    # The fake server answers only auth requests; after authentication it goes silent, which is
    # exactly the half-open-connection case. Our own heartbeats get no answer either, but that
    # does not matter here - the inbound-silence watchdog is what must notice and reconnect.
    server = _authenticating_server()
    server.answer_heartbeats = False
    await server.start()
    session = _session(
        server,
        heartbeat_idle_secs=0.05,
        inbound_silence_secs=0.3,
        backoff_base_secs=0.05,
    )
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        await wait_until(lambda: server.connection_count >= 2, description="reconnect")
        await session.wait_ready(timeout_secs=3.0)
    finally:
        await session.stop()
        await server.stop()


async def test_a_removed_restore_is_not_replayed() -> None:
    server = _authenticating_server()
    server.on(
        oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
        lambda _r: oa.ProtoOASubscribeSpotsRes(ctidTraderAccountId=ACCOUNT_ID),
    )
    await server.start()
    session = _session(server, backoff_base_secs=0.05)

    async def subscribe() -> None:
        await session.request(
            oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=ACCOUNT_ID, symbolId=[1]),
        )

    try:
        session.add_restore(("spots", 1), subscribe)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        session.remove_restore(("spots", 1))
        server.received.clear()
        await server.drop_connections()
        await wait_until(lambda: server.connection_count >= 2)
        await session.wait_ready(timeout_secs=3.0)

        assert not any(isinstance(m, oa.ProtoOASubscribeSpotsReq) for m in server.received)
    finally:
        await session.stop()
        await server.stop()


async def test_a_request_while_not_ready_fails_fast() -> None:
    server = _authenticating_server()
    await server.start()
    session = _session(server)
    try:
        with pytest.raises(CTraderConnectionError, match="not ready"):
            await session.request(oa.ProtoOATraderReq(ctidTraderAccountId=ACCOUNT_ID))
    finally:
        await session.stop()
        await server.stop()


async def test_unsolicited_events_reach_the_event_handler() -> None:
    server = _authenticating_server()
    await server.start()
    session = _session(server)
    seen: list[object] = []
    try:
        session.set_event_handler(seen.append)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        await server.push(
            oa.ProtoOASpotEvent(ctidTraderAccountId=ACCOUNT_ID, symbolId=1, bid=1, ask=2),
        )
        await asyncio.sleep(0.2)
        assert any(isinstance(m, oa.ProtoOASpotEvent) for m in seen)
    finally:
        await session.stop()
        await server.stop()


async def test_a_rejected_auth_is_recorded_as_last_error() -> None:
    # `start()` returns once the supervisor is running, so a rejection surfaces through
    # `last_error` rather than as an exception from `start()`.
    server = FakeCTraderServer()
    server.on(
        oa_model.PROTO_OA_APPLICATION_AUTH_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="INVALID_REQUEST"),
    )
    await server.start()
    session = _session(server, backoff_base_secs=0.05)
    try:
        await session.start()
        await wait_until(lambda: isinstance(session.last_error, CTraderAuthError))
        assert isinstance(session.last_error, CTraderAuthError)
    finally:
        await session.stop()
        await server.stop()


async def test_a_failing_restore_does_not_keep_the_session_down() -> None:
    server = _authenticating_server()
    server.on(
        oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="ENTITY_NOT_FOUND", description="gone"),
    )
    await server.start()
    session = _session(server, backoff_base_secs=0.05)
    attempts: list[int] = []

    async def subscribe() -> None:
        attempts.append(1)
        await session.request(
            oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=ACCOUNT_ID, symbolId=[1]),
        )

    try:
        session.add_restore(("spots", 1), subscribe)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        assert session.state is SessionState.READY
        assert server.connection_count == 1
        assert len(attempts) == 1

        # The failed key stays registered, so the next reconnect retries it.
        await server.drop_connections()
        await wait_until(lambda: server.connection_count >= 2)
        await session.wait_ready(timeout_secs=3.0)
        assert len(attempts) == 2
    finally:
        await session.stop()
        await server.stop()


async def test_a_restore_raising_a_connection_error_is_a_bring_up_failure() -> None:
    # The exception clause: a connection error from a restore must not be contained.
    server = _authenticating_server()
    await server.start()
    session = _session(server, backoff_base_secs=0.05)
    attempts: list[int] = []

    async def restore() -> None:
        attempts.append(1)
        if len(attempts) == 1:
            raise CTraderConnectionError("connection lost during restore")

    try:
        session.add_restore("flaky", restore)
        await session.start()
        await session.wait_ready(timeout_secs=3.0)
        assert len(attempts) == 2
        assert server.connection_count >= 2
    finally:
        await session.stop()
        await server.stop()


async def test_starting_twice_runs_a_single_supervisor() -> None:
    server = _authenticating_server()
    await server.start()
    session = _session(server)
    try:
        await session.start()
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        # A fixed wait is right here: this checks that a second connection never appears.
        await asyncio.sleep(0.1)
        assert server.connection_count == 1
    finally:
        await session.stop()
        await server.stop()


async def test_a_session_lost_soon_after_ready_backs_off() -> None:
    # A peer that drops every new session must not be hammered with immediate reconnects.
    server = _authenticating_server()
    await server.start()
    session = _session(server, backoff_base_secs=1.0)
    try:
        await session.start()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 2.0
        while loop.time() < deadline:
            if session.is_ready:
                before = server.connection_count
                await server.drop_connections()
                # Without backoff the session can be ready again before a poll sees it down.
                await wait_until(
                    lambda: not session.is_ready or server.connection_count > before,  # noqa: B023
                    description="loss being noticed",
                )
            await asyncio.sleep(0.01)

        assert server.connection_count <= 3
    finally:
        await session.stop()
        await server.stop()


async def test_a_protocol_error_during_a_restore_is_a_bring_up_failure() -> None:
    # A malformed frame rejects pending requests with the protocol error itself, not a
    # connection error. Bring-up must still fail - and back off - rather than mark a dead
    # socket ready and reconnect immediately.
    server = _authenticating_server()
    await server.start()
    logger = RecordingLogger()
    session = _session(server, logger=logger, backoff_base_secs=0.5)
    attempts: list[int] = []

    async def restore() -> None:
        attempts.append(1)
        if len(attempts) > 1:
            return
        # No handler is registered for this request, so it stays pending until the bad
        # frame arrives and the connection rejects it.
        pending = asyncio.create_task(
            session.request(
                oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=ACCOUNT_ID, symbolId=[1]),
            ),
        )
        try:
            await wait_until(
                lambda: any(isinstance(m, oa.ProtoOASubscribeSpotsReq) for m in server.received),
                description="restore request reaching the server",
            )
            await server.push_raw(struct.pack(LENGTH_PREFIX_FORMAT, MAX_FRAME_BYTES + 1))
            await pending
        finally:
            pending.cancel()

    try:
        session.add_restore("corrupted", restore)
        loop = asyncio.get_running_loop()
        started = loop.time()
        await session.start()
        await session.wait_ready(timeout_secs=5.0)

        # Reaching READY straight away would mean the dead socket was marked ready.
        assert loop.time() - started >= 0.4
        assert server.connection_count >= 2
        assert len(attempts) == 2
        # The loss is the cause, and the connection has already logged it.
        assert not any("Restore" in line for line in logger.errors())
    finally:
        await session.stop()
        await server.stop()


async def test_a_loss_after_a_stable_session_reconnects_at_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A session that had been ready for a while is recovering, not failing: the attempt
    # counter resets and the next connection is not made to wait out a backoff.
    monkeypatch.setattr("nautilus_ctrader.common.session.STABLE_SESSION_SECS", 0.0)
    server = _authenticating_server()
    await server.start()
    logger = RecordingLogger()
    session = CTraderSession(
        host=server.host,
        port=server.port,
        client_id="client-id",
        client_secret="client-secret",
        account_id=ACCOUNT_ID,
        access_token="access-token",
        logger=logger,
        tls=False,
        backoff_base_secs=5.0,
    )
    loop = asyncio.get_running_loop()
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        for _ in range(2):
            before = server.connection_count
            started = loop.time()
            await server.drop_connections()
            await wait_until(
                lambda: server.connection_count > before,  # noqa: B023
                description="reconnect after a stable loss",
            )
            await session.wait_ready(timeout_secs=3.0)
            elapsed = loop.time() - started
            assert elapsed < 2.0, f"reconnect took {elapsed:.2f}s, a backoff appears to apply"

        assert any(
            level == "warning" and "Connection lost, reconnecting" in message
            for level, message in logger.lines
        )
    finally:
        await session.stop()
        await server.stop()


async def test_a_loss_right_after_authenticating_skips_restoring(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A loss landing the instant authentication finishes must be caught before RESTORING
    # starts, not only after the restore loop runs - or a restore would go out on a socket
    # whose authentication just dropped.
    server = _authenticating_server()
    await server.start()
    session = _session(server, backoff_base_secs=0.05)
    attempts: list[int] = []
    triggered = False
    original_authenticate = CTraderSession._authenticate

    async def authenticate_then_lose(self: CTraderSession) -> None:
        nonlocal triggered
        await original_authenticate(self)
        if not triggered:
            triggered = True
            self._on_event(oa.ProtoOAAccountDisconnectEvent(ctidTraderAccountId=ACCOUNT_ID))
            # Bring-up has not moved past this point yet, so the session must already refuse
            # requests rather than let one through on the socket that just lost authentication.
            with pytest.raises(CTraderConnectionError, match="not ready"):
                await self.request(oa.ProtoOATraderReq(ctidTraderAccountId=ACCOUNT_ID))

    monkeypatch.setattr(CTraderSession, "_authenticate", authenticate_then_lose)

    async def restore() -> None:
        attempts.append(1)

    try:
        session.add_restore("r", restore)
        await session.start()
        await session.wait_ready(timeout_secs=3.0)

        # The first attempt's loss must be caught before the restore ever runs.
        assert attempts == [1]
    finally:
        await session.stop()
        await server.stop()


async def test_a_bring_up_failure_chains_the_real_disconnect_reason() -> None:
    # A protocol error during restore rejects the pending request with itself; the bring-up
    # failure raised from it must still carry that real reason in its exception chain.
    server = _authenticating_server()
    await server.start()
    session = _session(server, backoff_base_secs=1.0)
    attempts: list[int] = []

    async def restore() -> None:
        attempts.append(1)
        if len(attempts) > 1:
            return
        pending = asyncio.create_task(
            session.request(
                oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=ACCOUNT_ID, symbolId=[1]),
            ),
        )
        try:
            await wait_until(
                lambda: any(isinstance(m, oa.ProtoOASubscribeSpotsReq) for m in server.received),
                description="restore request reaching the server",
            )
            await server.push_raw(struct.pack(LENGTH_PREFIX_FORMAT, MAX_FRAME_BYTES + 1))
            await pending
        finally:
            pending.cancel()

    try:
        session.add_restore("corrupted", restore)
        await session.start()

        await wait_until(
            lambda: isinstance(session.last_error, CTraderConnectionError),
            description="bring-up failure",
        )
        failure = session.last_error
        assert isinstance(failure, CTraderConnectionError)
        assert isinstance(failure.__cause__, CTraderProtocolError)

        await session.wait_ready(timeout_secs=5.0)
    finally:
        await session.stop()
        await server.stop()


async def test_a_bring_up_loss_through_an_event_does_not_chain_a_stale_cause() -> None:
    # An auth-loss event (unlike a socket loss) carries no exception of its own. If the bring-up
    # failure it triggers still chains to `last_error`, repeated failures chain to each other and
    # the public `__cause__` ends up pointing at an unrelated, older failure.
    server = _authenticating_server()
    await server.start()
    session = _session(server, backoff_base_secs=0.05)

    async def restore() -> None:
        session._on_event(oa.ProtoOAAccountDisconnectEvent(ctidTraderAccountId=ACCOUNT_ID))

    try:
        session.add_restore("dropped", restore)
        await session.start()
        await wait_until(
            lambda: server.connection_count >= 3,
            description="repeated bring-up failures",
        )

        failure = session.last_error
        assert isinstance(failure, CTraderConnectionError)
        assert failure.__cause__ is None
    finally:
        await session.stop()
        await server.stop()


async def test_connect_timeout_secs_is_forwarded_to_the_connection() -> None:
    # A peer that accepts the TCP connection but never completes a TLS handshake stands in for
    # a black-holed host; a short connect timeout must reach the underlying connection, not
    # wait out its 10s default. `tls` is left at its verifying default so the handshake is the
    # thing that hangs.
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        with contextlib.suppress(ConnectionError):
            await reader.read()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    session = CTraderSession(
        host="127.0.0.1",
        port=port,
        client_id="client-id",
        client_secret="client-secret",
        account_id=ACCOUNT_ID,
        access_token="access-token",
        logger=Logger("test"),
        connect_timeout_secs=0.2,
        backoff_base_secs=5.0,
    )
    try:
        await session.start()
        await wait_until(
            lambda: session.last_error is not None,
            timeout_secs=1.0,
            description="connect timeout being recorded",
        )
        assert "timed out" in str(session.last_error)
    finally:
        await session.stop()
        server.close()
        await server.wait_closed()


async def test_failed_restores_reports_the_keys_that_failed_in_the_last_bring_up() -> None:
    server = _authenticating_server()
    replies = iter([oa.ProtoOAErrorRes(errorCode="ENTITY_NOT_FOUND", description="gone")])
    server.on(
        oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
        lambda _r: next(replies, oa.ProtoOASubscribeSpotsRes(ctidTraderAccountId=ACCOUNT_ID)),
    )
    await server.start()
    session = _session(server, backoff_base_secs=0.05)

    async def subscribe() -> None:
        await session.request(
            oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=ACCOUNT_ID, symbolId=[1]),
        )

    async def succeed() -> None:
        pass

    try:
        assert session.failed_restores == frozenset()
        session.add_restore(("spots", 1), subscribe)
        session.add_restore("ok", succeed)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        assert session.failed_restores == frozenset({("spots", 1)})

        await server.drop_connections()
        await wait_until(lambda: server.connection_count >= 2)
        await session.wait_ready(timeout_secs=3.0)
        assert session.failed_restores == frozenset()
    finally:
        await session.stop()
        await server.stop()


async def test_remove_restore_also_clears_it_from_failed_restores() -> None:
    server = _authenticating_server()
    server.on(
        oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="ENTITY_NOT_FOUND", description="gone"),
    )
    await server.start()
    session = _session(server)

    async def subscribe() -> None:
        await session.request(
            oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=ACCOUNT_ID, symbolId=[1]),
        )

    try:
        session.add_restore(("spots", 1), subscribe)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        assert session.failed_restores == frozenset({("spots", 1)})

        session.remove_restore(("spots", 1))
        assert session.failed_restores == frozenset()
    finally:
        await session.stop()
        await server.stop()


async def test_a_loss_after_the_last_restore_fails_the_bring_up() -> None:
    # The restore's own request succeeded, so only the loss check after the loop stands between
    # this bring-up and a session marked ready on a dead socket.
    server = _authenticating_server()
    server.on(
        oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
        lambda _r: oa.ProtoOASubscribeSpotsRes(ctidTraderAccountId=ACCOUNT_ID),
    )
    await server.start()
    logger = RecordingLogger()
    session = _session(server, logger=logger, backoff_base_secs=0.05)
    attempts: list[int] = []

    async def subscribe_then_lose() -> None:
        attempts.append(1)
        await session.request(
            oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=ACCOUNT_ID, symbolId=[1]),
        )
        if len(attempts) == 1:
            await server.drop_connections()
            await wait_until(session._lost.is_set, description="loss reaching the session")

    try:
        session.add_restore(("spots", 1), subscribe_then_lose)
        await session.start()
        await wait_until(lambda: server.connection_count >= 2, description="reconnect")
        await session.wait_ready(timeout_secs=3.0)

        assert len(attempts) == 2
        failures = [m for _level, m in logger.lines if "bring-up failed (attempt 1)" in m]
        assert failures, f"bring-up failure was never logged; lines were {logger.lines}"
        assert "lost during bring-up" in failures[0]
        assert not any("soon after becoming ready" in m for _level, m in logger.lines)
    finally:
        await session.stop()
        await server.stop()


async def test_bring_up_failures_escalate_to_error_at_the_threshold() -> None:
    server = FakeCTraderServer()
    server.on(
        oa_model.PROTO_OA_APPLICATION_AUTH_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="INVALID_REQUEST"),
    )
    await server.start()
    logger = RecordingLogger()
    session = _session(server, logger=logger, backoff_base_secs=0.05, failure_threshold=2)
    try:
        await session.start()
        await wait_until(lambda: bool(logger.errors()), description="an ERROR line")

        failures = [(level, m) for level, m in logger.lines if "bring-up failed" in m]
        assert failures[0][0] == "warning"
        assert "(attempt 1)" in failures[0][1]
        assert failures[1][0] == "error"
        assert "(attempt 2)" in failures[1][1]
    finally:
        await session.stop()
        await server.stop()


async def test_stop_during_bring_up_returns_promptly() -> None:
    # No handler: the application auth request is never answered.
    server = FakeCTraderServer()
    await server.start()
    session = _session(server, request_timeout_secs=30.0)
    try:
        await session.start()
        await wait_until(
            lambda: session.state is SessionState.AUTHENTICATING and bool(server.received),
            description="authentication in flight",
        )

        # Timed directly rather than via `wait_for(stop(), ...)`, though that would now work too.
        loop = asyncio.get_running_loop()
        started = loop.time()
        await session.stop()
        assert loop.time() - started < 1.0

        assert session.state is SessionState.STOPPED
        owned = ("CTraderSession.", "CTraderConnection.")
        leftovers = [
            task
            for task in asyncio.all_tasks()
            if not task.done() and task.get_coro().__qualname__.startswith(owned)
        ]
        assert leftovers == []
    finally:
        await session.stop()
        await server.stop()


async def test_stop_can_be_cancelled_by_its_own_caller() -> None:
    # If `stop()` swallowed cancellation aimed at it, `wait_for(session.stop(), ...)` could never
    # time out and a cancelled caller would see a normal return instead of `CancelledError`.
    server = _authenticating_server()
    await server.start()
    session = _session(server)
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        await asyncio.wait_for(session.stop(), timeout=1.0)
        assert session.state is SessionState.STOPPED

        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        stop_task = asyncio.create_task(session.stop())
        # Let `stop()` actually start and reach its own first await, so the cancellation below
        # arrives while it is suspended there - not before the coroutine has run at all.
        await asyncio.sleep(0)
        stop_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stop_task

        # A cancelled `stop()` must still finish tearing the session down: otherwise it is left
        # `READY` with a live, orphaned connection that nothing will ever close.
        assert session.state is SessionState.STOPPED
        assert session._connection.is_connected is False

        await session.stop()
    finally:
        await session.stop()
        await server.stop()


async def test_stop_survives_repeated_cancellation(monkeypatch: pytest.MonkeyPatch) -> None:
    # A heartbeat task slow to react to cancellation holds `close()` inside its task wait, so
    # the second cancellation lands there deterministically.
    original_heartbeat_loop = CTraderConnection._heartbeat_loop
    heartbeat_cancelled = asyncio.Event()

    async def slow_to_cancel_heartbeat_loop(self: CTraderConnection) -> None:
        try:
            await original_heartbeat_loop(self)
        except asyncio.CancelledError:
            heartbeat_cancelled.set()
            await asyncio.sleep(0.2)
            raise

    monkeypatch.setattr(CTraderConnection, "_heartbeat_loop", slow_to_cancel_heartbeat_loop)

    server = _authenticating_server()
    await server.start()
    session = _session(server)
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        heartbeat_task = session._connection._heartbeat_task
        assert heartbeat_task is not None

        stop_task = asyncio.create_task(session.stop())
        await asyncio.sleep(0)
        stop_task.cancel()
        await wait_until(heartbeat_cancelled.is_set, description="stop inside close()")
        assert not stop_task.done()
        stop_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stop_task

        assert session.state is SessionState.STOPPED
        assert session.is_ready is False
        assert session._connection.is_connected is False
        with pytest.raises(CTraderConnectionError):
            await session.request(oa.ProtoOATraderReq(ctidTraderAccountId=ACCOUNT_ID))

        await session.stop()
        await asyncio.wait({heartbeat_task})
    finally:
        await session.stop()
        await server.stop()


async def test_a_loss_while_stop_waits_keeps_the_session_stopped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A heartbeat task slow to react to cancellation keeps `stop()` waiting while the venue
    # drops the connection.
    original_heartbeat_loop = CTraderConnection._heartbeat_loop
    heartbeat_cancelled = asyncio.Event()

    async def slow_to_cancel_heartbeat_loop(self: CTraderConnection) -> None:
        try:
            await original_heartbeat_loop(self)
        except asyncio.CancelledError:
            heartbeat_cancelled.set()
            await asyncio.sleep(0.3)
            raise

    monkeypatch.setattr(CTraderConnection, "_heartbeat_loop", slow_to_cancel_heartbeat_loop)

    server = _authenticating_server()
    await server.start()
    session = _session(server)
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        stop_task = asyncio.create_task(session.stop())
        await wait_until(heartbeat_cancelled.is_set, description="stop waiting on its tasks")
        await server.drop_connections()
        await asyncio.sleep(0.05)
        assert not stop_task.done(), "the loss must land while stop() still waits"
        await stop_task

        assert session.state is SessionState.STOPPED
        assert session.is_ready is False
        assert session._connection.is_connected is False
    finally:
        await session.stop()
        await server.stop()


async def test_start_during_stop_ends_healthy() -> None:
    # A restore slow to honour cancellation keeps `stop()` waiting while `start()` is called,
    # as when Nautilus schedules disconnect and connect as separate tasks.
    server = _authenticating_server()
    server.on(
        oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
        lambda _r: oa.ProtoOASubscribeSpotsRes(ctidTraderAccountId=ACCOUNT_ID),
    )
    await server.start()
    # A fast limiter, so the new bring-up finishes well inside the restore's cancellation delay.
    limiter = RateLimiter({BUCKET_DEFAULT: 1000.0, BUCKET_HISTORICAL: 1000.0})
    session = _session(server, backoff_base_secs=0.01, rate_limiter=limiter)
    block_restore = False
    restore_entered = asyncio.Event()

    async def slow_to_cancel_restore() -> None:
        if not block_restore:
            return
        restore_entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.sleep(0.3)
            raise

    def subscribe_request() -> oa.ProtoOASubscribeSpotsReq:
        return oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=ACCOUNT_ID, symbolId=[1])

    def owned_tasks(name: str) -> list[asyncio.Task]:
        qualname = f"CTraderConnection.{name}"
        return [
            task
            for task in asyncio.all_tasks()
            if not task.done() and task.get_coro().__qualname__.endswith(qualname)
        ]

    try:
        session.add_restore("slow", slow_to_cancel_restore)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        # Hold the next bring-up inside the restore.
        block_restore = True
        await server.drop_connections()
        await wait_until(restore_entered.is_set, description="bring-up inside the restore")
        block_restore = False

        stop_task = asyncio.create_task(session.stop())
        await asyncio.sleep(0)
        assert not stop_task.done()
        await session.start()
        await session.wait_ready(timeout_secs=3.0)

        assert session.is_ready
        assert session._connection.is_connected
        await session.request(subscribe_request(), timeout_secs=2.0)

        await stop_task
        assert session.is_ready
        assert session._connection.is_connected
        await session.request(subscribe_request(), timeout_secs=2.0)
        assert len(owned_tasks("_heartbeat_loop")) == 1
        assert len(owned_tasks("_read_loop")) == 1
    finally:
        await session.stop()
        await server.stop()


async def test_stop_returns_promptly_after_the_server_drops_the_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Reproduces a probe where the server dropped the connection and `stop()` landed while the
    # supervisor was inside its own `close()` call: the cancellation-swallowing bug there made
    # `stop()` wait out the whole backoff instead of returning promptly. The heartbeat task is
    # made slow to react to its own cancellation, to hold the supervisor inside that window for
    # long enough that `stop()` reliably lands in it.
    original_heartbeat_loop = CTraderConnection._heartbeat_loop
    heartbeat_cancelled = asyncio.Event()

    async def slow_to_cancel_heartbeat_loop(self: CTraderConnection) -> None:
        try:
            await original_heartbeat_loop(self)
        except asyncio.CancelledError:
            heartbeat_cancelled.set()
            # With the fixed `stop()`, this sleep is what `stop()`'s own `close()` interrupts,
            # not something `stop()` waits out.
            await asyncio.sleep(0.2)
            raise

    monkeypatch.setattr(CTraderConnection, "_heartbeat_loop", slow_to_cancel_heartbeat_loop)

    server = _authenticating_server()
    await server.start()
    session = _session(server, backoff_base_secs=5.0)
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        await server.drop_connections()
        await wait_until(
            lambda: heartbeat_cancelled.is_set(),
            description="supervisor inside close()",
        )

        loop = asyncio.get_running_loop()
        started = loop.time()
        await session.stop()
        assert loop.time() - started < 1.0
    finally:
        await session.stop()
        await server.stop()
