"""The session: authentication order, subscription restore, and reconnect."""

import asyncio
import struct

import pytest
from nautilus_trader.common.component import Logger

from nautilus_ctrader.common.errors import (
    CTraderAuthError,
    CTraderConnectionError,
    CTraderProtocolError,
)
from nautilus_ctrader.common.session import CTraderSession, SessionState
from nautilus_ctrader.constants import LENGTH_PREFIX_FORMAT, MAX_FRAME_BYTES
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


def _session(server: FakeCTraderServer, **kwargs) -> CTraderSession:
    return CTraderSession(
        host=server.host,
        port=server.port,
        client_id="client-id",
        client_secret="client-secret",
        account_id=ACCOUNT_ID,
        access_token="access-token",
        logger=Logger("test"),
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
    session = _session(server, backoff_base_secs=0.5)
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
        await wait_until(
            lambda: any(isinstance(m, oa.ProtoOASubscribeSpotsReq) for m in server.received),
            description="restore request reaching the server",
        )
        await server.push_raw(struct.pack(LENGTH_PREFIX_FORMAT, MAX_FRAME_BYTES + 1))
        await pending

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
            self._lost.set()

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
        await wait_until(
            lambda: any(isinstance(m, oa.ProtoOASubscribeSpotsReq) for m in server.received),
            description="restore request reaching the server",
        )
        await server.push_raw(struct.pack(LENGTH_PREFIX_FORMAT, MAX_FRAME_BYTES + 1))
        await pending

    try:
        session.add_restore("corrupted", restore)
        await session.start()

        await wait_until(lambda: session.last_error is not None, description="bring-up failure")
        failure = session.last_error
        assert isinstance(failure, CTraderConnectionError)
        assert isinstance(failure.__cause__, CTraderProtocolError)

        await session.wait_ready(timeout_secs=5.0)
    finally:
        await session.stop()
        await server.stop()
