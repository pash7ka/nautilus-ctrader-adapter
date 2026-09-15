"""The session: authentication order, subscription restore, and reconnect."""

import asyncio

import pytest
from nautilus_trader.common.component import Logger

from nautilus_ctrader.common.errors import CTraderAuthError, CTraderConnectionError
from nautilus_ctrader.common.session import CTraderSession, SessionState
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from tests.fake_server import FakeCTraderServer
from tests.polling import wait_until

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
        ssl_context=None,
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


async def test_a_restore_that_loses_the_connection_is_a_bring_up_failure() -> None:
    # Containing this would mark a dead socket ready; it must reconnect instead.
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
        await asyncio.sleep(0.1)
        assert server.connection_count == 1
    finally:
        await session.stop()
        await server.stop()
