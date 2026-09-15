"""Token refresh, and recovery when the venue drops authentication on a live socket."""

import asyncio
import time

import pytest
from nautilus_trader.common.component import Logger

from nautilus_ctrader.common.errors import CTraderAuthError
from nautilus_ctrader.common.session import CTraderSession, SessionState
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from tests.fake_server import FakeCTraderServer
from tests.polling import wait_until

ACCOUNT_ID = 1234567
OTHER_ACCOUNT_ID = 7654321


def _refresh_response(_request: object) -> oa.ProtoOARefreshTokenRes:
    return oa.ProtoOARefreshTokenRes(
        accessToken="new-access",
        tokenType="bearer",
        expiresIn=2_592_000,
        refreshToken="new-refresh",
    )


def _server() -> FakeCTraderServer:
    server = FakeCTraderServer()
    server.on(
        oa_model.PROTO_OA_APPLICATION_AUTH_REQ,
        lambda _r: oa.ProtoOAApplicationAuthRes(),
    )
    server.on(
        oa_model.PROTO_OA_ACCOUNT_AUTH_REQ,
        lambda _r: oa.ProtoOAAccountAuthRes(ctidTraderAccountId=ACCOUNT_ID),
    )
    server.on(oa_model.PROTO_OA_REFRESH_TOKEN_REQ, _refresh_response)
    return server


def _session(server: FakeCTraderServer, **kwargs) -> CTraderSession:
    return CTraderSession(
        host=server.host,
        port=server.port,
        client_id="client-id",
        client_secret="client-secret",
        account_id=ACCOUNT_ID,
        access_token="old-access",
        refresh_token="old-refresh",
        logger=Logger("test"),
        ssl_context=None,
        **kwargs,
    )


def _account_auths(server: FakeCTraderServer) -> list[oa.ProtoOAAccountAuthReq]:
    return [m for m in server.received if isinstance(m, oa.ProtoOAAccountAuthReq)]


def _refreshes(server: FakeCTraderServer) -> list[oa.ProtoOARefreshTokenReq]:
    return [m for m in server.received if isinstance(m, oa.ProtoOARefreshTokenReq)]


async def test_refresh_replaces_both_tokens_and_fires_the_callback() -> None:
    server = _server()
    await server.start()
    persisted: list[tuple[str, str, float]] = []
    session = _session(
        server,
        on_tokens_refreshed=lambda a, r, e: persisted.append((a, r, e)),
    )
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        await session.refresh_tokens()

        assert len(persisted) == 1
        access, refresh, expires_at = persisted[0]
        assert access == "new-access"
        assert refresh == "new-refresh"
        assert expires_at > time.time()
    finally:
        await session.stop()
        await server.stop()


async def test_the_new_access_token_is_used_for_the_next_authentication() -> None:
    server = _server()
    await server.start()
    session = _session(server, backoff_base_secs=0.05)
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        await session.refresh_tokens()

        await server.drop_connections()
        await wait_until(lambda: server.connection_count >= 2)
        await session.wait_ready(timeout_secs=3.0)

        assert _account_auths(server)[-1].accessToken == "new-access"
    finally:
        await session.stop()
        await server.stop()


async def test_a_rejected_refresh_raises_an_auth_error() -> None:
    server = _server()
    server.on(
        oa_model.PROTO_OA_REFRESH_TOKEN_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="INVALID_REQUEST", description="expired"),
    )
    await server.start()
    session = _session(server)
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        with pytest.raises(CTraderAuthError, match="refresh"):
            await session.refresh_tokens()
    finally:
        await session.stop()
        await server.stop()


async def test_refresh_without_a_refresh_token_raises() -> None:
    server = _server()
    await server.start()
    session = CTraderSession(
        host=server.host,
        port=server.port,
        client_id="client-id",
        client_secret="client-secret",
        account_id=ACCOUNT_ID,
        access_token="only-access",
        logger=Logger("test"),
        ssl_context=None,
    )
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        with pytest.raises(CTraderAuthError, match="no refresh token"):
            await session.refresh_tokens()
    finally:
        await session.stop()
        await server.stop()


async def test_an_imminent_expiry_refreshes_and_reauthenticates_with_the_new_token() -> None:
    server = _server()
    await server.start()
    # An expiry already inside the refresh margin must refresh as soon as the session is up.
    session = _session(server, expires_at_secs=time.time() + 1.0)
    try:
        await session.start()
        await wait_until(lambda: server.connection_count >= 2)
        await session.wait_ready(timeout_secs=3.0)

        assert len(_refreshes(server)) == 1
        assert [m.accessToken for m in _account_auths(server)] == ["old-access", "new-access"]
    finally:
        await session.stop()
        await server.stop()


async def test_a_token_invalidation_reauthenticates_without_refreshing() -> None:
    # The schema lists "token was refreshed" among this event's causes, so answering it with a
    # refresh could loop forever against a live venue. It must re-authenticate with the token
    # already held, and nothing more.
    server = _server()
    await server.start()
    session = _session(server, backoff_base_secs=0.05)
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        await server.push(
            oa.ProtoOAAccountsTokenInvalidatedEvent(
                ctidTraderAccountIds=[ACCOUNT_ID],
                reason="token was refreshed",
            ),
        )
        await wait_until(lambda: server.connection_count >= 2)
        await session.wait_ready(timeout_secs=3.0)

        assert _refreshes(server) == []
        assert [m.accessToken for m in _account_auths(server)] == ["old-access", "old-access"]
    finally:
        await session.stop()
        await server.stop()


async def test_an_account_disconnect_reauthenticates_and_replays_restores() -> None:
    server = _server()
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

        await server.push(oa.ProtoOAAccountDisconnectEvent(ctidTraderAccountId=ACCOUNT_ID))
        await wait_until(lambda: server.connection_count >= 2)
        await session.wait_ready(timeout_secs=3.0)

        assert [type(m).__name__ for m in server.received] == [
            "ProtoOAApplicationAuthReq",
            "ProtoOAAccountAuthReq",
            "ProtoOASubscribeSpotsReq",
        ]
    finally:
        await session.stop()
        await server.stop()


async def test_a_client_disconnect_triggers_a_reconnect() -> None:
    server = _server()
    await server.start()
    session = _session(server, backoff_base_secs=0.05)
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        await server.push(oa.ProtoOAClientDisconnectEvent(reason="access blocked"))
        await wait_until(lambda: server.connection_count >= 2)
        await session.wait_ready(timeout_secs=3.0)

        assert len(_account_auths(server)) == 2
    finally:
        await session.stop()
        await server.stop()


async def test_events_for_another_account_are_ignored() -> None:
    server = _server()
    await server.start()
    session = _session(server)
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        await server.push(oa.ProtoOAAccountDisconnectEvent(ctidTraderAccountId=OTHER_ACCOUNT_ID))
        await server.push(
            oa.ProtoOAAccountsTokenInvalidatedEvent(ctidTraderAccountIds=[OTHER_ACCOUNT_ID]),
        )
        # Nothing is expected to happen, so there is no condition to poll for.
        await asyncio.sleep(0.2)

        assert server.connection_count == 1
        assert session.state is SessionState.READY
    finally:
        await session.stop()
        await server.stop()


async def test_a_rejected_account_auth_refreshes_once_and_retries() -> None:
    server = _server()
    server.on(
        oa_model.PROTO_OA_ACCOUNT_AUTH_REQ,
        lambda request: (
            oa.ProtoOAAccountAuthRes(ctidTraderAccountId=ACCOUNT_ID)
            if request.accessToken == "new-access"
            else oa.ProtoOAErrorRes(errorCode="INVALID_REQUEST", description="expired")
        ),
    )
    await server.start()
    session = _session(server)
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        assert len(_refreshes(server)) == 1
        assert [m.accessToken for m in _account_auths(server)] == ["old-access", "new-access"]
        assert server.connection_count == 1
    finally:
        await session.stop()
        await server.stop()


async def test_a_second_account_auth_rejection_is_an_auth_failure() -> None:
    server = _server()
    server.on(
        oa_model.PROTO_OA_ACCOUNT_AUTH_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="INVALID_REQUEST", description="revoked"),
    )
    await server.start()
    session = _session(server, backoff_base_secs=0.05)
    try:
        await session.start()
        await wait_until(lambda: isinstance(session.last_error, CTraderAuthError))

        assert "after refresh" in str(session.last_error)
        assert session.state is not SessionState.READY
    finally:
        await session.stop()
        await server.stop()
