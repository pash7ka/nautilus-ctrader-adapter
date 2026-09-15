"""Token refresh, and recovery when the venue drops authentication on a live socket."""

import asyncio
import struct
import time

import pytest
from nautilus_trader.common.component import Logger

from nautilus_ctrader.common.errors import CTraderAuthError
from nautilus_ctrader.common.session import CTraderSession, SessionState
from nautilus_ctrader.constants import LENGTH_PREFIX_FORMAT, MAX_FRAME_BYTES
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from tests.fake_server import FakeCTraderServer
from tests.polling import wait_until
from tests.recording_logger import RecordingLogger

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
        tls=False,
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

        with pytest.raises(CTraderAuthError, match="token refresh rejected"):
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
        tls=False,
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


@pytest.mark.parametrize("error_code", ["OA_AUTH_TOKEN_EXPIRED", "CH_ACCESS_TOKEN_INVALID"])
async def test_a_token_rejection_refreshes_once_and_retries(error_code: str) -> None:
    server = _server()
    server.on(
        oa_model.PROTO_OA_ACCOUNT_AUTH_REQ,
        lambda request: (
            oa.ProtoOAAccountAuthRes(ctidTraderAccountId=ACCOUNT_ID)
            if request.accessToken == "new-access"
            else oa.ProtoOAErrorRes(errorCode=error_code, description="expired")
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


async def test_a_late_refresh_reply_never_reaches_the_event_handler() -> None:
    # A refresh reply that arrives after its request timed out carries a fresh token pair and
    # falls through to `_on_event` with no pending future left to claim it; it must be dropped
    # there rather than forwarded to the application.
    server = _server()
    await server.start()
    session = _session(server)
    seen: list[object] = []
    try:
        session.set_event_handler(seen.append)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        await server.push(
            oa.ProtoOARefreshTokenRes(
                accessToken="late-access",
                tokenType="bearer",
                expiresIn=2_592_000,
                refreshToken="late-refresh",
            ),
        )
        await server.push(
            oa.ProtoOASpotEvent(ctidTraderAccountId=ACCOUNT_ID, symbolId=1, bid=1, ask=2),
        )
        await wait_until(
            lambda: any(isinstance(m, oa.ProtoOASpotEvent) for m in seen),
            description="spot event reaching the handler",
        )

        assert not any(isinstance(m, oa.ProtoOARefreshTokenRes) for m in seen)
    finally:
        await session.stop()
        await server.stop()


async def test_a_non_token_rejection_never_refreshes() -> None:
    # A new token cannot fix an unknown account; refreshing anyway would rotate tokens on
    # every retry.
    server = _server()
    server.on(
        oa_model.PROTO_OA_ACCOUNT_AUTH_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="CH_CTID_TRADER_ACCOUNT_NOT_FOUND"),
    )
    await server.start()
    session = _session(server, backoff_base_secs=0.05)
    try:
        await session.start()
        await wait_until(
            lambda: server.connection_count >= 3,
            description="several rejected attempts",
        )

        assert _refreshes(server) == []
        assert isinstance(session.last_error, CTraderAuthError)
        assert session.state is not SessionState.READY
    finally:
        await session.stop()
        await server.stop()


async def test_a_persistent_token_rejection_refreshes_at_most_once() -> None:
    # A venue that keeps reporting a revoked token as expired must not make every retry
    # rotate the tokens.
    server = _server()
    server.on(
        oa_model.PROTO_OA_ACCOUNT_AUTH_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="CH_ACCESS_TOKEN_INVALID", description="revoked"),
    )
    await server.start()
    session = _session(server, backoff_base_secs=0.05)
    try:
        await session.start()
        await wait_until(
            lambda: server.connection_count >= 3,
            description="several rejected attempts",
        )

        assert len(_refreshes(server)) == 1
        assert isinstance(session.last_error, CTraderAuthError)
    finally:
        await session.stop()
        await server.stop()


async def test_a_raising_persistence_callback_is_logged_and_the_session_carries_on() -> None:
    # The refresh succeeded, so the session must keep working on the new tokens - but the
    # operator must hear that they were not saved, without the tokens reaching the log.
    server = _server()
    await server.start()
    logger = RecordingLogger()

    def explode(_access: str, _refresh: str, _expires_at: float) -> None:
        raise RuntimeError("storage unavailable")

    session = CTraderSession(
        host=server.host,
        port=server.port,
        client_id="client-id",
        client_secret="client-secret",
        account_id=ACCOUNT_ID,
        access_token="old-access",
        refresh_token="old-refresh",
        expires_at_secs=time.time() + 1.0,
        on_tokens_refreshed=explode,
        logger=logger,
        tls=False,
    )
    try:
        await session.start()
        await wait_until(lambda: server.connection_count >= 2, description="re-authentication")
        await session.wait_ready(timeout_secs=3.0)

        assert any("not saved" in line for line in logger.errors())
        assert _account_auths(server)[-1].accessToken == "new-access"
        assert not any("new-access" in m or "new-refresh" in m for _, m in logger.lines)
    finally:
        await session.stop()
        await server.stop()


async def test_a_proactive_refresh_that_fails_unexpectedly_is_logged() -> None:
    # A malformed frame rejects the pending refresh with a protocol error - neither an auth
    # nor a connection error - and that must still be logged rather than end the loop silently.
    server = _server()
    server.on(oa_model.PROTO_OA_REFRESH_TOKEN_REQ, lambda _r: None)
    await server.start()
    logger = RecordingLogger()
    session = CTraderSession(
        host=server.host,
        port=server.port,
        client_id="client-id",
        client_secret="client-secret",
        account_id=ACCOUNT_ID,
        access_token="old-access",
        refresh_token="old-refresh",
        expires_at_secs=time.time() + 1.0,
        logger=logger,
        tls=False,
        backoff_base_secs=0.05,
    )
    try:
        await session.start()
        await wait_until(
            lambda: bool(_refreshes(server)),
            description="refresh request reaching the server",
        )
        await server.push_raw(struct.pack(LENGTH_PREFIX_FORMAT, MAX_FRAME_BYTES + 1))
        await wait_until(
            lambda: any("Proactive token refresh failed" in line for line in logger.errors()),
            description="refresh failure being logged",
        )
    finally:
        await session.stop()
        await server.stop()


async def test_a_proactive_refresh_that_will_be_retried_before_expiry_is_a_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The loop's own retry will land before the token expires, so this is a WARNING, not the
    # ERROR that means a human must intervene.
    monkeypatch.setattr("nautilus_ctrader.common.session.MIN_TOKEN_REFRESH_INTERVAL_SECS", 0.2)
    server = _server()
    server.on(oa_model.PROTO_OA_REFRESH_TOKEN_REQ, lambda _r: None)
    await server.start()
    logger = RecordingLogger()
    session = CTraderSession(
        host=server.host,
        port=server.port,
        client_id="client-id",
        client_secret="client-secret",
        account_id=ACCOUNT_ID,
        access_token="old-access",
        refresh_token="old-refresh",
        expires_at_secs=time.time() + 10.0,
        logger=logger,
        tls=False,
        backoff_base_secs=0.05,
    )
    try:
        await session.start()
        await wait_until(
            lambda: bool(_refreshes(server)),
            description="refresh request reaching the server",
        )
        await server.push_raw(struct.pack(LENGTH_PREFIX_FORMAT, MAX_FRAME_BYTES + 1))
        await wait_until(
            lambda: any(
                level == "warning" and "Proactive token refresh failed" in message
                for level, message in logger.lines
            ),
            description="refresh failure being logged as a warning",
        )

        assert not any("Proactive token refresh failed" in line for line in logger.errors())
    finally:
        await session.stop()
        await server.stop()


async def test_a_proactive_refresh_that_times_out_is_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A transient failure must not end proactive refresh until the next reconnect, or the
    # token is left to expire.
    monkeypatch.setattr("nautilus_ctrader.common.session.MIN_TOKEN_REFRESH_INTERVAL_SECS", 0.2)
    server = _server()
    server.on(oa_model.PROTO_OA_REFRESH_TOKEN_REQ, lambda _r: None)
    await server.start()
    session = _session(server, expires_at_secs=time.time() + 1.0, request_timeout_secs=0.3)
    try:
        await session.start()
        await wait_until(
            lambda: len(_refreshes(server)) >= 2,
            description="a second refresh attempt",
        )
    finally:
        await session.stop()
        await server.stop()


async def test_a_proactive_reauth_is_not_treated_as_a_failed_session() -> None:
    # A proactive refresh forces re-authentication with the new token by design; that must be
    # recognised as recovery, not counted as a session that failed soon after becoming ready.
    server = _server()
    await server.start()
    logger = RecordingLogger()
    session = CTraderSession(
        host=server.host,
        port=server.port,
        client_id="client-id",
        client_secret="client-secret",
        account_id=ACCOUNT_ID,
        access_token="old-access",
        refresh_token="old-refresh",
        expires_at_secs=time.time() + 1.0,
        logger=logger,
        tls=False,
        backoff_base_secs=5.0,
    )
    loop = asyncio.get_running_loop()
    try:
        started = loop.time()
        await session.start()
        await wait_until(lambda: server.connection_count >= 2, description="re-authentication")
        await session.wait_ready(timeout_secs=2.0)
        elapsed = loop.time() - started

        assert elapsed < 2.0, f"re-authentication took {elapsed:.2f}s, a backoff appears to apply"
        assert not any("soon after becoming ready" in message for _level, message in logger.lines)
        assert ("info", "Re-authenticating with the refreshed token") in logger.lines
    finally:
        await session.stop()
        await server.stop()


async def test_an_auth_loss_event_stops_the_session_accepting_requests() -> None:
    # Once the venue drops our authentication, the live socket must not keep accepting work.
    server = _server()
    await server.start()
    session = _session(server, backoff_base_secs=0.05)
    states: list[SessionState] = []
    # The handler runs right after the session reacts to the event, before any teardown.
    session.set_event_handler(lambda _payload: states.append(session.state))
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        await server.push(oa.ProtoOAAccountDisconnectEvent(ctidTraderAccountId=ACCOUNT_ID))
        await wait_until(lambda: bool(states), description="event reaching the handler")

        assert states[0] is not SessionState.READY
    finally:
        await session.stop()
        await server.stop()


async def test_an_auth_loss_during_bring_up_is_reported_without_a_stale_error() -> None:
    server = _server()
    await server.start()
    logger = RecordingLogger()
    session = CTraderSession(
        host=server.host,
        port=server.port,
        client_id="client-id",
        client_secret="client-secret",
        account_id=ACCOUNT_ID,
        access_token="old-access",
        logger=logger,
        tls=False,
        backoff_base_secs=0.05,
    )
    seen: list[object] = []
    session.set_event_handler(seen.append)
    attempts: list[int] = []

    async def restore() -> None:
        attempts.append(1)
        if len(attempts) > 1:
            return
        await server.push(oa.ProtoOAAccountDisconnectEvent(ctidTraderAccountId=ACCOUNT_ID))
        await wait_until(lambda: bool(seen), description="auth loss reaching the session")

    try:
        session.add_restore("dropped", restore)
        await session.start()
        await session.wait_ready(timeout_secs=3.0)

        failures = [m for level, m in logger.lines if level == "warning" and "bring-up" in m]
        assert failures, f"bring-up failure was never logged; lines were {logger.lines}"
        assert "connection or authentication lost during bring-up" in failures[0]
        assert "None" not in failures[0]
    finally:
        await session.stop()
        await server.stop()


async def test_an_invalidation_naming_no_account_is_treated_as_ours() -> None:
    server = _server()
    await server.start()
    session = _session(server, backoff_base_secs=0.05)
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        await server.push(oa.ProtoOAAccountsTokenInvalidatedEvent(reason="recalled"))
        await wait_until(lambda: server.connection_count >= 2, description="re-authentication")
        await session.wait_ready(timeout_secs=3.0)

        assert _refreshes(server) == []
    finally:
        await session.stop()
        await server.stop()
