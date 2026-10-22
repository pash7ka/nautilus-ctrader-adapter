"""Token refresh, and recovery when the venue drops authentication on a live socket."""

import asyncio
import functools
import struct
import time
from collections.abc import Callable

import pytest

from nautilus_ctrader.common.errors import (
    CTraderAuthError,
    CTraderConnectionError,
    CTraderQueueTimeoutError,
    CTraderRequestError,
    CTraderTimeoutError,
)
from nautilus_ctrader.common.session import CTraderSession, SessionState
from nautilus_ctrader.constants import LENGTH_PREFIX_FORMAT, MAX_FRAME_BYTES
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from tests.account_venue import HeldReplies
from tests.fake_server import FakeCTraderServer
from tests.polling import wait_until
from tests.recording_logger import RecordingLogger
from tests.session_venue import ACCOUNT_ID, authenticating_server, session_for

OTHER_ACCOUNT_ID = 7654321


def _refresh_response(_request: object) -> oa.ProtoOARefreshTokenRes:
    return oa.ProtoOARefreshTokenRes(
        accessToken="new-access",
        tokenType="bearer",
        expiresIn=2_592_000,
        refreshToken="new-refresh",
    )


def _server() -> FakeCTraderServer:
    server = authenticating_server()
    server.on(oa_model.PROTO_OA_REFRESH_TOKEN_REQ, _refresh_response)
    return server


# A session that holds a refresh token, unlike the shared builder's default.
_session = functools.partial(session_for, access_token="old-access", refresh_token="old-refresh")


def _account_auths(server: FakeCTraderServer) -> list[oa.ProtoOAAccountAuthReq]:
    return [m for m in server.received if isinstance(m, oa.ProtoOAAccountAuthReq)]


def _refreshes(server: FakeCTraderServer) -> list[oa.ProtoOARefreshTokenReq]:
    return [m for m in server.received if isinstance(m, oa.ProtoOARefreshTokenReq)]


def _due_after_ready(monkeypatch: pytest.MonkeyPatch) -> float:
    """An expiry whose proactive refresh falls due about a second after bring-up.

    A refresh already due at bring-up runs before account auth instead of in the loop.
    """
    monkeypatch.setattr("nautilus_ctrader.common.session.TOKEN_REFRESH_MARGIN_SECS", 1.0)
    return time.time() + 2.0


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
    session = _session(server, access_token="only-access", refresh_token=None)
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        with pytest.raises(CTraderAuthError, match="no refresh token"):
            await session.refresh_tokens()
    finally:
        await session.stop()
        await server.stop()


async def test_a_refresh_due_at_bring_up_runs_before_account_auth() -> None:
    # Refreshed once ready, the venue would end the session and fail the requests in flight.
    server = _server()
    await server.start()
    session = _session(server, expires_at_secs=time.time() + 1.0)
    try:
        await session.start()
        await session.wait_ready(timeout_secs=3.0)
        # Long enough for a refresh loop that wrongly fires again to reach the server.
        await asyncio.sleep(0.3)

        assert len(_refreshes(server)) == 1
        assert [m.accessToken for m in _account_auths(server)] == ["new-access"]
        assert server.connection_count == 1
    finally:
        await session.stop()
        await server.stop()


async def test_a_rejected_refresh_at_bring_up_authenticates_with_the_current_token() -> None:
    server = _server()
    server.on(
        oa_model.PROTO_OA_REFRESH_TOKEN_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="ACCESS_DENIED"),
    )
    await server.start()
    logger = RecordingLogger()
    session = _session(server, logger=logger, expires_at_secs=time.time() + 1.0)
    try:
        await session.start()
        await session.wait_ready(timeout_secs=3.0)

        assert [m.accessToken for m in _account_auths(server)] == ["old-access"]
        assert any("Token refresh rejected: ACCESS_DENIED" in m for m in logger.errors())
    finally:
        await session.stop()
        await server.stop()


async def test_an_unanswered_refresh_at_bring_up_authenticates_with_the_current_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("nautilus_ctrader.common.session.LATE_REFRESH_WAIT_SECS", 0.1)
    server = _server()
    server.on(oa_model.PROTO_OA_REFRESH_TOKEN_REQ, lambda _r: None)
    await server.start()
    logger = RecordingLogger()
    session = _session(
        server,
        logger=logger,
        expires_at_secs=time.time() + 1.0,
        request_timeout_secs=0.2,
    )
    try:
        await session.start()
        await session.wait_ready(timeout_secs=3.0)

        assert [m.accessToken for m in _account_auths(server)] == ["old-access"]
        assert any("got no reply" in m for m in logger.warnings())
    finally:
        await session.stop()
        await server.stop()


async def test_account_events_after_a_refresh_before_account_auth_are_ignored() -> None:
    # They name the token the refresh just replaced; no account is authenticated to end yet.
    server = _server()
    pushes: list[asyncio.Task] = []

    async def invalidate_then_answer(request: oa.ProtoOARefreshTokenReq, msg_id: str) -> None:
        await server.push(
            oa.ProtoOAAccountsTokenInvalidatedEvent(ctidTraderAccountIds=[ACCOUNT_ID])
        )
        await server.push(oa.ProtoOAAccountDisconnectEvent(ctidTraderAccountId=ACCOUNT_ID))
        await server.push(_refresh_response(request), client_msg_id=msg_id)

    def refresh(request: oa.ProtoOARefreshTokenReq) -> None:
        msg_id = server.received_client_msg_ids[-1]
        pushes.append(asyncio.create_task(invalidate_then_answer(request, msg_id)))

    server.on(oa_model.PROTO_OA_REFRESH_TOKEN_REQ, refresh)
    await server.start()
    session = _session(server, expires_at_secs=time.time() + 1.0)
    try:
        await session.start()
        await session.wait_ready(timeout_secs=3.0)

        assert [m.accessToken for m in _account_auths(server)] == ["new-access"]
        assert server.connection_count == 1
    finally:
        await session.stop()
        await server.stop()


async def test_a_late_pair_adopted_while_account_auth_is_in_flight_is_used_at_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The refresh before account auth gets no reply in time, so auth goes out with the old
    # token; the venue then answers the refresh and rejects the auth, in that order. The
    # adopted pair works, so the bring-up must not fail.
    monkeypatch.setattr("nautilus_ctrader.common.session.LATE_REFRESH_WAIT_SECS", 0.1)
    server = _server()
    held = HeldReplies(server, oa_model.PROTO_OA_REFRESH_TOKEN_REQ, _refresh_response)
    pushes: list[asyncio.Task] = []

    async def answer_refresh_then_reject(msg_id: str) -> None:
        await held.release()
        await server.push(
            oa.ProtoOAErrorRes(errorCode="CH_ACCESS_TOKEN_INVALID"),
            client_msg_id=msg_id,
        )

    def account_auth(request: oa.ProtoOAAccountAuthReq) -> oa.ProtoOAAccountAuthRes | None:
        if request.accessToken == "new-access":
            return oa.ProtoOAAccountAuthRes(ctidTraderAccountId=ACCOUNT_ID)
        msg_id = server.received_client_msg_ids[-1]
        pushes.append(asyncio.create_task(answer_refresh_then_reject(msg_id)))
        return None

    server.on(oa_model.PROTO_OA_ACCOUNT_AUTH_REQ, account_auth)
    await server.start()
    session = _session(
        server,
        expires_at_secs=time.time() + 1.0,
        request_timeout_secs=0.2,
    )
    try:
        await session.start()
        await session.wait_ready(timeout_secs=3.0)

        assert len(_refreshes(server)) == 1
        assert [m.accessToken for m in _account_auths(server)] == ["old-access", "new-access"]
        assert server.connection_count == 1
    finally:
        await session.stop()
        await server.stop()


async def test_a_refresh_due_once_ready_reauthenticates_with_the_new_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = _server()
    await server.start()
    session = _session(server, expires_at_secs=_due_after_ready(monkeypatch))
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
        session.add_event_handler(oa.ProtoOARefreshTokenRes, seen.append)
        session.add_event_handler(oa.ProtoOASpotEvent, seen.append)
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


def _pair_reply(access: str, refresh: str) -> Callable[[object], oa.ProtoOARefreshTokenRes]:
    return lambda _request: oa.ProtoOARefreshTokenRes(
        accessToken=access,
        tokenType="bearer",
        expiresIn=2_592_000,
        refreshToken=refresh,
    )


_TOKENS = ("old-access", "old-refresh", "late-access", "late-refresh", "new-access", "new-refresh")


def _lines_with_a_token(logger: RecordingLogger) -> list[str]:
    return [message for _, message in logger.lines if any(t in message for t in _TOKENS)]


async def test_a_refresh_reply_that_misses_its_timeout_is_adopted() -> None:
    # A refresh token is single-use, so once the request reached the venue the late reply holds
    # the only pair that still works.
    server = _server()
    held = HeldReplies(
        server,
        oa_model.PROTO_OA_REFRESH_TOKEN_REQ,
        _pair_reply("late-access", "late-refresh"),
    )
    await server.start()
    logger = RecordingLogger()
    persisted: list[tuple[str, str, float]] = []
    session = _session(
        server,
        logger,
        request_timeout_secs=0.2,
        backoff_base_secs=0.05,
        on_tokens_refreshed=lambda a, r, e: persisted.append((a, r, e)),
    )
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        with pytest.raises(CTraderTimeoutError):
            await session.refresh_tokens()

        before = time.time()
        await held.stop_holding()
        await wait_until(lambda: bool(persisted), description="the late pair being adopted")
        # Put to use at once, as after any proactive refresh.
        await wait_until(
            lambda: _account_auths(server)[-1].accessToken == "late-access",
            description="re-authentication with the adopted token",
        )
        await session.wait_ready(timeout_secs=3.0)

        assert [(a, r) for a, r, _ in persisted] == [("late-access", "late-refresh")]
        assert persisted[0][2] >= before + 2_592_000
        assert any("late token refresh reply" in m for m in logger.warnings())
        await session.refresh_tokens()
        assert _refreshes(server)[-1].refreshToken == "late-refresh"
        assert not _lines_with_a_token(logger)
    finally:
        await session.stop()
        await server.stop()


async def test_a_refresh_reply_dispatched_as_its_request_times_out_is_adopted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # On a timeout the reply can reach the event path before the requester resumes, while the
    # request is still on its way out of `refresh_tokens()`.
    server = _server()
    await server.start()
    persisted: list[tuple[str, str, float]] = []
    session = _session(server, on_tokens_refreshed=lambda a, r, e: persisted.append((a, r, e)))
    send = session._connection.request

    async def reply_then_time_out(payload, **kwargs):
        if isinstance(payload, oa.ProtoOARefreshTokenReq):
            kwargs["on_written"]()
            session._on_event(_pair_reply("late-access", "late-refresh")(payload))
            raise CTraderTimeoutError("no response in time")
        return await send(payload, **kwargs)

    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        monkeypatch.setattr(session._connection, "request", reply_then_time_out)

        with pytest.raises(CTraderTimeoutError):
            await session.refresh_tokens()

        assert [(a, r) for a, r, _ in persisted] == [("late-access", "late-refresh")]
    finally:
        await session.stop()
        await server.stop()


async def test_a_refusal_after_the_earlier_reply_was_adopted_leaves_nothing_awaited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The first refresh times out; while the second is in flight the first one's reply is
    # adopted; then the second is refused. Nothing is outstanding any more.
    server = _server()
    await server.start()
    logger = RecordingLogger()
    persisted: list[tuple[str, str, float]] = []
    session = _session(
        server,
        logger,
        on_tokens_refreshed=lambda a, r, e: persisted.append((a, r, e)),
    )
    send = session._connection.request
    attempts = 0

    async def time_out_then_adopt_and_refuse(payload, **kwargs):
        nonlocal attempts
        if not isinstance(payload, oa.ProtoOARefreshTokenReq):
            return await send(payload, **kwargs)
        attempts += 1
        kwargs["on_written"]()
        if attempts == 1:
            raise CTraderTimeoutError("no response in time")
        session._on_event(_pair_reply("late-access", "late-refresh")(payload))
        raise CTraderRequestError("CH_ACCESS_TOKEN_INVALID")

    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        monkeypatch.setattr(session._connection, "request", time_out_then_adopt_and_refuse)
        with pytest.raises(CTraderTimeoutError):
            await session.refresh_tokens()
        with pytest.raises(CTraderAuthError):
            await session.refresh_tokens()

        assert session._refresh_settled.is_set()
        # So a stray reply now is not adopted.
        session._on_event(_pair_reply("stray-access", "stray-refresh")(None))
        assert [(a, r) for a, r, _ in persisted] == [("late-access", "late-refresh")]
    finally:
        await session.stop()
        await server.stop()


async def test_a_refused_refresh_leaves_an_earlier_unanswered_one_adoptable() -> None:
    # Both carry the same refresh token, so the refusal can be the venue saying the earlier
    # request already spent it - whose reply is then the only one worth having.
    server = _server()
    held = HeldReplies(
        server,
        oa_model.PROTO_OA_REFRESH_TOKEN_REQ,
        _pair_reply("late-access", "late-refresh"),
    )
    await server.start()
    persisted: list[tuple[str, str, float]] = []
    session = _session(
        server,
        request_timeout_secs=0.2,
        on_tokens_refreshed=lambda a, r, e: persisted.append((a, r, e)),
    )
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        with pytest.raises(CTraderTimeoutError):
            await session.refresh_tokens()
        pending = held.pending
        held.pending = []
        server.on(
            oa_model.PROTO_OA_REFRESH_TOKEN_REQ,
            lambda _r: oa.ProtoOAErrorRes(errorCode="CH_ACCESS_TOKEN_INVALID"),
        )
        with pytest.raises(CTraderAuthError):
            await session.refresh_tokens()

        for client_msg_id, request in pending:
            await server.push(
                _pair_reply("late-access", "late-refresh")(request),
                client_msg_id=client_msg_id,
            )
        await wait_until(lambda: bool(persisted), description="the late pair being adopted")

        assert [(a, r) for a, r, _ in persisted] == [("late-access", "late-refresh")]
    finally:
        await session.stop()
        await server.stop()


async def test_a_late_refresh_reply_after_a_newer_pair_is_ignored() -> None:
    # The venue keeps only the newest grant's chain, so a pair taken since the late reply's
    # request was sent is the one to keep.
    server = _server()
    held = HeldReplies(
        server,
        oa_model.PROTO_OA_REFRESH_TOKEN_REQ,
        _pair_reply("late-access", "late-refresh"),
    )
    await server.start()
    logger = RecordingLogger()
    persisted: list[tuple[str, str, float]] = []
    session = _session(
        server,
        logger,
        request_timeout_secs=0.2,
        on_tokens_refreshed=lambda a, r, e: persisted.append((a, r, e)),
    )
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        with pytest.raises(CTraderTimeoutError):
            await session.refresh_tokens()
        server.on(oa_model.PROTO_OA_REFRESH_TOKEN_REQ, _pair_reply("new-access", "new-refresh"))
        await session.refresh_tokens()

        await held.release()
        await wait_until(
            lambda: any(
                level == "debug" and "late token refresh reply" in message
                for level, message in logger.lines
            ),
            description="the late reply being ignored",
        )

        assert [(a, r) for a, r, _ in persisted] == [("new-access", "new-refresh")]
        await session.refresh_tokens()
        assert _refreshes(server)[-1].refreshToken == "new-refresh"
        assert not _lines_with_a_token(logger)
    finally:
        await session.stop()
        await server.stop()


def _server_rejecting(access_token: str) -> FakeCTraderServer:
    """A venue that refuses account authentication with `access_token` as expired."""
    server = _server()
    server.on(
        oa_model.PROTO_OA_ACCOUNT_AUTH_REQ,
        lambda request: (
            oa.ProtoOAErrorRes(errorCode="OA_AUTH_TOKEN_EXPIRED", description="expired")
            if request.accessToken == access_token
            else oa.ProtoOAAccountAuthRes(ctidTraderAccountId=ACCOUNT_ID)
        ),
    )
    return server


async def test_a_late_refresh_reply_during_bring_up_is_adopted_and_the_bring_up_goes_on() -> None:
    server = _server_rejecting("old-access")
    held = HeldReplies(
        server,
        oa_model.PROTO_OA_REFRESH_TOKEN_REQ,
        _pair_reply("late-access", "late-refresh"),
    )
    await server.start()
    logger = RecordingLogger()
    persisted: list[tuple[str, str, float]] = []
    # A reconnect would only come after the backoff, far beyond this test.
    session = _session(
        server,
        logger,
        request_timeout_secs=0.2,
        backoff_base_secs=30.0,
        on_tokens_refreshed=lambda a, r, e: persisted.append((a, r, e)),
    )
    try:
        await session.start()
        await held.arrived.wait()
        # Well past the refresh request's own timeout.
        await asyncio.sleep(0.4)
        await held.stop_holding()
        await session.wait_ready(timeout_secs=3.0)

        assert server.connection_count == 1
        assert [(a, r) for a, r, _ in persisted] == [("late-access", "late-refresh")]
        assert [m.accessToken for m in _account_auths(server)] == ["old-access", "late-access"]
        assert any("late token refresh reply" in m for m in logger.warnings())
        server.on(oa_model.PROTO_OA_REFRESH_TOKEN_REQ, _pair_reply("new-access", "new-refresh"))
        await session.refresh_tokens()
        assert _refreshes(server)[-1].refreshToken == "late-refresh"
        assert not _lines_with_a_token(logger)
    finally:
        await session.stop()
        await server.stop()


async def test_a_bring_up_refresh_that_is_never_answered_fails_the_bring_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("nautilus_ctrader.common.session.LATE_REFRESH_WAIT_SECS", 0.2)
    server = _server_rejecting("old-access")
    server.on(oa_model.PROTO_OA_REFRESH_TOKEN_REQ, lambda _r: None)
    await server.start()
    logger = RecordingLogger()
    session = _session(server, logger, request_timeout_secs=0.2, backoff_base_secs=30.0)
    try:
        await session.start()
        await wait_until(
            lambda: any("Session bring-up failed" in m for m in logger.warnings()),
            description="the bring-up failing",
        )

        assert isinstance(session.last_error, CTraderTimeoutError)
        assert session.state is not SessionState.READY
    finally:
        await session.stop()
        await server.stop()


async def test_a_connection_lost_during_the_wait_for_a_late_reply_fails_the_bring_up_at_once() -> (
    None
):
    server = _server_rejecting("old-access")
    server.on(oa_model.PROTO_OA_REFRESH_TOKEN_REQ, lambda _r: None)
    await server.start()
    logger = RecordingLogger()
    session = _session(server, logger, request_timeout_secs=0.2, backoff_base_secs=30.0)
    try:
        await session.start()
        await wait_until(lambda: bool(_refreshes(server)), description="the refresh request")
        # Past the request's timeout, so inside the wait for a late reply.
        await asyncio.sleep(0.4)

        await server.drop_connections()
        # Far sooner than the wait's own bound.
        await wait_until(
            lambda: any("Session bring-up failed" in m for m in logger.warnings()),
            timeout_secs=2.0,
            description="the bring-up failing",
        )

        assert isinstance(session.last_error, CTraderConnectionError)
    finally:
        await session.stop()
        await server.stop()


async def test_a_stop_during_the_wait_for_a_late_refresh_reply_is_prompt() -> None:
    server = _server_rejecting("old-access")
    server.on(oa_model.PROTO_OA_REFRESH_TOKEN_REQ, lambda _r: None)
    await server.start()
    session = _session(server, request_timeout_secs=0.2)
    try:
        await session.start()
        await wait_until(lambda: bool(_refreshes(server)), description="the refresh request")
        # Past the request's timeout, so inside the wait for a late reply.
        await asyncio.sleep(0.4)

        started = time.monotonic()
        await asyncio.wait_for(session.stop(), 2.0)

        assert time.monotonic() - started < 1.0
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
    logger = RecordingLogger()
    session = _session(server, logger=logger, backoff_base_secs=0.05)
    try:
        await session.start()
        await wait_until(
            lambda: server.connection_count >= 3,
            description="several rejected attempts",
        )

        assert len(_refreshes(server)) == 1
        assert isinstance(session.last_error, CTraderAuthError)
        # The attempt that refreshed failed on the retry with the new token.
        first = next(m for _level, m in logger.lines if "bring-up failed (attempt 1)" in m)
        assert "account auth rejected after refresh" in first
    finally:
        await session.stop()
        await server.stop()


async def test_a_token_rejection_refreshes_again_once_the_interval_has_passed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("nautilus_ctrader.common.session.MIN_TOKEN_REFRESH_INTERVAL_SECS", 0.01)
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
            lambda: len(_refreshes(server)) >= 2,
            description="a second reactive refresh",
        )
    finally:
        await session.stop()
        await server.stop()


async def test_a_raising_persistence_callback_is_logged_and_the_session_carries_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The refresh succeeded, so the session must keep working on the new tokens - but the
    # operator must hear that they were not saved, without the tokens reaching the log.
    server = _server()
    await server.start()
    logger = RecordingLogger()

    def explode(_access: str, _refresh: str, _expires_at: float) -> None:
        raise RuntimeError("storage unavailable")

    session = _session(
        server,
        logger,
        expires_at_secs=_due_after_ready(monkeypatch),
        on_tokens_refreshed=explode,
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


async def test_a_proactive_refresh_that_fails_unexpectedly_is_logged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A malformed frame rejects the pending refresh with a protocol error - neither an auth
    # nor a connection error - and that must still be logged rather than end the loop silently.
    server = _server()
    server.on(oa_model.PROTO_OA_REFRESH_TOKEN_REQ, lambda _r: None)
    await server.start()
    logger = RecordingLogger()
    session = _session(
        server,
        logger,
        expires_at_secs=_due_after_ready(monkeypatch),
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
    session = _session(
        server,
        logger,
        expires_at_secs=_due_after_ready(monkeypatch),
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
    session = _session(
        server, expires_at_secs=_due_after_ready(monkeypatch), request_timeout_secs=0.3
    )
    try:
        await session.start()
        await wait_until(
            lambda: len(_refreshes(server)) >= 2,
            description="a second refresh attempt",
        )
    finally:
        await session.stop()
        await server.stop()


async def test_a_proactive_reauth_is_not_treated_as_a_failed_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A proactive refresh forces re-authentication with the new token by design; that must be
    # recognised as recovery, not counted as a session that failed soon after becoming ready.
    server = _server()
    await server.start()
    logger = RecordingLogger()
    session = _session(
        server,
        logger,
        expires_at_secs=_due_after_ready(monkeypatch),
        backoff_base_secs=5.0,
    )
    loop = asyncio.get_running_loop()
    try:
        started = loop.time()
        await session.start()
        await wait_until(lambda: server.connection_count >= 2, description="re-authentication")
        await session.wait_ready(timeout_secs=2.0)
        elapsed = loop.time() - started

        # About a second until the refresh is due; a backoff would add at least five.
        assert elapsed < 4.0, f"re-authentication took {elapsed:.2f}s, a backoff appears to apply"
        assert not any("soon after becoming ready" in message for _level, message in logger.lines)
        assert ("info", "Re-authenticating with the refreshed token") in logger.lines
    finally:
        await session.stop()
        await server.stop()


async def test_a_loss_during_a_proactive_refresh_keeps_its_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A real loss that lands while the refresh completes is a failure, not a deliberate
    # re-authentication, and must not be counted as recovery.
    server = _server()
    await server.start()
    logger = RecordingLogger()
    session = _session(
        server,
        logger,
        expires_at_secs=_due_after_ready(monkeypatch),
        backoff_base_secs=0.05,
    )
    original_refresh = CTraderSession.refresh_tokens

    async def refresh_then_lose(self: CTraderSession) -> None:
        await original_refresh(self)
        # No await before the refresh loop's next line, so the ordering is fixed.
        self._on_disconnect(CTraderConnectionError("lost while refreshing"))

    monkeypatch.setattr(CTraderSession, "refresh_tokens", refresh_then_lose)
    try:
        await session.start()
        await wait_until(
            lambda: any("soon after becoming ready" in m for _level, m in logger.lines),
            description="the loss taking the backoff path",
        )

        assert ("info", "Re-authenticating with the refreshed token") not in logger.lines
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
    session.add_event_handler(
        oa.ProtoOAAccountDisconnectEvent,
        lambda _payload: states.append(session.state),
    )
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
    session = _session(server, logger, refresh_token=None, backoff_base_secs=0.05)
    seen: list[object] = []
    session.add_event_handler(oa.ProtoOAAccountDisconnectEvent, seen.append)
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


class _HeldRefresh:
    """Holds the first refresh request's reply, to be written later on the connection that asked.

    A real venue answers only on that connection, so a reply must never reach a later one.
    """

    def __init__(self, server: FakeCTraderServer) -> None:
        self._server = server
        self._writer: asyncio.StreamWriter | None = None
        self._msg_id: str | None = None
        self.arrived = asyncio.Event()
        server.on(oa_model.PROTO_OA_REFRESH_TOKEN_REQ, self._handle)

    def _handle(self, request: oa.ProtoOARefreshTokenReq) -> oa.ProtoOARefreshTokenRes | None:
        if self.arrived.is_set():
            return _refresh_response(request)
        self._writer = self._server._writers[-1]
        self._msg_id = self._server.received_client_msg_ids[-1]
        self.arrived.set()
        return None

    async def write(self, message: object, *, as_reply: bool = False) -> None:
        assert self._writer is not None
        await self._server._write(self._writer, message, self._msg_id if as_reply else None)

    async def end_the_session(self) -> None:
        await self.write(oa.ProtoOAAccountsTokenInvalidatedEvent(ctidTraderAccountIds=[ACCOUNT_ID]))
        await self.write(oa.ProtoOAAccountDisconnectEvent(ctidTraderAccountId=ACCOUNT_ID))

    async def answer(self) -> None:
        await self.write(_refresh_response(None), as_reply=True)


@pytest.mark.parametrize("reply_state", ["in-flight", "timed-out"])
async def test_a_refresh_lost_to_a_teardown_is_waited_for_then_logged(
    monkeypatch: pytest.MonkeyPatch,
    reply_state: str,
) -> None:
    # The venue ends the session while a READY-path refresh still awaits its reply - either
    # within the request's own timeout or as a late reply - and the reply never comes.
    hold_secs = 0.5
    monkeypatch.setattr("nautilus_ctrader.common.session.LATE_REFRESH_WAIT_SECS", hold_secs)
    server = _server()
    held = _HeldRefresh(server)
    await server.start()
    logger = RecordingLogger()
    persisted: list[tuple[str, str, float]] = []
    session = _session(
        server,
        logger,
        expires_at_secs=_due_after_ready(monkeypatch),
        request_timeout_secs=5.0 if reply_state == "in-flight" else 0.2,
        backoff_base_secs=0.05,
        on_tokens_refreshed=lambda a, r, e: persisted.append((a, r, e)),
    )
    try:
        await session.start()
        await asyncio.wait_for(held.arrived.wait(), 5.0)
        if reply_state == "timed-out":
            await wait_until(
                lambda: any("Proactive token refresh failed" in m for _, m in logger.lines),
                description="the refresh timing out",
            )

        loop = asyncio.get_running_loop()
        ended_at = loop.time()
        await held.end_the_session()
        await wait_until(lambda: server.connection_count >= 2, description="the reconnect")

        assert loop.time() - ended_at >= hold_secs * 0.9
        assert any("reply lost" in m for m in logger.errors())
        assert persisted == []
        assert not _lines_with_a_token(logger)
    finally:
        await session.stop()
        await server.stop()


@pytest.mark.parametrize("order", ["events-before-reply", "reply-before-events"])
async def test_a_reply_arriving_during_the_held_teardown_is_kept(
    monkeypatch: pytest.MonkeyPatch,
    order: str,
) -> None:
    # The venue's order is unconfirmed, so the outcome must not depend on it.
    server = _server()
    held = _HeldRefresh(server)
    await server.start()
    logger = RecordingLogger()
    persisted: list[tuple[str, str, float]] = []
    session = _session(
        server,
        logger,
        expires_at_secs=_due_after_ready(monkeypatch),
        backoff_base_secs=0.05,
        on_tokens_refreshed=lambda a, r, e: persisted.append((a, r, e)),
    )
    try:
        await session.start()
        await asyncio.wait_for(held.arrived.wait(), 5.0)

        # Apart, so each lands in its own read.
        if order == "events-before-reply":
            await held.end_the_session()
            await asyncio.sleep(0.1)
            await held.answer()
        else:
            await held.answer()
            await asyncio.sleep(0.1)
            await held.end_the_session()
        await wait_until(lambda: server.connection_count >= 2, description="the reconnect")
        await session.wait_ready(timeout_secs=3.0)

        assert [(a, r) for a, r, _ in persisted] == [("new-access", "new-refresh")]
        assert _account_auths(server)[-1].accessToken == "new-access"
        assert not any("reply lost" in m for m in logger.errors())
    finally:
        await session.stop()
        await server.stop()


async def test_a_stop_logs_a_refresh_reply_it_loses(monkeypatch: pytest.MonkeyPatch) -> None:
    # Nothing stays awaited, or a restarted session would hold its first teardown for a reply
    # that cannot come.
    server = _server()
    held = _HeldRefresh(server)
    await server.start()
    logger = RecordingLogger()
    session = _session(server, logger, expires_at_secs=_due_after_ready(monkeypatch))
    try:
        await session.start()
        await asyncio.wait_for(held.arrived.wait(), 5.0)

        await session.stop()

        assert session._refresh_settled.is_set()
        assert any("reply lost" in m for m in logger.errors())
        assert not _lines_with_a_token(logger)
    finally:
        await session.stop()
        await server.stop()


async def test_a_refresh_still_queued_at_a_teardown_is_neither_held_nor_lost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hold_secs = 0.5
    monkeypatch.setattr("nautilus_ctrader.common.session.LATE_REFRESH_WAIT_SECS", hold_secs)
    server = _server()
    await server.start()
    logger = RecordingLogger()
    session = _session(
        server,
        logger,
        expires_at_secs=_due_after_ready(monkeypatch),
        backoff_base_secs=0.05,
    )
    send = session._connection.request
    queued = asyncio.Event()

    async def wait_in_the_queue(payload, **kwargs):
        if isinstance(payload, oa.ProtoOARefreshTokenReq):
            queued.set()
            # Never written.
            await asyncio.Event().wait()
        return await send(payload, **kwargs)

    monkeypatch.setattr(session._connection, "request", wait_in_the_queue)
    try:
        await session.start()
        await asyncio.wait_for(queued.wait(), 5.0)

        loop = asyncio.get_running_loop()
        ended_at = loop.time()
        await server.push(
            oa.ProtoOAAccountsTokenInvalidatedEvent(ctidTraderAccountIds=[ACCOUNT_ID])
        )
        await server.push(oa.ProtoOAAccountDisconnectEvent(ctidTraderAccountId=ACCOUNT_ID))
        await wait_until(lambda: server.connection_count >= 2, description="the reconnect")

        assert loop.time() - ended_at < hold_secs
        assert not any("reply lost" in m for m in logger.errors())
    finally:
        await session.stop()
        await server.stop()


async def test_a_late_reply_arriving_during_the_held_teardown_is_adopted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The refresh timed out, the venue ends the session, then the late reply comes.
    server = _server()
    held = _HeldRefresh(server)
    await server.start()
    logger = RecordingLogger()
    persisted: list[tuple[str, str, float]] = []
    session = _session(
        server,
        logger,
        expires_at_secs=_due_after_ready(monkeypatch),
        request_timeout_secs=0.2,
        backoff_base_secs=0.05,
        on_tokens_refreshed=lambda a, r, e: persisted.append((a, r, e)),
    )
    try:
        await session.start()
        await asyncio.wait_for(held.arrived.wait(), 5.0)
        await wait_until(
            lambda: any("Proactive token refresh failed" in m for _, m in logger.lines),
            description="the refresh timing out",
        )

        await held.end_the_session()
        await asyncio.sleep(0.1)
        await held.answer()
        await wait_until(lambda: server.connection_count >= 2, description="the reconnect")
        await session.wait_ready(timeout_secs=3.0)

        assert [(a, r) for a, r, _ in persisted] == [("new-access", "new-refresh")]
        assert _account_auths(server)[-1].accessToken == "new-access"
        assert any("late token refresh reply" in m for m in logger.warnings())
        assert not any("reply lost" in m for m in logger.errors())
        # The session was already lost, so the adoption asked for no re-authentication.
        assert ("info", "Re-authenticating with the refreshed token") not in logger.lines
    finally:
        await session.stop()
        await server.stop()


@pytest.mark.parametrize("failure", ["refused", "connection-lost"])
async def test_the_refresh_loop_ending_is_logged(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    hold_secs = 5.0
    monkeypatch.setattr("nautilus_ctrader.common.session.LATE_REFRESH_WAIT_SECS", hold_secs)
    server = _server()
    held = _HeldRefresh(server)
    if failure == "refused":
        server.on(
            oa_model.PROTO_OA_REFRESH_TOKEN_REQ,
            lambda _r: oa.ProtoOAErrorRes(errorCode="CH_ACCESS_TOKEN_INVALID"),
        )
    await server.start()
    logger = RecordingLogger()
    session = _session(
        server,
        logger,
        expires_at_secs=_due_after_ready(monkeypatch),
        backoff_base_secs=30.0,
    )
    try:
        await session.start()
        await wait_until(lambda: bool(_refreshes(server)), description="the refresh request")
        if failure == "connection-lost":
            # The venue ends the session, so the teardown is held; then it drops the socket,
            # which must end the hold at once.
            await held.end_the_session()
            await asyncio.sleep(0.1)
            loop = asyncio.get_running_loop()
            dropped_at = loop.time()
            await server.drop_connections()
            await wait_until(
                lambda: any("reply lost" in m for m in logger.errors()),
                description="the lost reply being logged",
            )
            assert loop.time() - dropped_at < 1.0

        await wait_until(
            lambda: any("Proactive token refresh stopped" in m for m in logger.warnings()),
            description="the loop's end being logged",
        )
    finally:
        await session.stop()
        await server.stop()


async def test_a_refresh_that_never_left_does_not_wait_for_a_late_pair(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A rejected account auth refreshes; that refresh times out in the queue, unsent. No reply
    # can come, so the bring-up must fail at once, well inside the late-reply wait.
    server = _server_rejecting("old-access")
    await server.start()
    logger = RecordingLogger()
    session = _session(server, logger, backoff_base_secs=30.0)
    send = session._connection.request

    async def time_out_in_the_queue(payload, **kwargs):
        if isinstance(payload, oa.ProtoOARefreshTokenReq):
            raise CTraderQueueTimeoutError("dropped unsent")
        return await send(payload, **kwargs)

    monkeypatch.setattr(session._connection, "request", time_out_in_the_queue)
    try:
        await session.start()
        await wait_until(
            lambda: any("Session bring-up failed" in m for m in logger.warnings()),
            timeout_secs=2.0,
            description="the bring-up failing",
        )

        assert isinstance(session.last_error, CTraderQueueTimeoutError)
        assert session._refresh_settled.is_set()
        assert not any("reply lost" in m for m in logger.errors())
    finally:
        await session.stop()
        await server.stop()


async def test_a_refresh_before_account_auth_that_was_not_sent_says_so(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = _server()
    await server.start()
    logger = RecordingLogger()
    session = _session(server, logger, expires_at_secs=time.time() + 1.0)
    send = session._connection.request

    async def time_out_in_the_queue(payload, **kwargs):
        if isinstance(payload, oa.ProtoOARefreshTokenReq):
            raise CTraderQueueTimeoutError("dropped unsent")
        return await send(payload, **kwargs)

    monkeypatch.setattr(session._connection, "request", time_out_in_the_queue)
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        assert [m.accessToken for m in _account_auths(server)] == ["old-access"]
        assert any("not sent" in m for m in logger.warnings())
        assert not any("got no reply" in m for m in logger.warnings())
    finally:
        await session.stop()
        await server.stop()
