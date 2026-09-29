"""The per-account client: host selection, session ownership, reference data."""

import asyncio
import time

import pytest
from google.protobuf.message import Message
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.identifiers import InstrumentId, Symbol

from nautilus_ctrader.common import account as account_module
from nautilus_ctrader.common.account import (
    CTraderAccountClient,
    get_cached_ctrader_account_client,
)
from nautilus_ctrader.common.errors import (
    CTraderAuthError,
    CTraderConnectionError,
    CTraderTimeoutError,
)
from nautilus_ctrader.common.session import SessionState
from nautilus_ctrader.constants import CTRADER_VENUE
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from nautilus_ctrader.providers import CTraderInstrumentProvider
from tests.account_venue import (
    ACCOUNT_ID,
    RECORDED,
    TRADER_LOGIN,
    HeldReplies,
    account_client,
    account_list,
    credentials,
    hold_account_auth,
    received,
    venue,
)
from tests.polling import wait_until
from tests.recording_logger import RecordingLogger

# `credentials()` defaults the expiry to "now + an hour", so the cache tests pin it to keep
# two calls byte-identical.
_EXPIRES_AT = 1_700_000_000.0
# Quoted in EUR, so it needs a conversion chain into the recorded USD deposit.
GER40_ID = InstrumentId(Symbol("GER40.cash"), CTRADER_VENUE)


async def test_auto_connects_to_the_listed_account_and_closes_the_pre_connection() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        await client.connect()

        response = await client.request(oa.ProtoOATraderReq(ctidTraderAccountId=ACCOUNT_ID))

        assert isinstance(response, oa.ProtoOATraderRes)
        assert server.connection_count == 2
        await wait_until(lambda: server.open_connection_count == 1, description="pre-conn closed")
        assert received(server, oa.ProtoOAGetAccountListByAccessTokenReq)[0].accessToken == (
            "access-token"
        )
    finally:
        await client.disconnect()
        await server.stop()


@pytest.mark.parametrize(
    ("environment", "is_live", "expected"),
    [
        ("auto", True, "live.invalid"),
        ("auto", False, "127.0.0.1"),
        # An explicit environment still resolves the login, but keeps the host it names.
        ("demo", True, "127.0.0.1"),
        ("live", False, "live.invalid"),
    ],
)
async def test_the_host_follows_the_is_live_flag_unless_the_environment_names_one(
    monkeypatch: pytest.MonkeyPatch,
    environment: str,
    is_live: bool,
    expected: str,
) -> None:
    hosts = []
    real_session = account_module.CTraderSession

    server = venue(is_live=is_live)

    # The live name does not resolve; the session is pointed back at the one fake server.
    def recording_session(**kwargs):
        hosts.append(kwargs["host"])
        return real_session(**{**kwargs, "host": server.host})

    monkeypatch.setattr(account_module, "CTraderSession", recording_session)
    await server.start()
    client = CTraderAccountClient(
        trader_login=TRADER_LOGIN,
        credentials=credentials(),
        environment=environment,
        logger=RecordingLogger(),
        demo_host=server.host,
        live_host="live.invalid",
        port=server.port,
        tls=False,
    )
    try:
        await client.connect()

        assert hosts == [expected]
        assert client.account_id == ACCOUNT_ID
    finally:
        await client.disconnect()
        await server.stop()


async def test_a_login_the_token_does_not_grant_is_refused() -> None:
    server = venue(logins=())
    await server.start()
    client = account_client(server)
    try:
        with pytest.raises(CTraderAuthError, match="no account with that trader login") as info:
            await client.connect()

        assert str(ACCOUNT_ID) not in str(info.value)
        assert str(TRADER_LOGIN) not in str(info.value)
        assert client.session is None
        await wait_until(lambda: server.open_connection_count == 0, description="all closed")
    finally:
        await server.stop()


async def test_two_granted_accounts_sharing_a_login_are_refused_rather_than_guessed() -> None:
    """Logins are unique per broker server, but one token can grant accounts on several."""
    server = venue(logins=(TRADER_LOGIN, TRADER_LOGIN))
    await server.start()
    client = account_client(server)
    try:
        with pytest.raises(CTraderAuthError, match="more than one granted account") as info:
            await client.connect()

        assert str(ACCOUNT_ID) not in str(info.value)
        assert str(TRADER_LOGIN) not in str(info.value)
        assert client.session is None
        assert not received(server, oa.ProtoOAAccountAuthReq)
    finally:
        await server.stop()


async def test_the_account_id_is_unreadable_before_it_is_resolved() -> None:
    """A request built before the login is looked up must fail, not go out unaddressed."""
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        with pytest.raises(CTraderConnectionError, match="not resolved"):
            _ = client.account_id

        await client.connect()

        assert client.account_id == ACCOUNT_ID
    finally:
        await client.disconnect()
        await server.stop()


async def test_auto_refreshes_a_rejected_token_once_and_notifies_listeners() -> None:
    server = venue()
    calls = []

    def list_accounts(request: oa.ProtoOAGetAccountListByAccessTokenReq) -> Message:
        calls.append(request.accessToken)
        if request.accessToken == "access-token":
            return oa.ProtoOAErrorRes(errorCode="CH_ACCESS_TOKEN_INVALID")
        return account_list(is_live=True)

    server.on(oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ, list_accounts)
    server.on(
        oa_model.PROTO_OA_REFRESH_TOKEN_REQ,
        lambda _r: oa.ProtoOARefreshTokenRes(
            accessToken="new-access",
            tokenType="bearer",
            expiresIn=3600,
            refreshToken="new-refresh",
        ),
    )
    await server.start()
    client = account_client(server)
    notified = []
    client.add_token_listener(lambda a, r, e: notified.append((a, r, e)))
    try:
        before = time.time()
        await client.connect()

        assert calls == ["access-token", "new-access"]
        assert [r.refreshToken for r in received(server, oa.ProtoOARefreshTokenReq)] == [
            "refresh-token",
        ]
        assert len(notified) == 1
        access, refresh, expires_at = notified[0]
        assert (access, refresh) == ("new-access", "new-refresh")
        assert before + 3600 <= expires_at <= time.time() + 3600
        # The session is built with the refreshed token.
        assert received(server, oa.ProtoOAAccountAuthReq)[0].accessToken == "new-access"
    finally:
        await client.disconnect()
        await server.stop()


async def test_auto_without_a_refresh_token_fails_on_a_rejected_token() -> None:
    server = venue()
    server.on(
        oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="CH_ACCESS_TOKEN_INVALID"),
    )
    await server.start()
    client = account_client(server, credentials=credentials(refresh_token=None))
    try:
        with pytest.raises(CTraderAuthError, match="no refresh token"):
            await client.connect()
        assert not received(server, oa.ProtoOARefreshTokenReq)
    finally:
        await server.stop()


@pytest.mark.parametrize("environment", ["auto", "demo"])
async def test_wrong_client_secret_fails_fast(environment: str) -> None:
    server = venue()
    server.on(
        oa_model.PROTO_OA_APPLICATION_AUTH_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="CH_CLIENT_AUTH_FAILURE"),
    )
    await server.start()
    client = account_client(server, environment=environment, connect_timeout_secs=30.0)
    try:
        started = time.monotonic()
        with pytest.raises(CTraderAuthError, match="CH_CLIENT_AUTH_FAILURE"):
            await client.connect()
        assert time.monotonic() - started < 2.0
        assert client.session is None
        await wait_until(lambda: server.open_connection_count == 0, description="all closed")
    finally:
        await server.stop()


async def test_explicit_environment_that_cannot_route_names_the_problem() -> None:
    server = venue()
    server.on(
        oa_model.PROTO_OA_ACCOUNT_AUTH_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="CANT_ROUTE_REQUEST"),
    )
    await server.start()
    client = account_client(server, environment="live", connect_timeout_secs=30.0)
    try:
        started = time.monotonic()
        with pytest.raises(CTraderAuthError, match="use 'auto'") as info:
            await client.connect()
        assert time.monotonic() - started < 2.0
        assert str(ACCOUNT_ID) not in str(info.value)
        # The list is fetched even for an explicit environment: it resolves the login.
        assert len(received(server, oa.ProtoOAGetAccountListByAccessTokenReq)) == 1
        await wait_until(lambda: server.open_connection_count == 0, description="all closed")
    finally:
        await server.stop()


async def test_a_session_that_never_becomes_ready_times_out() -> None:
    server = venue()
    server.on(oa_model.PROTO_OA_ACCOUNT_AUTH_REQ, lambda _r: None)
    await server.start()
    client = account_client(server, environment="demo", connect_timeout_secs=0.6)
    try:
        with pytest.raises(CTraderTimeoutError):
            await client.connect()
        assert client.session is None
        await wait_until(lambda: server.open_connection_count == 0, description="all closed")
    finally:
        await server.stop()


async def test_missing_token_expiry_logs_a_warning() -> None:
    server = venue()
    await server.start()
    logger = RecordingLogger()
    client = account_client(server, logger=logger, credentials=credentials(token_expires_at=None))
    try:
        await client.connect()

        assert (
            "warning",
            "token_expires_at not set: proactive token refresh is disabled",
        ) in logger.lines
    finally:
        await client.disconnect()
        await server.stop()


async def test_account_details_never_reach_the_log() -> None:
    server = venue()
    await server.start()
    logger = RecordingLogger()
    client = account_client(server, logger=logger)
    try:
        await client.connect()
    finally:
        await client.disconnect()
        await server.stop()

    text = "\n".join(message for _level, message in logger.lines)
    assert str(ACCOUNT_ID) not in text
    # The login the venue served and the client was configured with, not the fixture's: the
    # recorder clears `traderLogin`, and an unset one reads back as 0, which any log line may
    # contain.
    assert str(TRADER_LOGIN) not in text
    for entry in RECORDED["account_list"][0].ctidTraderAccount:
        if entry.brokerTitleShort:
            assert entry.brokerTitleShort not in text


async def test_reference_data_is_loaded_on_connect() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        await client.connect()

        assert client.deposit_asset.name == "USD"
        assert client.money_digits == 2
        assert client.assets[client.deposit_asset.assetId] == client.deposit_asset
        assert client.light_symbols["EURUSD"].symbolId == 1
        chain = await client.conversion_chain(5, 15)
        assert [s.symbolName for s in chain] == ["EURUSD"]
        assert received(server, oa.ProtoOASymbolsForConversionReq)[0].firstAssetId == 5
    finally:
        await client.disconnect()
        await server.stop()


async def test_reference_data_is_not_refetched_by_a_second_user() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        await client.connect()
        await client.connect()

        assert len(received(server, oa.ProtoOATraderReq)) == 1
        assert len(received(server, oa.ProtoOASymbolsListReq)) == 1
    finally:
        await client.disconnect()
        await client.disconnect()
        await server.stop()


async def test_symbol_specs_are_batched_and_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(account_module, "SYMBOL_BY_ID_BATCH", 2)
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        await client.connect()

        specs = await client.symbol_specs([41, 275, 279, 1])

        assert sorted(specs) == [1, 41, 275, 279]
        requests = received(server, oa.ProtoOASymbolByIdReq)
        assert [list(r.symbolId) for r in requests] == [[41, 275], [279, 1]]

        again = await client.symbol_specs([1, 41])

        assert again == {1: specs[1], 41: specs[41]}
        assert len(received(server, oa.ProtoOASymbolByIdReq)) == 2
    finally:
        await client.disconnect()
        await server.stop()


async def test_symbol_specs_refresh_bypasses_the_cache() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        await client.connect()
        await client.symbol_specs([1])
        assert len(received(server, oa.ProtoOASymbolByIdReq)) == 1

        await client.symbol_specs([1], refresh=True)

        requests = received(server, oa.ProtoOASymbolByIdReq)
        assert len(requests) == 2
        assert list(requests[-1].symbolId) == [1]
    finally:
        await client.disconnect()
        await server.stop()


async def test_request_before_connect_fails() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        with pytest.raises(CTraderConnectionError):
            await client.request(oa.ProtoOATraderReq(ctidTraderAccountId=ACCOUNT_ID))
    finally:
        await server.stop()


def test_cached_client_is_shared_per_account_and_application() -> None:
    first = get_cached_ctrader_account_client(
        trader_login=TRADER_LOGIN,
        credentials=credentials(),
        environment="auto",
        logger=RecordingLogger(),
    )
    same = get_cached_ctrader_account_client(
        trader_login=TRADER_LOGIN,
        credentials=credentials(access_token="other"),
        environment="live",
        logger=RecordingLogger(),
    )
    other_app = get_cached_ctrader_account_client(
        trader_login=TRADER_LOGIN,
        credentials=credentials(client_id="other-client"),
        environment="auto",
        logger=RecordingLogger(),
    )

    assert same is first
    assert other_app is not first


def test_a_cached_client_warns_when_another_environment_is_asked_for() -> None:
    get_cached_ctrader_account_client(
        trader_login=TRADER_LOGIN,
        credentials=credentials(token_expires_at=_EXPIRES_AT),
        environment="demo",
        logger=RecordingLogger(),
    )
    logger = RecordingLogger()

    get_cached_ctrader_account_client(
        trader_login=TRADER_LOGIN,
        credentials=credentials(token_expires_at=_EXPIRES_AT),
        environment="live",
        logger=logger,
    )

    warnings = [message for level, message in logger.lines if level == "warning"]
    assert len(warnings) == 1
    assert "'demo'" in warnings[0]
    assert "'live'" in warnings[0]
    assert str(TRADER_LOGIN) not in warnings[0]


def test_a_cached_client_warns_once_when_the_credentials_differ() -> None:
    get_cached_ctrader_account_client(
        trader_login=TRADER_LOGIN,
        credentials=credentials(token_expires_at=_EXPIRES_AT),
        environment="demo",
        logger=RecordingLogger(),
    )
    logger = RecordingLogger()

    get_cached_ctrader_account_client(
        trader_login=TRADER_LOGIN,
        credentials=credentials(
            access_token="second-access-token",
            refresh_token="second-refresh-token",
            token_expires_at=_EXPIRES_AT,
        ),
        environment="demo",
        logger=logger,
    )

    warnings = [message for level, message in logger.lines if level == "warning"]
    assert len(warnings) == 1
    assert "access token" in warnings[0] and "refresh token" in warnings[0]
    assert "client secret" not in warnings[0] and "token expiry" not in warnings[0]
    for secret in ("access-token", "refresh-token", "second-access-token", "second-refresh-token"):
        assert secret not in warnings[0]


def test_a_cached_client_stays_silent_about_a_token_it_refreshed_itself() -> None:
    """A refresh is the client's own doing, not a disagreement with the caller.

    Comparing against the live credentials would report every post-refresh call as an ignored
    setting, sending a reader after a token the caller never got wrong.
    """
    config_credentials = credentials(token_expires_at=_EXPIRES_AT)
    client = get_cached_ctrader_account_client(
        trader_login=TRADER_LOGIN,
        credentials=config_credentials,
        environment="demo",
        logger=RecordingLogger(),
    )
    client._on_tokens_refreshed("refreshed-access-token", "refreshed-refresh-token", _EXPIRES_AT)
    logger = RecordingLogger()

    get_cached_ctrader_account_client(
        trader_login=TRADER_LOGIN,
        credentials=config_credentials,
        environment="demo",
        logger=logger,
    )

    assert [message for level, message in logger.lines if level == "warning"] == []


def test_a_cached_client_for_the_same_environment_and_credentials_logs_nothing() -> None:
    get_cached_ctrader_account_client(
        trader_login=TRADER_LOGIN,
        credentials=credentials(token_expires_at=_EXPIRES_AT),
        environment="demo",
        logger=RecordingLogger(),
    )
    logger = RecordingLogger()

    get_cached_ctrader_account_client(
        trader_login=TRADER_LOGIN,
        credentials=credentials(token_expires_at=_EXPIRES_AT),
        environment="demo",
        logger=logger,
    )

    assert logger.lines == []


def test_unknown_environment_is_rejected() -> None:
    with pytest.raises(ValueError, match="environment"):
        CTraderAccountClient(
            trader_login=TRADER_LOGIN,
            credentials=credentials(),
            environment="paper",
            logger=RecordingLogger(),
        )


async def test_the_last_disconnect_closes_the_session() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        await client.connect()
        await client.connect()
        session = client.session

        await client.disconnect()

        assert client.session is session
        assert session.is_ready

        await client.disconnect()

        assert client.session is None
        assert not session.is_ready
        await wait_until(lambda: server.open_connection_count == 0, description="all closed")
    finally:
        await server.stop()


async def test_concurrent_connects_share_one_attempt() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        await asyncio.gather(client.connect(), client.connect(), client.connect())

        assert len(received(server, oa.ProtoOAGetAccountListByAccessTokenReq)) == 1
        assert len(received(server, oa.ProtoOAAccountAuthReq)) == 1

        await client.disconnect()
        await client.disconnect()
        assert client.session is not None
        await client.disconnect()
        assert client.session is None
    finally:
        await server.stop()


async def test_a_failed_connect_leaves_the_client_reusable() -> None:
    server = venue(logins=())
    await server.start()
    client = account_client(server)
    try:
        with pytest.raises(CTraderAuthError):
            await client.connect()

        server.on(
            oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ,
            lambda _r: account_list(is_live=True),
        )
        await client.connect()

        assert client.session is not None and client.session.is_ready
        await client.disconnect()
        assert client.session is None
    finally:
        await server.stop()


async def test_a_cancelled_sole_connect_stops_the_attempt() -> None:
    server = venue()
    server.on(oa_model.PROTO_OA_ACCOUNT_AUTH_REQ, lambda _r: None)
    await server.start()
    client = account_client(server, environment="demo")
    try:
        task = asyncio.create_task(client.connect())
        await wait_until(lambda: received(server, oa.ProtoOAAccountAuthReq), description="auth")
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert client.session is None
        await wait_until(lambda: server.open_connection_count == 0, description="all closed")
    finally:
        await server.stop()


async def test_failed_restores_are_retried_while_ready() -> None:
    server = venue()
    await server.start()
    client = account_client(server, restore_retry_interval_secs=0.05)
    try:
        await client.connect()
        session = client.session
        attempts = []

        async def flaky() -> None:
            attempts.append(1)
            if len(attempts) < 2:
                raise RuntimeError("rejected")

        session.add_restore("flaky", flaky)
        session._failed_restores.add("flaky")

        await wait_until(lambda: not session.failed_restores, description="restore retried")
        assert len(attempts) == 2
    finally:
        await client.disconnect()
        await server.stop()


async def test_a_persistently_failing_restore_logs_one_error() -> None:
    server = venue()
    await server.start()
    logger = RecordingLogger()
    client = account_client(server, logger=logger, restore_retry_interval_secs=0.02)
    try:
        await client.connect()
        session = client.session
        attempts = []

        async def broken() -> None:
            attempts.append(1)
            raise RuntimeError("rejected")

        session.add_restore("broken", broken)
        session._failed_restores.add("broken")

        await wait_until(lambda: len(attempts) >= 6, description="several retries")
        assert len([e for e in logger.errors() if "broken" in e]) == 1
    finally:
        await client.disconnect()
        await server.stop()


async def test_an_exception_in_the_retry_loop_is_logged_and_the_loop_continues() -> None:
    server = venue()
    await server.start()
    logger = RecordingLogger()
    client = account_client(server, logger=logger, restore_retry_interval_secs=0.02)
    try:
        await client.connect()
        calls = []

        async def failing_retry() -> None:
            calls.append(1)
            raise RuntimeError("bug")

        client.session.retry_failed_restores = failing_retry

        await wait_until(lambda: len(calls) >= 2, description="loop survived")
        assert logger.errors()
    finally:
        await client.disconnect()
        await server.stop()


def test_credentials_repr_hides_secrets() -> None:
    sample = credentials(
        client_secret="secret-value",
        access_token="access-value",
        refresh_token="refresh-value",
    )

    text = repr(sample)

    for secret in ("secret-value", "access-value", "refresh-value"):
        assert secret not in text


async def test_two_concurrent_connects_start_one_session() -> None:
    server = venue()
    held = hold_account_auth(server)
    await server.start()
    client = account_client(server, environment="demo")
    try:
        first = asyncio.create_task(client.connect())
        second = asyncio.create_task(client.connect())
        await asyncio.wait_for(held.arrived.wait(), 2.0)
        await held.release()
        await asyncio.gather(first, second)

        # One pre-connection to resolve the login, then the one session.
        assert server.connection_count == 2
        assert client._users == 2
    finally:
        await client.disconnect()
        await client.disconnect()
        await server.stop()


async def test_a_connect_right_after_the_first_completes_reuses_the_session() -> None:
    server = venue()
    await server.start()
    client = account_client(server, environment="demo")
    try:
        await client.connect()
        session = client.session
        await client.connect()

        assert client.session is session
        # One pre-connection to resolve the login, then the one session.
        assert server.connection_count == 2
    finally:
        await client.disconnect()
        await client.disconnect()
        await server.stop()


async def test_cancelling_the_caller_running_bring_up_lets_a_queued_caller_connect() -> None:
    server = venue()
    held = hold_account_auth(server)
    await server.start()
    client = account_client(server, environment="demo")
    try:
        first = asyncio.create_task(client.connect())
        await asyncio.wait_for(held.arrived.wait(), 2.0)
        second = asyncio.create_task(client.connect())
        await asyncio.sleep(0)
        # The first attempt's account auth is never answered.
        held.pending.clear()
        held.arrived.clear()
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first

        await asyncio.wait_for(held.arrived.wait(), 2.0)
        await held.release()
        await second

        assert client._users == 1
        assert client.session is not None and client.session.is_ready
        await wait_until(lambda: server.open_connection_count == 1, description="one session")
    finally:
        await client.disconnect()
        await server.stop()


async def test_cancelling_a_queued_caller_leaves_the_first_connected() -> None:
    server = venue()
    held = hold_account_auth(server)
    await server.start()
    client = account_client(server, environment="demo")
    try:
        first = asyncio.create_task(client.connect())
        await asyncio.wait_for(held.arrived.wait(), 2.0)
        second = asyncio.create_task(client.connect())
        await asyncio.sleep(0)
        second.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second

        await held.release()
        await first

        assert client._users == 1
        # One pre-connection to resolve the login, then the one session.
        assert server.connection_count == 2
    finally:
        await client.disconnect()
        await server.stop()


async def test_disconnect_stops_the_session_only_after_the_last_user() -> None:
    server = venue()
    await server.start()
    client = account_client(server, environment="demo")
    try:
        await client.connect()
        await client.connect()
        session = client.session

        await client.disconnect()
        assert session.is_ready
        assert server.open_connection_count == 1

        await client.disconnect()
        assert not session.is_ready
        await wait_until(lambda: server.open_connection_count == 0, description="closed")
    finally:
        await server.stop()


@pytest.mark.parametrize("extra_ticks", [0, 1, 2, 3])
async def test_a_connect_landing_as_the_first_attempt_finishes_starts_no_second_session(
    extra_ticks: int,
) -> None:
    server = venue()
    held = hold_account_auth(server)
    await server.start()
    client = account_client(server, environment="demo")
    late = None

    async def connect_as_soon_as_ready() -> None:
        # Checked on every loop tick, then a few more ticks, so the call lands in each of the
        # ticks between bring-up finishing and the first caller counting itself.
        # A bare `sleep(0)` per tick rather than an Event: the timing must be exact to the tick.
        while True:
            if client._users > 0 or client._money_digits is not None:
                break
            await asyncio.sleep(0)
        for _ in range(extra_ticks):
            await asyncio.sleep(0)
        await client.connect()

    try:
        first = asyncio.create_task(client.connect())
        await asyncio.wait_for(held.arrived.wait(), 2.0)
        late = asyncio.create_task(connect_as_soon_as_ready())
        await held.release()
        await asyncio.wait_for(asyncio.gather(first, late), 5.0)

        # One pre-connection to resolve the login, then the one session.
        assert server.connection_count == 2
        assert client._users == 2
    finally:
        if late is not None and not late.done():
            late.cancel()
        await client.disconnect()
        await client.disconnect()
        await server.stop()


async def test_a_cancelled_disconnect_still_stops_the_session() -> None:
    server = venue()
    await server.start()
    client = account_client(server, environment="demo")
    try:
        await client.connect()

        # A retry task whose shutdown dawdles, so the disconnect can be cancelled mid-way.
        async def slow_to_stop() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.sleep(0.5)

        client._retry_task.cancel()
        client._retry_task = asyncio.create_task(slow_to_stop())
        await asyncio.sleep(0)

        disconnecting = asyncio.create_task(client.disconnect())
        await asyncio.sleep(0.05)
        disconnecting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await disconnecting

        await wait_until(lambda: server.open_connection_count == 0, description="session closed")

        await client.connect()
        assert client.session is not None and client.session.is_ready
    finally:
        await client.disconnect()
        await server.stop()


@pytest.mark.parametrize(
    "unanswered",
    [oa_model.PROTO_OA_APPLICATION_AUTH_REQ, oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ],
)
async def test_connect_timeout_bounds_the_whole_connect(unanswered: int) -> None:
    server = venue()
    server.on(unanswered, lambda _r: None)
    await server.start()
    client = account_client(server, connect_timeout_secs=0.5)
    try:
        started = time.monotonic()
        with pytest.raises(CTraderTimeoutError, match=r"connect did not complete within 0\.5s"):
            await client.connect()

        assert time.monotonic() - started < 1.5
        assert client.session is None
        await wait_until(lambda: server.open_connection_count == 0, description="all closed")
    finally:
        await server.stop()


# -- A later user joining a session that is not ready ---------------------------------------


async def _connect_then_lose_the_connection(
    server,
    client: CTraderAccountClient,
    monkeypatch: pytest.MonkeyPatch,
) -> HeldReplies:
    """Connect the first user, then drop the connection and hold the reconnect's account auth."""
    # Reconnect at once rather than after a backoff.
    monkeypatch.setattr("nautilus_ctrader.common.session.STABLE_SESSION_SECS", 0.0)
    held = hold_account_auth(server, answer_first=1)
    await client.connect()
    await server.drop_connections()
    await asyncio.wait_for(held.arrived.wait(), 5.0)
    assert client.session.state is SessionState.AUTHENTICATING
    return held


async def test_a_later_user_connecting_during_a_reconnect_waits_until_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = venue()
    await server.start()
    client = account_client(server, environment="demo")
    later = None
    try:
        held = await _connect_then_lose_the_connection(server, client, monkeypatch)

        later = asyncio.create_task(client.connect())
        await asyncio.sleep(0.1)
        assert not later.done()

        await held.release()
        await asyncio.wait_for(later, 5.0)

        response = await client.request(oa.ProtoOATraderReq(ctidTraderAccountId=ACCOUNT_ID))
        assert isinstance(response, oa.ProtoOATraderRes)
        assert client._users == 2
    finally:
        if later is not None and not later.done():
            later.cancel()
        await client.disconnect()
        await client.disconnect()
        await server.stop()


async def test_a_later_user_whose_wait_times_out_is_not_counted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = venue()
    await server.start()
    client = account_client(server, environment="demo", connect_timeout_secs=0.5)
    try:
        await _connect_then_lose_the_connection(server, client, monkeypatch)

        started = time.monotonic()
        with pytest.raises(CTraderTimeoutError, match=r"connect did not complete within 0\.5s"):
            await asyncio.wait_for(client.connect(), 5.0)
        assert time.monotonic() - started < 1.5
        assert client._users == 1

        await client.disconnect()
        assert client.session is None
        await wait_until(lambda: server.open_connection_count == 0, description="all closed")
    finally:
        await client.disconnect()
        await server.stop()


async def test_a_later_user_fails_at_once_when_the_reconnect_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("nautilus_ctrader.common.session.STABLE_SESSION_SECS", 0.0)
    server = venue()
    await server.start()
    client = account_client(server, environment="demo", connect_timeout_secs=30.0)
    try:
        await client.connect()
        server.on(
            oa_model.PROTO_OA_ACCOUNT_AUTH_REQ,
            lambda _r: oa.ProtoOAErrorRes(errorCode="RET_ACCOUNT_DISABLED"),
        )
        await server.drop_connections()
        await wait_until(lambda: not client.session.is_ready, description="loss noticed")

        started = time.monotonic()
        with pytest.raises(CTraderAuthError, match="RET_ACCOUNT_DISABLED"):
            await asyncio.wait_for(client.connect(), 5.0)
        assert time.monotonic() - started < 2.0
        assert client._users == 1

        await client.disconnect()
        assert client.session is None
        await wait_until(lambda: server.open_connection_count == 0, description="all closed")
    finally:
        await client.disconnect()
        await server.stop()


async def test_a_later_user_cancelled_while_waiting_is_not_counted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = venue()
    await server.start()
    client = account_client(server, environment="demo")
    try:
        await _connect_then_lose_the_connection(server, client, monkeypatch)

        later = asyncio.create_task(client.connect())
        await asyncio.sleep(0.1)
        assert not later.done()
        later.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(later, 5.0)
        assert client._users == 1

        await client.disconnect()
        assert client.session is None
        await wait_until(lambda: server.open_connection_count == 0, description="all closed")
    finally:
        await client.disconnect()
        await server.stop()


async def test_a_later_user_of_a_ready_session_neither_waits_nor_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = venue()
    await server.start()
    client = account_client(server, environment="demo")
    try:
        await client.connect()
        session = client.session

        async def no_wait(timeout_secs: float | None = None) -> None:
            raise AssertionError("a ready session was waited on")

        monkeypatch.setattr(session, "wait_ready", no_wait)
        before = len(server.received)

        await client.connect()

        assert len(server.received) == before
        assert client._users == 2
    finally:
        await client.disconnect()
        await client.disconnect()
        await server.stop()


# -- The instrument provider ----------------------------------------------------------------


def _instrument_provider(client: CTraderAccountClient, **overrides) -> CTraderInstrumentProvider:
    settings = {
        "config": InstrumentProviderConfig(load_ids=frozenset({GER40_ID})),
        "asset_class_overrides": {},
        "fail_on_instrument_error": False,
    }
    settings.update(overrides)
    return client.get_instrument_provider(**settings, logger=RecordingLogger())


def test_the_account_builds_one_instrument_provider() -> None:
    client = account_client(venue())

    first = _instrument_provider(client)

    assert _instrument_provider(client) is first
    assert client.instrument_provider is first


@pytest.mark.parametrize(
    ("setting", "overrides"),
    [
        ("instrument_provider", {"config": InstrumentProviderConfig(load_all=True)}),
        ("asset_class_overrides", {"asset_class_overrides": {"US100.cash": AssetClass.INDEX}}),
        ("fail_on_instrument_error", {"fail_on_instrument_error": True}),
    ],
)
def test_other_instrument_settings_for_the_same_account_are_refused(
    setting: str,
    overrides: dict,
) -> None:
    client = account_client(venue())
    _instrument_provider(client)

    with pytest.raises(ValueError, match=setting) as raised:
        _instrument_provider(client, **overrides)

    others = {"instrument_provider", "asset_class_overrides", "fail_on_instrument_error"}
    message = str(raised.value)
    assert not any(other in message for other in others - {setting})
    assert str(TRADER_LOGIN) not in message


async def test_the_account_bring_up_re_queries_conversion_chains_and_a_reconnect_does_not(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("nautilus_ctrader.common.session.STABLE_SESSION_SECS", 0.0)
    monkeypatch.setattr("nautilus_ctrader.common.session.BACKOFF_BASE_SECS", 0.01)
    server = venue()
    await server.start()
    client = account_client(server)
    provider = _instrument_provider(client)

    async def resolve_chain() -> int:
        await provider.conversion_instruments_for(provider.find(GER40_ID))
        return len(received(server, oa.ProtoOASymbolsForConversionReq))

    try:
        await client.connect()
        await provider.initialize()
        assert await resolve_chain() == 1

        await server.drop_connections()
        await wait_until(
            lambda: server.connection_count >= 3 and client.session.is_ready,
            timeout_secs=10.0,
            description="session reconnected on its own",
        )
        assert await resolve_chain() == 1

        await client.disconnect()
        await client.connect()
        assert await resolve_chain() == 2
    finally:
        await client.disconnect()
        await server.stop()


# -- Symbol changes -------------------------------------------------------------------------

GER40_SYMBOL_ID = 279
# Not in the recorded symbol list, so its reload fails without a request.
UNOFFERED_SYMBOL_ID = 999_999


def _symbol_changed(symbol_id: int) -> oa.ProtoOASymbolChangedEvent:
    return oa.ProtoOASymbolChangedEvent(ctidTraderAccountId=ACCOUNT_ID, symbolId=[symbol_id])


def _symbol_warnings(logger: RecordingLogger) -> list[str]:
    return [m for level, m in logger.lines if level == "warning" and "Symbol changed" in m]


async def _round_trip(client: CTraderAccountClient) -> None:
    """Return once every event pushed before it has been dispatched, and its requests sent."""
    await client.request(oa.ProtoOATraderReq(ctidTraderAccountId=ACCOUNT_ID))


async def test_the_account_reloads_a_changed_symbol_once_and_notifies_every_listener() -> None:
    server = venue()
    await server.start()
    logger = RecordingLogger()
    client = account_client(server, logger=logger)
    provider = _instrument_provider(client)
    first: list = []
    second: list = []
    client.add_reload_listener(first.append)
    client.add_reload_listener(second.append)
    try:
        await client.connect()
        await provider.initialize()
        requests = len(received(server, oa.ProtoOASymbolByIdReq))

        await server.push(_symbol_changed(GER40_SYMBOL_ID))
        await wait_until(lambda: first and second, description="both listeners notified")
        await _round_trip(client)

        assert [i.id for i in first] == [GER40_ID]
        assert second == first
        assert provider.find(GER40_ID) is first[0]
        assert len(received(server, oa.ProtoOASymbolByIdReq)) == requests + 1
        assert _symbol_warnings(logger) == ["Symbol changed at the venue: GER40.cash; reloading"]
    finally:
        await client.disconnect()
        await server.stop()


async def test_a_raising_reload_listener_neither_stops_the_others_nor_loses_the_reload() -> None:
    server = venue()
    await server.start()
    logger = RecordingLogger()
    client = account_client(server, logger=logger)
    provider = _instrument_provider(client)
    notified: list = []

    def raising(_instrument) -> None:
        raise RuntimeError("listener bug")

    client.add_reload_listener(raising)
    client.add_reload_listener(notified.append)
    try:
        await client.connect()
        await provider.initialize()

        await server.push(_symbol_changed(GER40_SYMBOL_ID))
        await wait_until(lambda: notified, description="the second listener notified")

        assert provider.find(GER40_ID) is notified[0]
        assert [e for e in logger.errors() if "Reload listener" in e] == [
            f"Reload listener raised for {GER40_ID}",
        ]
    finally:
        await client.disconnect()
        await server.stop()


async def test_a_removed_reload_listener_is_not_notified() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    provider = _instrument_provider(client)
    removed: list = []
    kept: list = []
    client.add_reload_listener(removed.append)
    client.add_reload_listener(kept.append)
    client.remove_reload_listener(removed.append)
    try:
        await client.connect()
        await provider.initialize()

        await server.push(_symbol_changed(GER40_SYMBOL_ID))
        await wait_until(lambda: kept, description="the kept listener notified")
        await _round_trip(client)

        assert not removed
    finally:
        await client.disconnect()
        await server.stop()


async def test_a_failed_reload_is_reported_once_and_notifies_nobody() -> None:
    server = venue()
    await server.start()
    logger = RecordingLogger()
    client = account_client(server, logger=logger)
    provider = _instrument_provider(client)
    notified: list = []
    client.add_reload_listener(notified.append)
    try:
        await client.connect()
        await provider.initialize()

        await server.push(_symbol_changed(UNOFFERED_SYMBOL_ID))
        await wait_until(lambda: logger.errors(), description="the failure reported")
        await _round_trip(client)

        errors = logger.errors()
        assert len(errors) == 1
        assert errors[0].startswith(f"Reload of symbol id {UNOFFERED_SYMBOL_ID} failed: ")
        assert not notified
    finally:
        await client.disconnect()
        await server.stop()


async def test_symbol_changes_are_handled_after_a_reconnect_and_after_a_new_bring_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("nautilus_ctrader.common.session.STABLE_SESSION_SECS", 0.0)
    monkeypatch.setattr("nautilus_ctrader.common.session.BACKOFF_BASE_SECS", 0.01)
    server = venue()
    await server.start()
    client = account_client(server)
    provider = _instrument_provider(client)
    notified: list = []
    client.add_reload_listener(notified.append)
    try:
        await client.connect()
        await provider.initialize()

        await server.drop_connections()
        await wait_until(
            lambda: server.connection_count >= 3 and client.session.is_ready,
            timeout_secs=10.0,
            description="session reconnected on its own",
        )
        await server.push(_symbol_changed(GER40_SYMBOL_ID))
        await wait_until(lambda: len(notified) == 1, description="handled after a reconnect")

        await client.disconnect()
        await client.connect()
        requests = len(received(server, oa.ProtoOASymbolByIdReq))
        await server.push(_symbol_changed(GER40_SYMBOL_ID))
        await wait_until(lambda: len(notified) == 2, description="handled after a bring-up")
        await _round_trip(client)

        assert len(notified) == 2
        assert len(received(server, oa.ProtoOASymbolByIdReq)) == requests + 1
    finally:
        await client.disconnect()
        await server.stop()
