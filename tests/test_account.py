"""The per-account client: host selection, session ownership, reference data."""

import asyncio
import time

import pytest
from google.protobuf.message import Message

from nautilus_ctrader.common import account as account_module
from nautilus_ctrader.common.account import (
    AccountCredentials,
    CTraderAccountClient,
    get_cached_ctrader_account_client,
)
from nautilus_ctrader.common.errors import (
    CTraderAuthError,
    CTraderConnectionError,
    CTraderTimeoutError,
)
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from tests.fake_server import FakeCTraderServer
from tests.fixtures import FAKE_ACCOUNT_ID, load_recorded
from tests.polling import wait_until
from tests.recording_logger import RecordingLogger

ACCOUNT_ID = 7654321
RECORDED = load_recorded()


@pytest.fixture(autouse=True)
def _clear_account_cache():
    account_module._clear_account_cache()
    yield
    account_module._clear_account_cache()


def _for_account(message: Message, account_id: int) -> Message:
    """A copy of a recorded reply, re-addressed to `account_id`."""
    copy = type(message)()
    copy.CopyFrom(message)
    copy.ctidTraderAccountId = account_id
    if isinstance(copy, oa.ProtoOATraderRes):
        copy.trader.ctidTraderAccountId = account_id
    return copy


def _account_list(
    *, is_live: bool, listed: bool = True
) -> oa.ProtoOAGetAccountListByAccessTokenRes:
    res = oa.ProtoOAGetAccountListByAccessTokenRes()
    res.CopyFrom(RECORDED["account_list"][0])
    del res.ctidTraderAccount[:]
    if listed:
        for recorded in RECORDED["account_list"][0].ctidTraderAccount:
            entry = res.ctidTraderAccount.add()
            entry.CopyFrom(recorded)
            assert entry.ctidTraderAccountId == FAKE_ACCOUNT_ID
            entry.ctidTraderAccountId = ACCOUNT_ID
            entry.isLive = is_live
    return res


def _symbol_by_id(request: oa.ProtoOASymbolByIdReq) -> oa.ProtoOASymbolByIdRes:
    wanted = set(request.symbolId)
    recorded = RECORDED["symbol_specs"][0]
    return oa.ProtoOASymbolByIdRes(
        ctidTraderAccountId=request.ctidTraderAccountId,
        symbol=[s for s in recorded.symbol if s.symbolId in wanted],
    )


def _venue(*, is_live: bool = True, listed: bool = True) -> FakeCTraderServer:
    """A server that authenticates, lists the account, and serves recorded reference data."""
    server = FakeCTraderServer()
    server.on(
        oa_model.PROTO_OA_APPLICATION_AUTH_REQ,
        lambda _r: oa.ProtoOAApplicationAuthRes(),
    )
    server.on(
        oa_model.PROTO_OA_ACCOUNT_AUTH_REQ,
        lambda r: oa.ProtoOAAccountAuthRes(ctidTraderAccountId=r.ctidTraderAccountId),
    )
    server.on(
        oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ,
        lambda _r: _account_list(is_live=is_live, listed=listed),
    )
    for payload_type, key in (
        (oa_model.PROTO_OA_TRADER_REQ, "trader"),
        (oa_model.PROTO_OA_ASSET_LIST_REQ, "assets"),
        (oa_model.PROTO_OA_SYMBOLS_LIST_REQ, "symbols"),
        (oa_model.PROTO_OA_SYMBOLS_FOR_CONVERSION_REQ, "conversion_eur_usd"),
    ):
        server.on(
            payload_type,
            lambda r, key=key: _for_account(RECORDED[key][0], r.ctidTraderAccountId),
        )
    server.on(oa_model.PROTO_OA_SYMBOL_BY_ID_REQ, _symbol_by_id)
    return server


def _credentials(**overrides) -> AccountCredentials:
    values = {
        "client_id": "client-id",
        "client_secret": "client-secret",
        "access_token": "access-token",
        "refresh_token": "refresh-token",
        "token_expires_at": time.time() + 3600.0,
    }
    values.update(overrides)
    return AccountCredentials(**values)


def _client(
    server: FakeCTraderServer,
    *,
    environment: str = "auto",
    logger: RecordingLogger | None = None,
    **kwargs,
) -> CTraderAccountClient:
    kwargs.setdefault("credentials", _credentials())
    return CTraderAccountClient(
        account_id=ACCOUNT_ID,
        environment=environment,
        logger=RecordingLogger() if logger is None else logger,
        demo_host=server.host,
        live_host=server.host,
        port=server.port,
        tls=False,
        **kwargs,
    )


def _received(server: FakeCTraderServer, cls: type[Message]) -> list[Message]:
    return [m for m in server.received if isinstance(m, cls)]


async def test_auto_connects_to_the_listed_account_and_closes_the_pre_connection() -> None:
    server = _venue()
    await server.start()
    client = _client(server)
    try:
        await client.connect()

        response = await client.request(oa.ProtoOATraderReq(ctidTraderAccountId=ACCOUNT_ID))

        assert isinstance(response, oa.ProtoOATraderRes)
        assert server.connection_count == 2
        await wait_until(lambda: server.open_connection_count == 1, description="pre-conn closed")
        assert _received(server, oa.ProtoOAGetAccountListByAccessTokenReq)[0].accessToken == (
            "access-token"
        )
    finally:
        await client.disconnect()
        await server.stop()


@pytest.mark.parametrize(("is_live", "expected"), [(True, "live.invalid"), (False, "127.0.0.1")])
async def test_auto_picks_the_host_from_the_is_live_flag(
    monkeypatch: pytest.MonkeyPatch,
    is_live: bool,
    expected: str,
) -> None:
    hosts = []
    real_session = account_module.CTraderSession

    server = _venue(is_live=is_live)

    # The live name does not resolve; the session is pointed back at the one fake server.
    def recording_session(**kwargs):
        hosts.append(kwargs["host"])
        return real_session(**{**kwargs, "host": server.host})

    monkeypatch.setattr(account_module, "CTraderSession", recording_session)
    await server.start()
    client = CTraderAccountClient(
        account_id=ACCOUNT_ID,
        credentials=_credentials(),
        environment="auto",
        logger=RecordingLogger(),
        demo_host=server.host,
        live_host="live.invalid",
        port=server.port,
        tls=False,
    )
    try:
        await client.connect()

        assert hosts == [expected]
    finally:
        await client.disconnect()
        await server.stop()


async def test_auto_rejects_an_account_the_token_does_not_grant() -> None:
    server = _venue(listed=False)
    await server.start()
    client = _client(server)
    try:
        with pytest.raises(CTraderAuthError, match="account not granted to this token") as info:
            await client.connect()

        assert str(ACCOUNT_ID) not in str(info.value)
        assert "login" not in str(info.value).lower()
        assert client.session is None
        await wait_until(lambda: server.open_connection_count == 0, description="all closed")
    finally:
        await server.stop()


async def test_auto_refreshes_a_rejected_token_once_and_notifies_listeners() -> None:
    server = _venue()
    calls = []

    def account_list(request: oa.ProtoOAGetAccountListByAccessTokenReq) -> Message:
        calls.append(request.accessToken)
        if request.accessToken == "access-token":
            return oa.ProtoOAErrorRes(errorCode="CH_ACCESS_TOKEN_INVALID")
        return _account_list(is_live=True)

    server.on(oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ, account_list)
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
    client = _client(server)
    notified = []
    client.add_token_listener(lambda a, r, e: notified.append((a, r, e)))
    try:
        before = time.time()
        await client.connect()

        assert calls == ["access-token", "new-access"]
        assert [r.refreshToken for r in _received(server, oa.ProtoOARefreshTokenReq)] == [
            "refresh-token",
        ]
        assert len(notified) == 1
        access, refresh, expires_at = notified[0]
        assert (access, refresh) == ("new-access", "new-refresh")
        assert before + 3600 <= expires_at <= time.time() + 3600
        # The session is built with the refreshed token.
        assert _received(server, oa.ProtoOAAccountAuthReq)[0].accessToken == "new-access"
    finally:
        await client.disconnect()
        await server.stop()


async def test_auto_without_a_refresh_token_fails_on_a_rejected_token() -> None:
    server = _venue()
    server.on(
        oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="CH_ACCESS_TOKEN_INVALID"),
    )
    await server.start()
    client = _client(server, credentials=_credentials(refresh_token=None))
    try:
        with pytest.raises(CTraderAuthError, match="no refresh token"):
            await client.connect()
        assert not _received(server, oa.ProtoOARefreshTokenReq)
    finally:
        await server.stop()


@pytest.mark.parametrize("environment", ["auto", "demo"])
async def test_wrong_client_secret_fails_fast(environment: str) -> None:
    server = _venue()
    server.on(
        oa_model.PROTO_OA_APPLICATION_AUTH_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="CH_CLIENT_AUTH_FAILURE"),
    )
    await server.start()
    client = _client(server, environment=environment, connect_timeout_secs=30.0)
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
    server = _venue()
    server.on(
        oa_model.PROTO_OA_ACCOUNT_AUTH_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="CANT_ROUTE_REQUEST"),
    )
    await server.start()
    client = _client(server, environment="live", connect_timeout_secs=30.0)
    try:
        started = time.monotonic()
        with pytest.raises(CTraderAuthError, match="use 'auto'") as info:
            await client.connect()
        assert time.monotonic() - started < 2.0
        assert str(ACCOUNT_ID) not in str(info.value)
        assert not _received(server, oa.ProtoOAGetAccountListByAccessTokenReq)
        await wait_until(lambda: server.open_connection_count == 0, description="all closed")
    finally:
        await server.stop()


async def test_a_session_that_never_becomes_ready_times_out() -> None:
    server = _venue()
    server.on(oa_model.PROTO_OA_ACCOUNT_AUTH_REQ, lambda _r: None)
    await server.start()
    client = _client(server, environment="demo", connect_timeout_secs=0.6)
    try:
        with pytest.raises(CTraderTimeoutError):
            await client.connect()
        assert client.session is None
        await wait_until(lambda: server.open_connection_count == 0, description="all closed")
    finally:
        await server.stop()


async def test_missing_token_expiry_logs_a_warning() -> None:
    server = _venue()
    await server.start()
    logger = RecordingLogger()
    client = _client(server, logger=logger, credentials=_credentials(token_expires_at=None))
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
    server = _venue()
    await server.start()
    logger = RecordingLogger()
    client = _client(server, logger=logger)
    try:
        await client.connect()
    finally:
        await client.disconnect()
        await server.stop()

    text = "\n".join(message for _level, message in logger.lines)
    assert str(ACCOUNT_ID) not in text
    for entry in RECORDED["account_list"][0].ctidTraderAccount:
        assert str(entry.traderLogin) not in text
        if entry.brokerTitleShort:
            assert entry.brokerTitleShort not in text


async def test_reference_data_is_loaded_on_connect() -> None:
    server = _venue()
    await server.start()
    client = _client(server)
    try:
        await client.connect()

        assert client.deposit_asset.name == "USD"
        assert client.money_digits == 2
        assert client.assets[client.deposit_asset.assetId] == client.deposit_asset
        assert client.light_symbols["EURUSD"].symbolId == 1
        chain = await client.conversion_chain(5, 15)
        assert [s.symbolName for s in chain] == ["EURUSD"]
        assert _received(server, oa.ProtoOASymbolsForConversionReq)[0].firstAssetId == 5
    finally:
        await client.disconnect()
        await server.stop()


async def test_reference_data_is_not_refetched_by_a_second_user() -> None:
    server = _venue()
    await server.start()
    client = _client(server)
    try:
        await client.connect()
        await client.connect()

        assert len(_received(server, oa.ProtoOATraderReq)) == 1
        assert len(_received(server, oa.ProtoOASymbolsListReq)) == 1
    finally:
        await client.disconnect()
        await client.disconnect()
        await server.stop()


async def test_symbol_specs_are_batched_and_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(account_module, "SYMBOL_BY_ID_BATCH", 2)
    server = _venue()
    await server.start()
    client = _client(server)
    try:
        await client.connect()

        specs = await client.symbol_specs([41, 275, 279, 1])

        assert sorted(specs) == [1, 41, 275, 279]
        requests = _received(server, oa.ProtoOASymbolByIdReq)
        assert [list(r.symbolId) for r in requests] == [[41, 275], [279, 1]]

        again = await client.symbol_specs([1, 41])

        assert again == {1: specs[1], 41: specs[41]}
        assert len(_received(server, oa.ProtoOASymbolByIdReq)) == 2
    finally:
        await client.disconnect()
        await server.stop()


async def test_request_before_connect_fails() -> None:
    server = _venue()
    await server.start()
    client = _client(server)
    try:
        with pytest.raises(CTraderConnectionError):
            await client.request(oa.ProtoOATraderReq(ctidTraderAccountId=ACCOUNT_ID))
    finally:
        await server.stop()


def test_cached_client_is_shared_per_account_and_application() -> None:
    first = get_cached_ctrader_account_client(
        account_id=ACCOUNT_ID,
        credentials=_credentials(),
        environment="auto",
        logger=RecordingLogger(),
    )
    same = get_cached_ctrader_account_client(
        account_id=ACCOUNT_ID,
        credentials=_credentials(access_token="other"),
        environment="live",
        logger=RecordingLogger(),
    )
    other_app = get_cached_ctrader_account_client(
        account_id=ACCOUNT_ID,
        credentials=_credentials(client_id="other-client"),
        environment="auto",
        logger=RecordingLogger(),
    )

    assert same is first
    assert other_app is not first


def test_unknown_environment_is_rejected() -> None:
    with pytest.raises(ValueError, match="environment"):
        CTraderAccountClient(
            account_id=ACCOUNT_ID,
            credentials=_credentials(),
            environment="paper",
            logger=RecordingLogger(),
        )


async def test_the_last_disconnect_closes_the_session() -> None:
    server = _venue()
    await server.start()
    client = _client(server)
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
    server = _venue()
    await server.start()
    client = _client(server)
    try:
        await asyncio.gather(client.connect(), client.connect(), client.connect())

        assert len(_received(server, oa.ProtoOAGetAccountListByAccessTokenReq)) == 1
        assert len(_received(server, oa.ProtoOAAccountAuthReq)) == 1

        await client.disconnect()
        await client.disconnect()
        assert client.session is not None
        await client.disconnect()
        assert client.session is None
    finally:
        await server.stop()


async def test_a_failed_connect_leaves_the_client_reusable() -> None:
    server = _venue(listed=False)
    await server.start()
    client = _client(server)
    try:
        with pytest.raises(CTraderAuthError):
            await client.connect()

        server.on(
            oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ,
            lambda _r: _account_list(is_live=True),
        )
        await client.connect()

        assert client.session is not None and client.session.is_ready
        await client.disconnect()
        assert client.session is None
    finally:
        await server.stop()


async def test_a_cancelled_sole_connect_stops_the_attempt() -> None:
    server = _venue()
    server.on(oa_model.PROTO_OA_ACCOUNT_AUTH_REQ, lambda _r: None)
    await server.start()
    client = _client(server, environment="demo")
    try:
        task = asyncio.create_task(client.connect())
        await wait_until(lambda: _received(server, oa.ProtoOAAccountAuthReq), description="auth")
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert client.session is None
        await wait_until(lambda: server.open_connection_count == 0, description="all closed")
    finally:
        await server.stop()


async def test_failed_restores_are_retried_while_ready() -> None:
    server = _venue()
    await server.start()
    client = _client(server, restore_retry_interval_secs=0.05)
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
    server = _venue()
    await server.start()
    logger = RecordingLogger()
    client = _client(server, logger=logger, restore_retry_interval_secs=0.02)
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
    server = _venue()
    await server.start()
    logger = RecordingLogger()
    client = _client(server, logger=logger, restore_retry_interval_secs=0.02)
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
