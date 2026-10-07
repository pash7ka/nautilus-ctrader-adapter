"""Account and symbol discovery against the fake venue: read-only, and never refreshing."""

from __future__ import annotations

import time
from decimal import Decimal

import pytest
from google.protobuf.message import Message

from nautilus_ctrader import discovery
from nautilus_ctrader.common.connection import CTraderConnection
from nautilus_ctrader.common.errors import CTraderAuthError
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from tests.account_venue import ACCOUNT_ID, TRADER_LOGIN, credentials, received, venue
from tests.fake_server import FakeCTraderServer
from tests.polling import wait_until
from tests.recording_logger import RecordingLogger

ACCESS_TOKEN = "the-access-token"
REFRESH_TOKEN = "the-refresh-token"
CLIENT_SECRET = "the-client-secret"

_READ_ONLY = (
    oa.ProtoOAApplicationAuthReq,
    oa.ProtoOAGetAccountListByAccessTokenReq,
    oa.ProtoOAAccountAuthReq,
    oa.ProtoOATraderReq,
    oa.ProtoOAAssetListReq,
    oa.ProtoOASymbolsListReq,
    oa.ProtoOASymbolByIdReq,
)

LIVE_ID, DEMO_ID, UNKNOWN_ID, REFUSED_ID = 501, 502, 503, 504


def _assert_read_only(server: FakeCTraderServer) -> None:
    sent = {type(m) for m in server.received}
    assert sent <= set(_READ_ONLY), sent - set(_READ_ONLY)


def _granted(*entries: oa_model.ProtoOACtidTraderAccount) -> Message:
    return oa.ProtoOAGetAccountListByAccessTokenRes(
        accessToken=ACCESS_TOKEN,
        permissionScope=oa_model.SCOPE_TRADE,
        ctidTraderAccount=list(entries),
    )


def _four_accounts() -> Message:
    return _granted(
        oa_model.ProtoOACtidTraderAccount(
            ctidTraderAccountId=LIVE_ID,
            isLive=True,
            traderLogin=11,
            brokerTitleShort="Broker A",
        ),
        oa_model.ProtoOACtidTraderAccount(
            ctidTraderAccountId=DEMO_ID, isLive=False, traderLogin=12
        ),
        # No isLive and no traderLogin: both unknown.
        oa_model.ProtoOACtidTraderAccount(ctidTraderAccountId=UNKNOWN_ID),
        oa_model.ProtoOACtidTraderAccount(
            ctidTraderAccountId=REFUSED_ID, isLive=True, traderLogin=14
        ),
    )


def _route_by_host(monkeypatch: pytest.MonkeyPatch, server: FakeCTraderServer) -> list:
    """Point every discovery connection at `server`, recording `(host, payload)` per request.

    The hosts named to discovery are never resolved, so the recording is the only evidence of
    which host a request was meant for.
    """
    sent: list[tuple[str, Message]] = []

    class _Routed(CTraderConnection):
        def __init__(self, host: str, port: int, **kwargs) -> None:
            super().__init__(server.host, port, **kwargs)
            self.requested_host = host

        async def request(self, payload: Message, **kwargs) -> Message:
            sent.append((self.requested_host, payload))
            return await super().request(payload, **kwargs)

    monkeypatch.setattr(discovery, "CTraderConnection", _Routed)
    return sent


async def _list_accounts(server: FakeCTraderServer, **kwargs) -> list[discovery.GrantedAccount]:
    kwargs.setdefault("demo_host", "demo.invalid")
    kwargs.setdefault("live_host", "live.invalid")
    return await discovery.list_accounts(
        "client-id",
        CLIENT_SECRET,
        ACCESS_TOKEN,
        port=server.port,
        tls=False,
        **kwargs,
    )


# --------------------------------------------------------------------------------------
# list_accounts
# --------------------------------------------------------------------------------------


async def test_list_accounts_names_each_deposit_on_the_host_its_live_flag_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = venue()
    server.on(oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ, lambda _r: _four_accounts())
    server.on(
        oa_model.PROTO_OA_ACCOUNT_AUTH_REQ,
        lambda r: (
            oa.ProtoOAErrorRes(errorCode="RET_ACCOUNT_DISABLED", description="disabled")
            if r.ctidTraderAccountId == REFUSED_ID
            else oa.ProtoOAAccountAuthRes(ctidTraderAccountId=r.ctidTraderAccountId)
        ),
    )
    sent = _route_by_host(monkeypatch, server)
    await server.start()
    try:
        accounts = await _list_accounts(server)
        await wait_until(lambda: server.open_connection_count == 0, description="all closed")
    finally:
        await server.stop()

    assert accounts == [
        discovery.GrantedAccount(11, LIVE_ID, True, "Broker A", "USD", None),
        discovery.GrantedAccount(12, DEMO_ID, False, None, "USD", None),
        discovery.GrantedAccount(None, UNKNOWN_ID, None, None, "USD", None),
        discovery.GrantedAccount(14, REFUSED_ID, True, None, None, "RET_ACCOUNT_DISABLED"),
    ]
    auth_hosts = {
        payload.ctidTraderAccountId: host
        for host, payload in sent
        if isinstance(payload, oa.ProtoOAAccountAuthReq)
    }
    # An unknown flag goes to the demo host, as for the account client.
    assert auth_hosts == {
        LIVE_ID: "live.invalid",
        DEMO_ID: "demo.invalid",
        UNKNOWN_ID: "demo.invalid",
        REFUSED_ID: "live.invalid",
    }
    list_hosts = [h for h, p in sent if isinstance(p, oa.ProtoOAGetAccountListByAccessTokenReq)]
    assert list_hosts == ["demo.invalid"]
    # One connection per host, each authenticated once.
    assert server.connection_count == 2
    assert len(received(server, oa.ProtoOAApplicationAuthReq)) == 2
    _assert_read_only(server)


async def test_list_accounts_with_no_granted_account_returns_an_empty_list() -> None:
    server = venue()
    server.on(oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ, lambda _r: _granted())
    await server.start()
    try:
        accounts = await _list_accounts(server, demo_host=server.host, live_host=server.host)
    finally:
        await server.stop()

    assert accounts == []
    assert server.connection_count == 1


async def test_list_accounts_reports_a_rejected_application_without_secrets() -> None:
    server = venue()
    server.on(
        oa_model.PROTO_OA_APPLICATION_AUTH_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="CH_CLIENT_AUTH_FAILURE", description="bad"),
    )
    await server.start()
    try:
        with pytest.raises(CTraderAuthError, match="CH_CLIENT_AUTH_FAILURE") as info:
            await _list_accounts(server, demo_host=server.host, live_host=server.host)
        await wait_until(lambda: server.open_connection_count == 0, description="all closed")
    finally:
        await server.stop()

    assert CLIENT_SECRET not in str(info.value)


@pytest.mark.parametrize("code", ["CH_ACCESS_TOKEN_INVALID", "OA_AUTH_TOKEN_EXPIRED"])
async def test_list_accounts_never_refreshes_a_rejected_token(code: str) -> None:
    server = venue()
    server.on(
        oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode=code, description=f"token {ACCESS_TOKEN}"),
    )
    logger = RecordingLogger()
    await server.start()
    try:
        with pytest.raises(CTraderAuthError, match="refresh") as info:
            await _list_accounts(
                server,
                demo_host=server.host,
                live_host=server.host,
                logger=logger,
            )
    finally:
        await server.stop()

    message = str(info.value)
    assert code in message
    assert ACCESS_TOKEN not in message
    assert not received(server, oa.ProtoOARefreshTokenReq)
    assert all(ACCESS_TOKEN not in line for _level, line in logger.lines)


async def test_list_accounts_reports_another_list_rejection_by_its_code() -> None:
    server = venue()
    server.on(
        oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="CH_ACCESS_DENIED"),
    )
    await server.start()
    try:
        with pytest.raises(CTraderAuthError, match="account list rejected: CH_ACCESS_DENIED"):
            await _list_accounts(server, demo_host=server.host, live_host=server.host)
    finally:
        await server.stop()


async def test_list_accounts_keeps_an_account_whose_trader_read_is_refused() -> None:
    server = venue()
    server.on(
        oa_model.PROTO_OA_TRADER_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="ACCESS_DENIED"),
    )
    await server.start()
    try:
        accounts = await _list_accounts(server, demo_host=server.host, live_host=server.host)
    finally:
        await server.stop()

    assert len(accounts) == 1
    assert accounts[0].deposit_currency is None
    assert accounts[0].refusal == "ACCESS_DENIED"


# --------------------------------------------------------------------------------------
# list_symbols
# --------------------------------------------------------------------------------------


async def _list_symbols(server: FakeCTraderServer, **kwargs) -> dict[str, discovery.SymbolInfo]:
    kwargs.setdefault(
        "credentials",
        credentials(
            access_token=ACCESS_TOKEN,
            refresh_token=REFRESH_TOKEN,
            client_secret=CLIENT_SECRET,
            # Inside the refresh margin: a session that kept the refresh token would spend it.
            token_expires_at=time.time() + 10.0,
        ),
    )
    return await discovery.list_symbols(
        kwargs.pop("credentials"),
        kwargs.pop("trader_login", TRADER_LOGIN),
        demo_host=server.host,
        live_host=server.host,
        port=server.port,
        tls=False,
        **kwargs,
    )


async def test_list_symbols_without_names_returns_the_light_list_only() -> None:
    server = venue()
    await server.start()
    try:
        symbols = await _list_symbols(server)
        await wait_until(lambda: server.open_connection_count == 0, description="all closed")
    finally:
        await server.stop()

    assert set(symbols) == {"XAUUSD", "US100.cash", "GER40.cash", "EURUSD"}
    assert symbols["EURUSD"] == discovery.SymbolInfo(
        "EURUSD", 1, True, None, None, None, None, None, None
    )
    assert symbols["XAUUSD"].symbol_id == 41
    assert not received(server, oa.ProtoOASymbolByIdReq)
    assert not received(server, oa.ProtoOARefreshTokenReq)
    _assert_read_only(server)


async def test_list_symbols_with_names_fills_digits_volumes_and_assets() -> None:
    server = venue()
    await server.start()
    try:
        symbols = await _list_symbols(server, names=["EURUSD", "XAUUSD", "NOT-LISTED", "EURUSD"])
    finally:
        await server.stop()

    # Recorded specs: EURUSD lotSize 10_000_000, minVolume and stepVolume 100_000, all in
    # hundredths of a unit; XAUUSD lotSize 10_000, minVolume and stepVolume 100.
    assert symbols == {
        "EURUSD": discovery.SymbolInfo(
            "EURUSD",
            1,
            True,
            "EUR",
            "USD",
            5,
            Decimal(1000),
            Decimal(1000),
            Decimal(100000),
        ),
        "XAUUSD": discovery.SymbolInfo(
            "XAUUSD",
            41,
            True,
            "XAU",
            "USD",
            2,
            Decimal(1),
            Decimal(1),
            Decimal(100),
        ),
    }
    by_id = received(server, oa.ProtoOASymbolByIdReq)
    assert len(by_id) == 1
    assert sorted(by_id[0].symbolId) == [1, 41]
    assert not received(server, oa.ProtoOARefreshTokenReq)
    _assert_read_only(server)


async def test_list_symbols_resolves_the_account_by_trader_login() -> None:
    server = venue()
    await server.start()
    try:
        await _list_symbols(server, names=["EURUSD"])
    finally:
        await server.stop()

    assert {m.ctidTraderAccountId for m in received(server, oa.ProtoOASymbolsListReq)} == {
        ACCOUNT_ID
    }


async def test_list_symbols_refuses_a_login_the_token_does_not_grant() -> None:
    server = venue()
    await server.start()
    try:
        with pytest.raises(CTraderAuthError, match="trader login"):
            await _list_symbols(server, trader_login=TRADER_LOGIN + 1)
    finally:
        await server.stop()


@pytest.mark.parametrize(
    "rejected",
    [oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ, oa_model.PROTO_OA_ACCOUNT_AUTH_REQ],
)
async def test_list_symbols_never_refreshes_a_rejected_token(rejected: int) -> None:
    server = venue()
    server.on(
        rejected,
        lambda _r: oa.ProtoOAErrorRes(errorCode="CH_ACCESS_TOKEN_INVALID"),
    )
    logger = RecordingLogger()
    await server.start()
    try:
        with pytest.raises(CTraderAuthError, match="refresh") as info:
            await _list_symbols(server, environment="auto", logger=logger)
        await wait_until(lambda: server.open_connection_count == 0, description="all closed")
    finally:
        await server.stop()

    assert "CH_ACCESS_TOKEN_INVALID" in str(info.value)
    assert not received(server, oa.ProtoOARefreshTokenReq)
    for secret in (ACCESS_TOKEN, REFRESH_TOKEN, CLIENT_SECRET):
        assert secret not in str(info.value)
        assert all(secret not in line for _level, line in logger.lines)
