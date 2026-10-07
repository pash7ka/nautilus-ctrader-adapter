"""Account and symbol discovery against the fake venue: read-only, and never refreshing."""

from __future__ import annotations

import time
from collections.abc import Callable
from decimal import Decimal

import pytest
from google.protobuf.message import Message

from nautilus_ctrader import discovery
from nautilus_ctrader.common.connection import CTraderConnection
from nautilus_ctrader.common.errors import (
    CTraderAccountError,
    CTraderAuthError,
    CTraderConnectionError,
    CTraderError,
    CTraderProtocolError,
    CTraderRequestError,
    CTraderTimeoutError,
)
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from tests.account_venue import (
    ACCOUNT_ID,
    RECORDED,
    TRADER_LOGIN,
    credentials,
    for_account,
    received,
    venue,
)
from tests.fake_server import FakeCTraderServer
from tests.polling import wait_until
from tests.recording_logger import RecordingLogger
from tests.secrecy import assert_secret_free

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


def _route_by_host(
    monkeypatch: pytest.MonkeyPatch,
    server: FakeCTraderServer,
    *,
    dead_hosts: frozenset[str] = frozenset(),
    garbled: Callable[[str, Message], bool] = lambda _host, _payload: False,
    request_timeout_secs: float = 5.0,
    connects: list[str] | None = None,
) -> list:
    """Point every discovery connection at `server`, recording `(host, payload)` per request.

    The hosts named to discovery are never resolved, so the recording is the only evidence of
    which host a request was meant for. A connection to one of `dead_hosts` fails to connect;
    every connection attempt's host is appended to `connects`. A request `garbled` picks
    fails with a `CTraderProtocolError`, as an undecodable answer would.
    """
    sent: list[tuple[str, Message]] = []

    class _Routed(CTraderConnection):
        def __init__(self, host: str, port: int, **kwargs) -> None:
            super().__init__(
                server.host,
                port,
                request_timeout_secs=request_timeout_secs,
                **kwargs,
            )
            self.requested_host = host

        async def connect(self) -> None:
            if connects is not None:
                connects.append(self.requested_host)
            if self.requested_host in dead_hosts:
                raise CTraderConnectionError("cannot connect")
            await super().connect()

        async def request(self, payload: Message, **kwargs) -> Message:
            sent.append((self.requested_host, payload))
            if garbled(self.requested_host, payload):
                raise CTraderProtocolError("undecodable payload")
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

    assert_secret_free(info.value, CLIENT_SECRET, ACCESS_TOKEN)


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

    assert code in str(info.value)
    assert_secret_free(info.value, ACCESS_TOKEN, CLIENT_SECRET)
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
        lambda _r: oa.ProtoOAErrorRes(
            errorCode="CH_ACCESS_TOKEN_INVALID",
            description=f"token {ACCESS_TOKEN} is not valid",
        ),
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
    assert_secret_free(info.value, ACCESS_TOKEN, REFRESH_TOKEN, CLIENT_SECRET)
    for secret in (ACCESS_TOKEN, REFRESH_TOKEN, CLIENT_SECRET):
        assert all(secret not in line for _level, line in logger.lines)


def _echoing(code: str) -> oa.ProtoOAErrorRes:
    """A refusal whose free-text description echoes what the venue was sent."""
    return oa.ProtoOAErrorRes(
        errorCode=code,
        description=f"refused {ACCESS_TOKEN} {CLIENT_SECRET} {REFRESH_TOKEN}",
    )


@pytest.mark.parametrize(
    ("rejected", "message"),
    [
        (oa_model.PROTO_OA_APPLICATION_AUTH_REQ, "application auth rejected: CH_CLIENT_AUTH"),
        (
            oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ,
            "account list rejected: CH_CLIENT_AUTH",
        ),
    ],
)
async def test_list_accounts_reports_a_rejection_by_its_code_only(
    rejected: int,
    message: str,
) -> None:
    server = venue()
    server.on(rejected, lambda _r: _echoing("CH_CLIENT_AUTH"))
    await server.start()
    try:
        with pytest.raises(CTraderAuthError) as info:
            await _list_accounts(server, demo_host=server.host, live_host=server.host)
    finally:
        await server.stop()

    assert str(info.value) == message
    assert_secret_free(info.value, ACCESS_TOKEN, CLIENT_SECRET, REFRESH_TOKEN)


async def test_list_accounts_marks_the_accounts_of_an_unreachable_host_and_tries_it_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = venue()
    server.on(oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ, lambda _r: _four_accounts())
    connects: list[str] = []
    _route_by_host(
        monkeypatch,
        server,
        dead_hosts=frozenset({"live.invalid"}),
        connects=connects,
    )
    await server.start()
    try:
        accounts = await _list_accounts(server)
    finally:
        await server.stop()

    assert [(a.ctid_trader_account_id, a.deposit_currency, a.refusal) for a in accounts] == [
        (LIVE_ID, None, "unreachable"),
        (DEMO_ID, "USD", None),
        (UNKNOWN_ID, "USD", None),
        (REFUSED_ID, None, "unreachable"),
    ]
    assert connects.count("live.invalid") == 1


async def test_list_accounts_marks_an_account_whose_request_times_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = venue()
    server.on(oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ, lambda _r: _four_accounts())
    server.on(
        oa_model.PROTO_OA_TRADER_REQ,
        lambda r: (
            None
            if r.ctidTraderAccountId == DEMO_ID
            else for_account(RECORDED["trader"][0], r.ctidTraderAccountId)
        ),
    )
    _route_by_host(monkeypatch, server, request_timeout_secs=0.3)
    await server.start()
    try:
        accounts = await _list_accounts(server)
    finally:
        await server.stop()

    refusals = {a.ctid_trader_account_id: a.refusal for a in accounts}
    assert refusals == {LIVE_ID: None, DEMO_ID: "timeout", UNKNOWN_ID: None, REFUSED_ID: None}


async def test_list_accounts_raises_when_the_host_serving_the_list_is_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = venue()
    _route_by_host(monkeypatch, server, dead_hosts=frozenset({"demo.invalid"}))
    await server.start()
    try:
        with pytest.raises(CTraderConnectionError):
            await _list_accounts(server)
    finally:
        await server.stop()


@pytest.mark.parametrize(
    ("rejected", "expected"),
    [
        (oa_model.PROTO_OA_APPLICATION_AUTH_REQ, CTraderAuthError),
        (oa_model.PROTO_OA_TRADER_REQ, CTraderRequestError),
    ],
)
async def test_list_symbols_reports_a_rejection_by_its_code_only(
    rejected: int,
    expected: type[Exception],
) -> None:
    server = venue()
    server.on(rejected, lambda _r: _echoing("SOME_REFUSAL"))
    await server.start()
    try:
        with pytest.raises(expected) as info:
            await _list_symbols(server)
    finally:
        await server.stop()

    assert "SOME_REFUSAL" in str(info.value)
    assert_secret_free(info.value, ACCESS_TOKEN, CLIENT_SECRET, REFRESH_TOKEN)


async def test_list_symbols_gives_its_caller_no_warning() -> None:
    server = venue()
    logger = RecordingLogger()
    await server.start()
    try:
        await _list_symbols(server, names=["EURUSD"], logger=logger)
    finally:
        await server.stop()

    assert logger.warnings() == []


async def test_list_symbols_refuses_a_single_str_for_names() -> None:
    with pytest.raises(TypeError, match="single str"):
        await discovery.list_symbols(credentials(), TRADER_LOGIN, names="EURUSD")


async def test_list_accounts_marks_an_account_whose_answer_is_undecodable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = venue()
    server.on(oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ, lambda _r: _four_accounts())
    _route_by_host(
        monkeypatch,
        server,
        garbled=lambda _host, payload: (
            isinstance(payload, oa.ProtoOATraderReq) and payload.ctidTraderAccountId == LIVE_ID
        ),
    )
    await server.start()
    try:
        accounts = await _list_accounts(server)
    finally:
        await server.stop()

    refusals = {a.ctid_trader_account_id: a.refusal for a in accounts}
    assert refusals == {
        LIVE_ID: "protocol error",
        DEMO_ID: None,
        UNKNOWN_ID: None,
        REFUSED_ID: None,
    }


async def test_list_accounts_marks_every_account_of_a_host_that_garbles_its_auth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = venue()
    server.on(oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ, lambda _r: _four_accounts())
    connects: list[str] = []
    _route_by_host(
        monkeypatch,
        server,
        connects=connects,
        garbled=lambda host, payload: (
            host == "live.invalid" and isinstance(payload, oa.ProtoOAApplicationAuthReq)
        ),
    )
    await server.start()
    try:
        accounts = await _list_accounts(server)
    finally:
        await server.stop()

    refusals = {a.ctid_trader_account_id: a.refusal for a in accounts}
    assert refusals == {
        LIVE_ID: "protocol error",
        DEMO_ID: None,
        UNKNOWN_ID: None,
        REFUSED_ID: "protocol error",
    }
    assert connects.count("live.invalid") == 1


def test_a_rebuilt_request_error_keeps_its_numbers_but_not_its_description() -> None:
    original = CTraderRequestError(
        "BLOCKED_PAYLOAD_TYPE",
        f"echo {ACCESS_TOKEN}",
        maintenance_end_secs=1_700_000_000,
        retry_after_secs=3,
    )

    rebuilt = discovery._without_venue_text(original)

    assert isinstance(rebuilt, CTraderRequestError)
    assert rebuilt.error_code == "BLOCKED_PAYLOAD_TYPE"
    assert rebuilt.retry_after_secs == 3
    assert rebuilt.maintenance_end_secs == 1_700_000_000
    assert_secret_free(rebuilt, ACCESS_TOKEN)


class _NoMessageAuthError(CTraderAuthError):
    def __init__(self) -> None:
        super().__init__("refused for a fixed reason")


class _NoMessageError(CTraderError):
    def __init__(self) -> None:
        super().__init__("failed for a fixed reason")


@pytest.mark.parametrize(
    ("original", "expected"),
    [
        (CTraderAuthError("account auth rejected: CODE"), CTraderAuthError),
        (CTraderConnectionError("connection closed"), CTraderConnectionError),
        (CTraderTimeoutError("no response"), CTraderTimeoutError),
        (CTraderProtocolError("bad frame"), CTraderProtocolError),
        (CTraderAccountError("not hedging"), CTraderAccountError),
        (_NoMessageAuthError(), CTraderAuthError),
        (_NoMessageError(), CTraderError),
    ],
)
def test_a_rebuilt_error_keeps_its_kind_and_message_and_drops_its_chain(
    original: CTraderError,
    expected: type[CTraderError],
) -> None:
    try:
        raise original from CTraderRequestError("CODE", f"echo {ACCESS_TOKEN}")
    except CTraderError as e:
        rebuilt = discovery._without_venue_text(e)

    assert type(rebuilt) is expected
    assert str(rebuilt) == str(original)
    assert rebuilt.__cause__ is None
    assert rebuilt.__context__ is None
