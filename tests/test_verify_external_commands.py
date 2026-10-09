"""Tests for the owner's live check of commands on orders the node did not place.

`scripts/verify_external_commands.py` changes real orders, so it never meets a broker here: the
refusals are checked with everything that could reach an account replaced, the guard and the
decisions are pure, and whole runs go against the fake venue.
"""

from __future__ import annotations

import asyncio
import builtins
import importlib.util
import pathlib
import sys
import time
from collections.abc import Callable
from decimal import Decimal
from types import SimpleNamespace

import pytest
from google.protobuf.message import Message
from nautilus_trader.model.identifiers import ClientOrderId, VenueOrderId

from nautilus_ctrader.common import order_record
from nautilus_ctrader.common.account import AccountCredentials
from nautilus_ctrader.common.connection import CTraderConnection
from nautilus_ctrader.common.errors import CTraderConnectionError
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.account_venue import ACCOUNT_ID, TRADER_LOGIN
from tests.execution_replay import make_deal, make_event, make_order, make_position
from tests.execution_venue import US100_SYMBOL_ID, ExecutionVenue
from tests.fake_server import FakeCTraderServer
from tests.polling import wait_until
from tests.recording_logger import RecordingLogger

_SCRIPT_PATH = (
    pathlib.Path(__file__).resolve().parents[1] / "scripts" / "verify_external_commands.py"
)
_SPEC = importlib.util.spec_from_file_location("verify_external_commands", _SCRIPT_PATH)
vec = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = vec
_SPEC.loader.exec_module(vec)

SYMBOL = "US100.cash"
POSITION, ENTRY, PROTECTIVE = 5_900_001, 6_900_001, 6_900_002
PENDING, PENDING_POSITION = 6_800_001, 5_800_001
# The minimum volume of US100.cash, in the venue's hundredths of a unit, and one step more.
MINIMUM, RAISED = 1, 2
EXPIRES = 4_102_444_800_000
OPENED = 1_600_000_000_000

FAKE_SECRETS = {
    "CTRADER_CLIENT_ID": "fake-client-id-8c1f",
    "CTRADER_CLIENT_SECRET": "fake-client-secret-77d2",
    "CTRADER_ACCESS_TOKEN": "fake-access-token-3a9e",
    "CTRADER_REFRESH_TOKEN": "fake-refresh-token-b410",
}
FAKE_ENV = {**FAKE_SECRETS, "CTRADER_TRADER_LOGIN": str(TRADER_LOGIN)}
CREDENTIALS = AccountCredentials(
    client_id=FAKE_SECRETS["CTRADER_CLIENT_ID"],
    client_secret=FAKE_SECRETS["CTRADER_CLIENT_SECRET"],
    access_token=FAKE_SECRETS["CTRADER_ACCESS_TOKEN"],
    refresh_token=None,
    token_expires_at=None,
)
OPENING_OR_CLOSING = (oa.ProtoOANewOrderReq, oa.ProtoOAClosePositionReq)


# -- The owner's two objects ---------------------------------------------------------------


def owner_position(opened: int) -> om.ProtoOAPosition:
    """A trader's long of the minimum volume, its stop-loss trailing."""
    position = make_position(POSITION, volume=MINIMUM, symbol=US100_SYMBOL_ID)
    position.price = 85000.0
    position.stopLoss = 84900.0
    position.takeProfit = 85100.0
    position.trailingStopLoss = True
    position.stopLossTriggerMethod = om.TRADE
    position.tradeData.openTimestamp = opened
    position.utcLastUpdateTimestamp = opened + 2
    return position


def protective_order(opened: int) -> om.ProtoOAOrder:
    order = make_order(
        PROTECTIVE,
        POSITION,
        order_type=om.STOP_LOSS_TAKE_PROFIT,
        side=om.SELL,
        closing=True,
        stop=84900.0,
        limit=85100.0,
        volume=MINIMUM,
        utc=opened + 2,
        symbol=US100_SYMBOL_ID,
    )
    order.trailingStopLoss = True
    order.timeInForce = om.GOOD_TILL_CANCEL
    return order


def pending_order(opened: int, **fields) -> om.ProtoOAOrder:
    """A trader's buy limit of the minimum volume, far below the market, levels attached."""
    order = make_order(
        PENDING,
        PENDING_POSITION,
        order_type=om.LIMIT,
        limit=84000.0,
        volume=MINIMUM,
        utc=opened + 3,
        symbol=US100_SYMBOL_ID,
    )
    order.timeInForce = om.GOOD_TILL_DATE
    order.expirationTimestamp = EXPIRES
    order.relativeStopLoss = 5_000_000
    order.relativeTakeProfit = 10_000_000
    for name, value in fields.items():
        if value is None:
            order.ClearField(name)
        else:
            setattr(order, name, value)
    return order


def position_amends(venue: ExecutionVenue):
    """The broker setting the levels and terms an amend asks for, answered as a replaced order."""

    def answer(request: oa.ProtoOAAmendPositionSLTPReq) -> Message:
        position = next(p for p in venue.snapshot.position if p.positionId == request.positionId)
        protective = next(o for o in venue.snapshot.order if o.orderId == PROTECTIVE)
        for name, field in (("stopLoss", "stopPrice"), ("takeProfit", "limitPrice")):
            if request.HasField(name):
                setattr(position, name, getattr(request, name))
                setattr(protective, field, getattr(request, name))
            else:
                position.ClearField(name)
                protective.ClearField(field)
        for name in ("trailingStopLoss", "guaranteedStopLoss", "stopLossTriggerMethod"):
            if request.HasField(name):
                setattr(position, name, getattr(request, name))
        position.utcLastUpdateTimestamp += 10
        protective.utcLastUpdateTimestamp = position.utcLastUpdateTimestamp
        event = make_event(om.ORDER_REPLACED, protective, position=position)
        event.ctidTraderAccountId = ACCOUNT_ID
        return event

    return answer


def owner_venue() -> ExecutionVenue:
    """The fake venue holding the owner's pending order and protected position, as placed."""
    venue = ExecutionVenue()
    opened = int(time.time() * 1000) - 600_000
    entry = make_order(ENTRY, POSITION, volume=MINIMUM, utc=opened, symbol=US100_SYMBOL_ID)
    entry.orderStatus = om.ORDER_STATUS_FILLED
    deal = make_deal(
        7_900_001, ENTRY, POSITION, side=om.BUY, volume=MINIMUM, price=85000.0, ts=opened
    )
    deal.symbolId = US100_SYMBOL_ID
    venue.snapshot.position.append(owner_position(opened))
    venue.snapshot.order.extend([protective_order(opened), pending_order(opened)])
    venue.position_orders = {POSITION: [entry, protective_order(opened)]}
    venue.position_deals = {POSITION: [deal]}
    venue.deals = [deal]
    venue.server.on(om.PROTO_OA_AMEND_POSITION_SLTP_REQ, position_amends(venue))
    return venue


def settings(
    tmp_path: pathlib.Path, *, answer_wait_secs: float = 5.0, close_wait_secs: float = 10.0
) -> vec.Settings:
    return vec.Settings(
        symbol=SYMBOL,
        trailing_wait_secs=0.3,
        answer_wait_secs=answer_wait_secs,
        settle_secs=0.05,
        close_wait_secs=close_wait_secs,
        close_settle_secs=0.3,
        log_dir=tmp_path,
    )


async def run(
    venue: ExecutionVenue,
    tmp_path: pathlib.Path,
    *,
    while_listening: list[Message] | Callable[[ExecutionVenue], list[Message]] = (),
    answer_wait_secs: float = 5.0,
    close_wait_secs: float = 10.0,
    watch: bool = False,
) -> vec.Result:
    """One run against `venue`; `while_listening`, or what it makes, is pushed once it listens."""
    server = venue.server
    await server.start()
    listening = asyncio.Event()
    try:
        task = asyncio.create_task(
            vec.run_check(
                settings(
                    tmp_path, answer_wait_secs=answer_wait_secs, close_wait_secs=close_wait_secs
                ),
                CREDENTIALS,
                TRADER_LOGIN,
                address=vec.Address(server.host, server.host, server.port, tls=False),
                status=lambda _text: None,
                listening=listening,
                watch=watch,
            ),
        )
        if while_listening:
            await wait_until(lambda: listening.is_set() or task.done(), timeout_secs=60.0)
            pushed = while_listening(venue) if callable(while_listening) else while_listening
            for message in pushed:
                await server.push(message)
        return await asyncio.wait_for(task, 60.0)
    finally:
        await server.stop()


def by_item(result: vec.Result) -> dict[int, vec.Finding]:
    return {finding.item: finding for finding in result.findings}


def received(venue: ExecutionVenue, cls: type[Message]) -> list[Message]:
    return [m for m in venue.server.received if isinstance(m, cls)]


def trailing_move(stop: float) -> oa.ProtoOATrailingSLChangedEvent:
    return oa.ProtoOATrailingSLChangedEvent(
        ctidTraderAccountId=ACCOUNT_ID,
        positionId=POSITION,
        orderId=PROTECTIVE,
        stopPrice=stop,
        utcLastUpdateTimestamp=int(time.time() * 1000),
    )


def exchange(
    request: Message | None = None,
    *,
    answer: Message | None = None,
    error_code: str | None = None,
    timed_out: bool = False,
) -> vec.Exchange:
    return vec.Exchange(
        request or oa.ProtoOACancelOrderReq(ctidTraderAccountId=ACCOUNT_ID, orderId=1),
        sent_at=0.0,
        answer=answer,
        error_code=error_code,
        timed_out=timed_out,
        answered_at=0.1,
    )


def execution(kind: int, *, error: str = "") -> oa.ProtoOAExecutionEvent:
    event = make_event(kind, pending_order(OPENED), error=error)
    event.ctidTraderAccountId = ACCOUNT_ID
    return event


ORDER_ERROR = oa.ProtoOAOrderErrorEvent(
    ctidTraderAccountId=ACCOUNT_ID, errorCode="ORDER_NOT_FOUND", description="Order not found"
)


# -- Refusals before anything is built -----------------------------------------------------


@pytest.fixture
def nothing_may_be_built(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Every way the script could reach an account, replaced by a call recorder."""
    calls: list[str] = []

    def recorder(name: str):
        def call(*_args, **_kwargs):
            calls.append(name)
            raise AssertionError(f"{name} was called")

        return call

    monkeypatch.setattr(vec, "run_check", recorder("run_check"))
    monkeypatch.setattr(vec.GuardedAccount, "__init__", recorder("account"))
    monkeypatch.setattr(CTraderConnection, "connect", recorder("connect"))
    monkeypatch.setattr(vec.get_tokens, "load_env", recorder("load_env"))
    return calls


def test_refuses_without_the_flag(
    nothing_may_be_built: list[str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def no_input(_prompt: str = "") -> str:
        nothing_may_be_built.append("input")
        raise AssertionError("input was asked")

    monkeypatch.setattr(builtins, "input", no_input)

    assert vec.main(["--symbol", SYMBOL]) == 2
    assert nothing_may_be_built == []
    out, err = capsys.readouterr()
    assert "--send-commands" in err
    assert out == ""


def test_refuses_a_mistyped_symbol_after_printing_the_plan(
    nothing_may_be_built: list[str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(builtins, "input", lambda _prompt="": "EURUSD")

    assert vec.main(["--symbol", SYMBOL, "--send-commands"]) == 2
    assert nothing_may_be_built == []
    assert vec.plan(SYMBOL) in capsys.readouterr().out


def test_refuses_an_expired_access_token(
    nothing_may_be_built: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(builtins, "input", lambda _prompt="": SYMBOL)
    expired = {**FAKE_ENV, "CTRADER_TOKEN_EXPIRES_AT": str(int(time.time()) - 60)}
    monkeypatch.setattr(vec.get_tokens, "load_env", lambda _path: dict(expired))

    assert vec.main(["--symbol", SYMBOL, "--send-commands"]) == 2
    assert nothing_may_be_built == []


@pytest.mark.parametrize(
    ("env", "named"),
    [
        ({k: v for k, v in FAKE_ENV.items() if k != "CTRADER_ACCESS_TOKEN"}, "ACCESS_TOKEN"),
        ({**FAKE_ENV, "CTRADER_TRADER_LOGIN": "not-a-login"}, "not a number"),
    ],
)
def test_bad_credentials_are_named_without_being_echoed(
    env: dict[str, str],
    named: str,
    nothing_may_be_built: list[str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(builtins, "input", lambda _prompt="": SYMBOL)
    monkeypatch.setattr(vec.get_tokens, "load_env", lambda _path: dict(env))

    assert vec.main(["--symbol", SYMBOL, "--send-commands"]) == 1
    assert nothing_may_be_built == []
    err = capsys.readouterr().err
    assert named in err
    for secret in (*FAKE_SECRETS.values(), "not-a-login"):
        assert secret not in err


def test_a_failed_run_prints_only_the_errors_type(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    async def failing(*_args, **_kwargs):
        raise vec.CTraderRequestError(
            "CH_ACCESS_TOKEN_INVALID", f"token {FAKE_SECRETS['CTRADER_ACCESS_TOKEN']} refused"
        )

    monkeypatch.setattr(builtins, "input", lambda _prompt="": SYMBOL)
    monkeypatch.setattr(vec.get_tokens, "load_env", lambda _path: dict(FAKE_ENV))
    monkeypatch.setattr(vec, "run_check", failing)

    assert vec.main(["--symbol", SYMBOL, "--send-commands"]) == 1
    out, err = capsys.readouterr()
    assert "CTraderRequestError" in err
    assert "refused" not in err
    for secret in FAKE_SECRETS.values():
        assert secret not in out + err


@pytest.mark.parametrize(
    ("result", "code"),
    [
        (vec.Result(findings=[vec.Finding(1, "t", vec.OK), vec.Finding(2, "t", vec.UNKNOWN)]), 0),
        (vec.Result(findings=[vec.Finding(1, "t", vec.DIFFERS)]), 1),
        (vec.Result(refusal=("0 positions on the symbol, not exactly one",)), 1),
        (
            vec.Result(
                findings=[vec.Finding(1, "t", vec.OK)],
                failure="CTraderConnectionError",
                left_open=("The position on US100.cash stays open. Close it by hand.",),
            ),
            1,
        ),
    ],
)
def test_main_prints_the_report_and_exits_by_it(
    result: vec.Result,
    code: int,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    seen: list = []

    async def canned(settings, credentials, trader_login, **_kwargs):
        seen.append((settings, credentials, trader_login))
        return result

    monkeypatch.setattr(builtins, "input", lambda _prompt="": SYMBOL)
    monkeypatch.setattr(vec.get_tokens, "load_env", lambda _path: dict(FAKE_ENV))
    monkeypatch.setattr(vec, "run_check", canned)

    argv = ["--symbol", SYMBOL, "--send-commands", "--trailing-wait-secs", "5"]
    assert vec.main(argv) == code
    ((given, credentials, login),) = seen
    assert (given.symbol, given.trailing_wait_secs, login) == (SYMBOL, 5.0, TRADER_LOGIN)
    # No refresh: a rotated token would not be written back.
    assert credentials.refresh_token is None
    assert vec.format_report(result) in capsys.readouterr().out


def test_a_run_stopped_by_hand_says_what_to_check(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    async def interrupted(*_args, **_kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(builtins, "input", lambda _prompt="": SYMBOL)
    monkeypatch.setattr(vec.get_tokens, "load_env", lambda _path: dict(FAKE_ENV))
    monkeypatch.setattr(vec, "run_check", interrupted)

    assert vec.main(["--symbol", SYMBOL, "--send-commands"]) == 130
    out = capsys.readouterr().out
    assert "Stopped by hand" in out
    assert "close the position by hand" in out


def test_the_plan_says_what_changes_and_that_the_position_stays_open() -> None:
    text = vec.plan(SYMBOL)

    assert SYMBOL in text
    for change in ("limit price", "volume", "cancels it", "does not exist", "take-profit"):
        assert change in text
    assert "never opens or closes a position and never places an order" in text
    assert "STAYS OPEN" in text and "close it by hand" in text


def test_the_parser_defaults_and_refuses_waits_that_are_not_positive() -> None:
    args = vec.build_arg_parser().parse_args(["--symbol", SYMBOL])

    assert not args.send_commands
    assert (args.trailing_wait_secs, args.answer_wait_secs) == (60.0, 30.0)
    assert args.log_dir == vec._REPO_ROOT / "tests" / "recordings"
    for wait in ("0", "-1", "inf", "nan", "soon"):
        with pytest.raises(SystemExit):
            vec.build_arg_parser().parse_args(["--symbol", SYMBOL, "--trailing-wait-secs", wait])


# -- The guard -----------------------------------------------------------------------------

TARGETS = vec.Targets(
    pending_order_id=PENDING,
    position_id=POSITION,
    no_such_order_id=PENDING + 1_000_000_000,
    max_volume=RAISED,
    pending_is_buy=True,
    start_limit_price=84000.0,
    position_is_long=True,
    start_stop_loss=84900.0,
    start_take_profit=85100.0,
)


def all_requests() -> list[type[Message]]:
    return [
        getattr(oa, name) for name in dir(oa) if name.startswith("ProtoOA") and name.endswith("Req")
    ]


def guard(targets: vec.Targets | None = TARGETS) -> vec.Guard:
    made = vec.Guard(vec.Recorder(vec.record_execution.Recording(0)))
    made.targets = targets
    return made


def amend(**fields) -> oa.ProtoOAAmendOrderReq:
    return oa.ProtoOAAmendOrderReq(ctidTraderAccountId=ACCOUNT_ID, **fields)


def levels(**fields) -> oa.ProtoOAAmendPositionSLTPReq:
    return oa.ProtoOAAmendPositionSLTPReq(ctidTraderAccountId=ACCOUNT_ID, **fields)


def cancel(order_id: int) -> oa.ProtoOACancelOrderReq:
    return oa.ProtoOACancelOrderReq(ctidTraderAccountId=ACCOUNT_ID, orderId=order_id)


def test_no_order_opening_or_position_closing_request_is_allowed() -> None:
    assert not set(OPENING_OR_CLOSING) & vec.ALLOWED_REQUESTS
    assert {
        oa.ProtoOACancelOrderReq,
        oa.ProtoOAAmendOrderReq,
        oa.ProtoOAAmendPositionSLTPReq,
    } == vec.COMMAND_REQUESTS
    # A refresh would rotate the owner's token pair, which this script does not write back.
    assert oa.ProtoOARefreshTokenReq not in vec.ALLOWED_REQUESTS


@pytest.mark.parametrize(
    "cls",
    [c for c in all_requests() if c not in vec.ALLOWED_REQUESTS],
    ids=lambda c: c.__name__,
)
def test_the_guard_refuses_every_request_outside_the_list(cls: type[Message]) -> None:
    checked = guard()

    with pytest.raises(vec.CommandRefused, match=cls.__name__):
        checked.check(cls())
    assert checked.refused


def test_the_guard_lets_reads_through_and_no_command_before_the_targets_are_known() -> None:
    checked = guard(targets=None)

    checked.check(oa.ProtoOAReconcileReq(ctidTraderAccountId=ACCOUNT_ID))
    for command in (
        cancel(PENDING),
        amend(orderId=PENDING, volume=MINIMUM),
        levels(positionId=POSITION, stopLoss=84900.0),
    ):
        with pytest.raises(vec.CommandRefused, match="before the owner's orders were found"):
            checked.check(command)


@pytest.mark.parametrize(
    "request_",
    [
        cancel(PENDING),
        cancel(TARGETS.no_such_order_id),
        amend(orderId=PENDING, volume=RAISED, limitPrice=83999.9),
        amend(orderId=PENDING, volume=MINIMUM, limitPrice=84000.0),
        levels(positionId=POSITION, stopLoss=84899.9, takeProfit=85100.0),
        levels(positionId=POSITION, stopLoss=84899.9),
    ],
)
def test_the_guard_lets_through_the_commands_on_the_owners_objects(request_: Message) -> None:
    guard().check(request_)


@pytest.mark.parametrize(
    ("request_", "reason"),
    [
        (cancel(PROTECTIVE), "other than the owner's"),
        (amend(orderId=PENDING + 1, volume=MINIMUM), "other than the owner's"),
        (amend(orderId=PENDING, volume=RAISED + 1), "volume"),
        (amend(orderId=PENDING, volume=MINIMUM, limitPrice=84000.1), "toward the market"),
        (levels(positionId=POSITION + 1, stopLoss=84899.9), "other than the owner's"),
        (levels(positionId=POSITION, takeProfit=85100.0), "removing the stop-loss"),
        (levels(positionId=POSITION, stopLoss=84900.1), "stop-loss toward the market"),
        (
            levels(positionId=POSITION, stopLoss=84899.9, takeProfit=85200.0),
            "changing the take-profit",
        ),
    ],
)
def test_the_guard_refuses_commands_beyond_the_owners_objects(
    request_: Message, reason: str
) -> None:
    with pytest.raises(vec.CommandRefused, match=reason):
        guard().check(request_)


def test_a_short_positions_stop_loss_may_only_move_up() -> None:
    short = guard(vec.Targets(**{**TARGETS.__dict__, "position_is_long": False}))

    short.check(levels(positionId=POSITION, stopLoss=84900.1))
    with pytest.raises(vec.CommandRefused, match="stop-loss toward the market"):
        short.check(levels(positionId=POSITION, stopLoss=84899.9))


def test_a_stop_loss_the_broker_trailed_may_be_sent_back_where_it_stands() -> None:
    checked = guard()
    checked.recorder.inbound.append(vec.Inbound(0.0, trailing_move(84950.0), pushed=True))

    checked.check(levels(positionId=POSITION, stopLoss=84950.0))
    with pytest.raises(vec.CommandRefused, match="stop-loss toward the market"):
        checked.check(levels(positionId=POSITION, stopLoss=84950.1))


def test_a_sell_limit_may_only_move_up() -> None:
    selling = guard(vec.Targets(**{**TARGETS.__dict__, "pending_is_buy": False}))

    selling.check(amend(orderId=PENDING, volume=MINIMUM, limitPrice=84000.1))
    with pytest.raises(vec.CommandRefused, match="toward the market"):
        selling.check(amend(orderId=PENDING, volume=MINIMUM, limitPrice=83999.9))


def test_the_guard_knows_whether_a_connection_still_sends_through_it() -> None:
    connection = CTraderConnection("127.0.0.1", 1, logger=RecordingLogger(), tls=False)
    checked = guard()
    assert not checked.covers(connection)

    checked.instrument(connection)
    assert checked.covers(connection)
    assert not guard().covers(connection)

    connection.request = CTraderConnection.request.__get__(connection)
    assert not checked.covers(connection)


async def test_a_guarded_connection_refuses_before_the_socket_and_logs_every_answer() -> None:
    server = FakeCTraderServer()
    server.on(
        om.PROTO_OA_RECONCILE_REQ,
        lambda r: oa.ProtoOAReconcileRes(ctidTraderAccountId=r.ctidTraderAccountId),
    )
    server.on(
        om.PROTO_OA_ORDER_DETAILS_REQ,
        lambda _r: oa.ProtoOAErrorRes(ctidTraderAccountId=ACCOUNT_ID, errorCode="ORDER_NOT_FOUND"),
    )
    server.on(om.PROTO_OA_CANCEL_ORDER_REQ, lambda _r: ORDER_ERROR)
    await server.start()
    connection = CTraderConnection(server.host, server.port, logger=RecordingLogger(), tls=False)
    checked = guard()
    checked.instrument(connection)
    checked.recorder.tap(connection)
    await connection.connect()
    try:
        for forbidden in OPENING_OR_CLOSING:
            with pytest.raises(vec.CommandRefused):
                await connection.request(forbidden(ctidTraderAccountId=ACCOUNT_ID))
            with pytest.raises(vec.CommandRefused):
                await connection.send(forbidden(ctidTraderAccountId=ACCOUNT_ID))
        await connection.request(oa.ProtoOAReconcileReq(ctidTraderAccountId=ACCOUNT_ID))
        with pytest.raises(vec.CTraderRequestError):
            await connection.request(
                oa.ProtoOAOrderDetailsReq(ctidTraderAccountId=ACCOUNT_ID, orderId=7)
            )
        await connection.request(cancel(PENDING))
    finally:
        await connection.close()
        await server.stop()

    assert [type(m) for m in server.received] == [
        oa.ProtoOAReconcileReq,
        oa.ProtoOAOrderDetailsReq,
        oa.ProtoOACancelOrderReq,
    ]
    snapshot, details, cancelled = checked.exchanges
    assert isinstance(snapshot.answer, oa.ProtoOAReconcileRes)
    assert (details.error_code, details.answer) == ("ORDER_NOT_FOUND", None)
    assert cancelled.answer == ORDER_ERROR
    recording = checked.recorder.recording
    assert [type(m) for m in recording.messages()] == [
        oa.ProtoOAReconcileRes,
        oa.ProtoOAErrorRes,
        oa.ProtoOAOrderErrorEvent,
    ]
    assert [e.note for e in recording.timeline if e.kind == "marker"] == [
        "request ProtoOAReconcileReq",
        "request ProtoOAOrderDetailsReq",
        "request ProtoOACancelOrderReq",
    ]


async def test_a_request_lost_with_the_connection_keeps_its_error_on_the_exchange() -> None:
    async def lost(_payload, **_kwargs):
        raise CTraderConnectionError("connection lost")

    connection = SimpleNamespace(request=lost, send=None)
    checked = guard()
    checked.instrument(connection)

    with pytest.raises(CTraderConnectionError):
        await connection.request(oa.ProtoOAReconcileReq(ctidTraderAccountId=ACCOUNT_ID))

    (logged,) = checked.exchanges
    assert logged.failed == "CTraderConnectionError"
    assert vec.answer_text(logged) == "no answer: CTraderConnectionError"
    assert vec.decide_details_of_ended(logged)[0] == vec.UNKNOWN


# -- Finding the owner's orders ------------------------------------------------------------


def snapshot(*, orders=None, positions=None) -> oa.ProtoOAReconcileRes:
    found = oa.ProtoOAReconcileRes(ctidTraderAccountId=ACCOUNT_ID)
    found.order.extend(
        [protective_order(OPENED), pending_order(OPENED)] if orders is None else orders
    )
    found.position.extend([owner_position(OPENED)] if positions is None else positions)
    return found


def changed_position(**fields) -> om.ProtoOAPosition:
    position = owner_position(OPENED)
    for name, value in fields.items():
        if name == "volume":
            position.tradeData.volume = value
        elif value is None:
            position.ClearField(name)
        else:
            setattr(position, name, value)
    return position


def labelled(order: om.ProtoOAOrder) -> om.ProtoOAOrder:
    order.tradeData.label = order_record.encode_label("O-E-1")
    return order


def sized(order: om.ProtoOAOrder, volume: int) -> om.ProtoOAOrder:
    order.tradeData.volume = volume
    return order


def test_the_owners_two_objects_are_chosen_and_the_protective_order_is_not_one() -> None:
    choice = vec.choose(snapshot(), symbol_id=US100_SYMBOL_ID, min_volume=MINIMUM)

    assert choice == vec.Choice(pending_order(OPENED), owner_position(OPENED))


def test_objects_on_other_symbols_are_left_out() -> None:
    elsewhere = pending_order(OPENED, orderId=PENDING + 1)
    elsewhere.tradeData.symbolId = US100_SYMBOL_ID + 1
    found = snapshot(orders=[pending_order(OPENED), elsewhere])

    assert isinstance(vec.choose(found, symbol_id=US100_SYMBOL_ID, min_volume=MINIMUM), vec.Choice)


@pytest.mark.parametrize(
    ("found", "reason"),
    [
        (snapshot(orders=[]), "0 pending orders on the symbol"),
        (
            snapshot(orders=[pending_order(OPENED), pending_order(OPENED, orderId=PENDING + 1)]),
            "2 pending orders",
        ),
        (
            snapshot(orders=[pending_order(OPENED, orderType=om.STOP, stopPrice=86000.0)]),
            "a STOP order, not a LIMIT order",
        ),
        (snapshot(orders=[labelled(pending_order(OPENED))]), "placed by the node"),
        (
            snapshot(orders=[sized(pending_order(OPENED), RAISED)]),
            "volume is 0.02, not the minimum 0.01",
        ),
        (snapshot(positions=[]), "0 positions on the symbol"),
        (snapshot(positions=[owner_position(OPENED), owner_position(OPENED)]), "2 positions"),
        (snapshot(positions=[changed_position(stopLoss=None)]), "no stop-loss"),
        (snapshot(positions=[changed_position(takeProfit=None)]), "no take-profit"),
        (
            snapshot(positions=[changed_position(volume=100)]),
            "the position's volume is 1, not the minimum 0.01",
        ),
    ],
)
def test_anything_but_exactly_the_owners_two_objects_is_refused(found, reason: str) -> None:
    refusal = vec.choose(found, symbol_id=US100_SYMBOL_ID, min_volume=MINIMUM)

    assert isinstance(refusal, vec.Refusal)
    assert any(reason in line for line in refusal.reasons), refusal.reasons


def held(venue_order_id: str, client_order_id: str, *, is_open: bool = True) -> SimpleNamespace:
    """What `confirm()` reads of a Nautilus order."""
    return SimpleNamespace(
        venue_order_id=VenueOrderId(venue_order_id),
        client_order_id=ClientOrderId(client_order_id),
        is_open=is_open,
    )


ENTRY_ORDER = make_order(ENTRY, POSITION, volume=MINIMUM, symbol=US100_SYMBOL_ID)
NAUTILUS = [
    held(str(PENDING), "P"),
    held(f"{ENTRY}-SL", "SL"),
    held(f"{ENTRY}-TP", "TP"),
    held(f"{ENTRY}-TP-2", "TP-OLD", is_open=False),
]
CHOICE = vec.Choice(pending_order(OPENED), owner_position(OPENED))


def test_confirm_finds_the_pending_order_and_the_live_legs_in_nautilus() -> None:
    found = vec.confirm(CHOICE, [protective_order(OPENED), ENTRY_ORDER], NAUTILUS)

    assert isinstance(found, vec.Found)
    assert (found.entry_id, found.pending_order.value) == (ENTRY, "P")
    assert (found.stop_loss.value, found.take_profit.value) == ("SL", "TP")


@pytest.mark.parametrize(
    ("orders", "nautilus", "reason"),
    [
        ([protective_order(OPENED)], NAUTILUS, "names no entry"),
        ([labelled(make_order(ENTRY, POSITION))], NAUTILUS, "opened by the node"),
        ([ENTRY_ORDER], NAUTILUS[1:], "no open order for the pending order"),
        ([ENTRY_ORDER], [NAUTILUS[0], NAUTILUS[2]], "0 open stop-loss legs"),
        ([ENTRY_ORDER], [*NAUTILUS, held(f"{ENTRY}-TP-3", "TP-3")], "2 open take-profit legs"),
    ],
)
def test_confirm_refuses_what_nautilus_does_not_hold_as_the_owners(
    orders, nautilus, reason
) -> None:
    refusal = vec.confirm(CHOICE, orders, nautilus)

    assert isinstance(refusal, vec.Refusal)
    assert any(reason in line for line in refusal.reasons), refusal.reasons


def test_the_missing_order_id_is_past_every_id_seen() -> None:
    assert vec.no_such_order_id([5, 9, 7]) == 9 + 1_000_000_000
    assert vec.no_such_order_id([]) == 1_000_000_000


def test_a_move_away_is_ten_steps_and_never_reaches_zero() -> None:
    step = Decimal("0.01")

    assert vec.moved_away(Decimal("84000.00"), step, lower=True) == Decimal("83999.90")
    assert vec.moved_away(Decimal("84000.00"), step, lower=False) == Decimal("84000.10")
    assert vec.moved_away(Decimal("0.05"), step, lower=True) is None


# -- Decisions -----------------------------------------------------------------------------


def test_a_refusal_reason_loses_the_brokers_description() -> None:
    assert vec.code_only("TRADING_BAD_STOPS: Invalid stops for 7654321") == "TRADING_BAD_STOPS"
    for own in (
        "the broker kept the level at 85387.22",
        "a protective level covers the whole position (1.00); its quantity cannot be set",
    ):
        assert vec.code_only(own) == own


@pytest.mark.parametrize(
    ("made", "text", "refusal"),
    [
        (None, "not sent", None),
        (
            vec.Exchange(cancel(1), 0.0, failed="CTraderConnectionError"),
            "not sent: CTraderConnectionError",
            None,
        ),
        (
            vec.Exchange(cancel(1), 0.0, failed="CTraderConnectionError", answered_at=0.1),
            "no answer: CTraderConnectionError",
            None,
        ),
        (
            exchange(error_code="ORDER_NOT_FOUND"),
            "ProtoOAErrorRes ORDER_NOT_FOUND",
            "ORDER_NOT_FOUND",
        ),
        (exchange(timed_out=True), "no answer carrying the request's id", None),
        (exchange(answer=ORDER_ERROR), "ProtoOAOrderErrorEvent ORDER_NOT_FOUND", "ORDER_NOT_FOUND"),
        (
            exchange(answer=execution(om.ORDER_CANCEL_REJECTED, error="TRADING_DISABLED")),
            "ProtoOAExecutionEvent ORDER_CANCEL_REJECTED TRADING_DISABLED",
            "TRADING_DISABLED",
        ),
        (
            exchange(answer=execution(om.ORDER_REJECTED)),
            "ProtoOAExecutionEvent ORDER_REJECTED",
            "ORDER_REJECTED",
        ),
        (
            exchange(answer=execution(om.ORDER_REPLACED)),
            "ProtoOAExecutionEvent ORDER_REPLACED",
            None,
        ),
    ],
)
def test_answers_are_told_by_type_and_code(made, text: str, refusal: str | None) -> None:
    assert vec.answer_text(made) == text
    assert vec.refusal_of(made) == refusal


REPLACED = exchange(answer=execution(om.ORDER_REPLACED))
REFUSED = exchange(answer=ORDER_ERROR)


@pytest.mark.parametrize(
    ("outcome", "made", "status"),
    [
        (vec.Outcome(done=True), REPLACED, vec.OK),
        (vec.Outcome(rejection="TRADING_BAD_VOLUME"), REFUSED, vec.DIFFERS),
        (vec.Outcome(), None, vec.UNKNOWN),
        (vec.Outcome(skipped="the order is no longer open in Nautilus"), None, vec.UNKNOWN),
    ],
)
def test_a_step_is_decided_by_what_nautilus_shows(outcome, made, status: str) -> None:
    decided, detail = vec.decide_step(outcome, made, "price 83999.90")

    assert decided == status
    assert detail[0].startswith(("Nautilus", "not run"))


@pytest.mark.parametrize(
    ("made", "status"),
    [
        ([], vec.UNKNOWN),
        ([REPLACED, REPLACED], vec.OK),
        ([REPLACED, REFUSED], vec.OK),
        ([REFUSED, REFUSED], vec.UNKNOWN),
        ([exchange(answer=execution(om.ORDER_ACCEPTED))], vec.DIFFERS),
    ],
)
def test_an_accepted_amend_of_a_pending_order_should_be_answered_replaced(made, status) -> None:
    assert vec.decide_order_replaced(made)[0] == status


def amended(**fields) -> om.ProtoOAOrder:
    order = pending_order(OPENED, limitPrice=83999.9, **fields)
    order.tradeData.volume = RAISED
    order.utcLastUpdateTimestamp += 5
    return order


@pytest.mark.parametrize(
    ("after", "status", "named"),
    [
        (amended(), vec.OK, "expirationTimestamp"),
        # A flag stated at its schema default is the same flag.
        (amended(trailingStopLoss=False, stopTriggerMethod=om.TRADE), vec.OK, "kept"),
        (amended(expirationTimestamp=None), vec.DIFFERS, "expirationTimestamp"),
        (amended(relativeStopLoss=None, stopLoss=83950.0), vec.DIFFERS, "relativeStopLoss"),
        (amended(timeInForce=om.GOOD_TILL_CANCEL), vec.DIFFERS, "timeInForce"),
        (amended(trailingStopLoss=True), vec.DIFFERS, "trailingStopLoss"),
        (amended(relativeStopLoss=4_000_000), vec.DIFFERS, "relativeStopLoss"),
        (None, vec.UNKNOWN, "not listed"),
    ],
)
def test_an_amend_should_keep_what_it_re_sent(after, status: str, named: str) -> None:
    decided, detail = vec.decide_order_kept(pending_order(OPENED), after, amended=True)

    assert decided == status
    assert any(named in line for line in detail), detail


def test_a_price_held_next_to_its_distance_may_follow_the_moved_limit_price() -> None:
    before = pending_order(OPENED, stopLoss=83950.0, takeProfit=84100.0)
    after = amended(stopLoss=83949.9, takeProfit=84099.9)

    decided, detail = vec.decide_order_kept(before, after, amended=True)

    assert decided == vec.OK
    assert "stopLoss not compared: it follows the moved limit price" in detail
    assert "takeProfit not compared: it follows the moved limit price" in detail
    lost = amended(relativeTakeProfit=None, stopLoss=83949.9, takeProfit=84099.9)
    assert vec.decide_order_kept(before, lost, amended=True)[0] == vec.DIFFERS


def test_an_order_with_nothing_optional_to_keep_settles_nothing() -> None:
    bare = pending_order(
        OPENED, expirationTimestamp=None, relativeStopLoss=None, relativeTakeProfit=None
    )

    assert vec.decide_order_kept(bare, bare, amended=True)[0] == vec.UNKNOWN
    assert vec.decide_order_kept(bare, bare, amended=False)[0] == vec.UNKNOWN


@pytest.mark.parametrize(
    ("fields", "status", "line"),
    [
        ({}, vec.OK, "stop-loss: relativeStopLoss"),
        ({"relativeStopLoss": None, "stopLoss": 83950.0}, vec.OK, "stop-loss: stopLoss"),
        ({"stopLoss": 83950.0}, vec.OK, "both forms are reported"),
        ({"relativeStopLoss": None, "relativeTakeProfit": None}, vec.UNKNOWN, "carries no"),
    ],
)
def test_the_form_of_attached_levels_is_read_off_the_order(fields, status, line) -> None:
    decided, detail = vec.decide_level_form(pending_order(OPENED, **fields))

    assert decided == status
    assert any(line in text for text in detail), detail


@pytest.mark.parametrize(
    ("made", "status"),
    [
        (exchange(answer=ORDER_ERROR), vec.OK),
        (exchange(answer=execution(om.ORDER_CANCEL_REJECTED, error="ORDER_NOT_FOUND")), vec.OK),
        (exchange(error_code="ORDER_NOT_FOUND"), vec.OK),
        (exchange(timed_out=True), vec.DIFFERS),
        (exchange(answer=execution(om.ORDER_CANCELLED)), vec.DIFFERS),
        (None, vec.UNKNOWN),
        (vec.Exchange(cancel(1), 0.0, failed="CTraderConnectionError"), vec.UNKNOWN),
    ],
)
def test_each_way_the_adapter_reads_a_refusal_is_ok(made, status: str) -> None:
    assert vec.decide_refusal_form(made)[0] == status


DETAILS = oa.ProtoOAOrderDetailsRes(
    ctidTraderAccountId=ACCOUNT_ID,
    order=pending_order(OPENED, orderStatus=om.ORDER_STATUS_CANCELLED),
)


@pytest.mark.parametrize(
    ("made", "ended", "unknown"),
    [
        (exchange(answer=DETAILS), vec.OK, vec.DIFFERS),
        (exchange(error_code="ORDER_NOT_FOUND"), vec.DIFFERS, vec.OK),
        (exchange(timed_out=True), vec.DIFFERS, vec.DIFFERS),
        (None, vec.UNKNOWN, vec.UNKNOWN),
        (vec.Exchange(cancel(1), 0.0, failed="CTraderConnectionError"), vec.UNKNOWN, vec.UNKNOWN),
    ],
)
def test_order_details_of_ended_and_unknown_orders(made, ended: str, unknown: str) -> None:
    assert vec.decide_details_of_ended(made)[0] == ended
    assert vec.decide_details_of_unknown(made)[0] == unknown


MOVE = levels(
    positionId=POSITION,
    stopLoss=84899.9,
    takeProfit=85100.0,
    stopLossTriggerMethod=om.TRADE,
    trailingStopLoss=True,
    guaranteedStopLoss=False,
)


@pytest.mark.parametrize(
    ("made", "status"),
    [
        (exchange(MOVE, answer=execution(om.ORDER_REPLACED)), vec.OK),
        (exchange(MOVE, answer=ORDER_ERROR), vec.DIFFERS),
        (exchange(MOVE, error_code="INVALID_REQUEST"), vec.DIFFERS),
        (exchange(MOVE, timed_out=True), vec.UNKNOWN),
        (exchange(levels(positionId=POSITION, stopLoss=84899.9)), vec.UNKNOWN),
        (None, vec.UNKNOWN),
    ],
)
def test_an_amend_stating_the_stop_loss_terms_should_be_accepted(made, status: str) -> None:
    assert vec.decide_terms_accepted(made)[0] == status


def position_after(*, trailing: bool = True, stop: float | None = 84899.9, target=None):
    return changed_position(trailingStopLoss=trailing, stopLoss=stop, takeProfit=target)


@pytest.mark.parametrize(
    ("kwargs", "status"),
    [
        ({"trailing_at_start": True, "moved": True, "after": position_after()}, vec.OK),
        (
            {"trailing_at_start": True, "moved": True, "after": position_after(trailing=False)},
            vec.DIFFERS,
        ),
        ({"trailing_at_start": False, "moved": True, "after": position_after()}, vec.UNKNOWN),
        ({"trailing_at_start": True, "moved": False, "after": position_after()}, vec.UNKNOWN),
        ({"trailing_at_start": True, "moved": True, "after": None}, vec.UNKNOWN),
    ],
)
def test_a_moved_trailing_stop_should_stay_trailing(kwargs, status: str) -> None:
    assert vec.decide_trailing_kept(**kwargs)[0] == status


@pytest.mark.parametrize(
    ("after", "removed", "left_out", "kept"),
    [
        (position_after(), True, vec.OK, vec.OK),
        (position_after(target=85100.0), True, vec.DIFFERS, vec.OK),
        (position_after(stop=None), True, vec.OK, vec.DIFFERS),
        (position_after(trailing=False), True, vec.OK, vec.DIFFERS),
        (None, True, vec.UNKNOWN, vec.UNKNOWN),
        (position_after(), False, vec.UNKNOWN, vec.UNKNOWN),
    ],
)
def test_removing_the_take_profit_should_leave_the_trailing_stop(
    after, removed: bool, left_out: str, kept: str
) -> None:
    assert vec.decide_level_left_out(removed=removed, after=after)[0] == left_out
    decided = vec.decide_stop_kept(removed=removed, trailing_at_start=True, after=after)
    assert decided[0] == kept


def test_a_stop_not_trailing_at_the_start_is_checked_for_its_level_only() -> None:
    decided, detail = vec.decide_stop_kept(
        removed=True, trailing_at_start=False, after=position_after(trailing=False)
    )

    assert decided == vec.OK
    assert any("not checked" in line for line in detail)


@pytest.mark.parametrize(
    ("seen", "status"),
    [
        ([], vec.UNKNOWN),
        ([("price amend", []), ("cancel", [])], vec.OK),
        ([("price amend", ["ORDER_REPLACED"]), ("cancel", [])], vec.DIFFERS),
    ],
)
def test_an_accepted_command_should_bring_only_its_answer(seen, status: str) -> None:
    assert vec.decide_besides_answers(seen)[0] == status


@pytest.mark.parametrize(
    ("trailing", "executions", "at_start", "status"),
    [
        (0, 0, True, vec.UNKNOWN),
        (2, 0, True, vec.OK),
        (2, 2, True, vec.DIFFERS),
        (0, 2, True, vec.DIFFERS),
        (2, 0, False, vec.UNKNOWN),
    ],
)
def test_trailing_moves_should_arrive_as_trailing_events_only(
    trailing: int, executions: int, at_start: bool, status: str
) -> None:
    decided = vec.decide_trailing_moves(
        trailing_events=trailing,
        execution_events=executions,
        wait_secs=60.0,
        trailing_at_start=at_start,
    )

    assert decided[0] == status


def test_the_volume_after_a_partial_fill_is_left_unknown_with_its_reason() -> None:
    status, (reason,) = vec.decide_volume_after_partial_fill()

    assert status == vec.UNKNOWN
    assert "fills nothing" in reason


def test_what_is_left_open_tells_the_owner_what_to_close() -> None:
    standing = snapshot(positions=[position_after()])
    lines = vec.left_open(standing, symbol=SYMBOL, pending_order_id=PENDING, position_id=POSITION)

    assert any("stays open with its stop-loss at 84899.9, trailing" in line for line in lines)
    assert any("Close it by hand" in line for line in lines)
    assert any("pending order" in line and "cancel it by hand" in line for line in lines)

    gone = snapshot(orders=[], positions=[])
    assert vec.left_open(gone, symbol=SYMBOL, pending_order_id=PENDING, position_id=POSITION) == (
        f"Nothing of the two is left open on {SYMBOL}.",
    )
    unread = vec.left_open(None, symbol=SYMBOL, pending_order_id=PENDING, position_id=POSITION)
    assert "close the position by hand" in unread[0]


def test_a_position_left_without_a_stop_loss_is_said_loudly() -> None:
    bare = snapshot(orders=[], positions=[position_after(stop=None)])

    (line,) = vec.left_open(bare, symbol=SYMBOL, pending_order_id=PENDING, position_id=POSITION)

    assert line == f"The position on {SYMBOL} stays open WITHOUT a stop-loss: close it by hand now."


def test_a_failed_run_is_said_first_in_the_report() -> None:
    text = vec.format_report(vec.Result(failure="RuntimeError", left_open=("left",)))

    assert "The run ended early on RuntimeError" in text
    assert text.rstrip().endswith("left")
    assert vec.exit_code(vec.Result(failure="RuntimeError")) == 1


def test_the_report_counts_the_findings_and_says_how_to_scrub_the_recording(tmp_path) -> None:
    raw = tmp_path / "verify_external_commands-x.raw.json"
    result = vec.Result(
        findings=[
            vec.Finding(1, "first", vec.OK, ("detail",)),
            vec.Finding(2, "second", vec.UNKNOWN),
        ],
        left_open=("The position on US100.cash stays open.",),
        raw=raw,
    )

    text = vec.format_report(result)

    assert "OK       1. first\n         detail" in text
    assert "OK 1, DIFFERS 0, UNKNOWN 1" in text
    assert "The position on US100.cash stays open." in text
    assert f"--rescrub {raw}" in text


# -- Runs against the fake venue -----------------------------------------------------------


async def test_a_run_carries_out_every_step_on_the_owners_orders(tmp_path) -> None:
    venue = owner_venue()

    result = await run(venue, tmp_path, while_listening=[trailing_move(84895.0)])

    assert result.refusal == ()
    findings = by_item(result)
    statuses = {item: f.status for item, f in findings.items()}
    assert statuses == {**dict.fromkeys(range(1, 18), vec.OK), 18: vec.UNKNOWN}, result.findings
    assert findings[8].detail == ("stop-loss: relativeStopLoss", "take-profit: relativeTakeProfit")
    assert findings[9].detail == ("refused with ProtoOAOrderErrorEvent ORDER_NOT_FOUND",)

    price, volume = received(venue, oa.ProtoOAAmendOrderReq)
    assert (price.orderId, price.limitPrice, price.volume) == (PENDING, 83999.9, MINIMUM)
    assert (volume.orderId, volume.limitPrice, volume.volume) == (PENDING, 83999.9, RAISED)
    assert price.expirationTimestamp == volume.expirationTimestamp == EXPIRES
    assert [c.orderId for c in received(venue, oa.ProtoOACancelOrderReq)] == [
        PENDING,
        result.guard.targets.no_such_order_id,
    ]
    move, removal = received(venue, oa.ProtoOAAmendPositionSLTPReq)
    assert (move.positionId, move.stopLoss, move.takeProfit) == (POSITION, 84899.9, 85100.0)
    assert move.trailingStopLoss and move.HasField("guaranteedStopLoss")
    assert move.stopLossTriggerMethod == om.TRADE
    assert (removal.stopLoss, removal.HasField("takeProfit"), removal.trailingStopLoss) == (
        84899.9,
        False,
        True,
    )
    for opening_or_closing in OPENING_OR_CLOSING:
        assert received(venue, opening_or_closing) == []
    assert result.guard.refused == []
    assert vec.exit_code(result) == 0

    assert any("stays open with its stop-loss" in line for line in result.left_open)
    assert not any("pending order" in line for line in result.left_open)
    recording, account_id, login = vec.record_execution.decode_raw(result.raw.read_bytes())
    assert (account_id, login) == (ACCOUNT_ID, TRADER_LOGIN)
    kinds = {type(m) for m in recording.messages()}
    assert {oa.ProtoOAOrderErrorEvent, oa.ProtoOATrailingSLChangedEvent} <= kinds


def both_ways() -> list[Message]:
    """A trailing move arriving as its own event and as an execution event."""
    event = make_event(om.ORDER_REPLACED, protective_order(OPENED), position=owner_position(OPENED))
    event.ctidTraderAccountId = ACCOUNT_ID
    return [trailing_move(84895.0), event]


@pytest.mark.parametrize(
    ("pushed", "status"),
    [
        pytest.param([], vec.UNKNOWN, id="market-still"),
        pytest.param([trailing_move(84895.0)], vec.OK, id="trailing-only"),
        pytest.param(both_ways(), vec.DIFFERS, id="both"),
    ],
)
async def test_trailing_moves_are_counted_while_the_run_listens(tmp_path, pushed, status) -> None:
    result = await run(owner_venue(), tmp_path, while_listening=pushed)

    assert by_item(result)[17].status == status


async def test_no_order_opening_or_position_closing_request_is_ever_built(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    built: list[str] = []

    def forbidden(cls: type[Message]) -> type:
        class Forbidden:
            def __init__(self, *_args, **_kwargs) -> None:
                built.append(cls.__name__)
                raise AssertionError(f"{cls.__name__} was built")

        return Forbidden

    for cls in OPENING_OR_CLOSING:
        monkeypatch.setattr(oa, cls.__name__, forbidden(cls))

    result = await run(owner_venue(), tmp_path)

    assert built == []
    assert result.findings
    assert result.guard.refused == []


def refusing_venue() -> ExecutionVenue:
    """The owner's objects at a broker that refuses every cancel and amend."""
    venue = owner_venue()
    stops = oa.ProtoOAOrderErrorEvent(
        ctidTraderAccountId=ACCOUNT_ID, errorCode="TRADING_BAD_STOPS", description="Invalid stops"
    )
    closed = oa.ProtoOAOrderErrorEvent(
        ctidTraderAccountId=ACCOUNT_ID, errorCode="TRADING_DISABLED", description="Market closed"
    )
    venue.server.on(om.PROTO_OA_AMEND_POSITION_SLTP_REQ, lambda _r: stops)
    venue.server.on(om.PROTO_OA_AMEND_ORDER_REQ, lambda _r: closed)
    venue.server.on(om.PROTO_OA_CANCEL_ORDER_REQ, lambda _r: closed)
    return venue


async def test_a_run_at_a_broker_that_refuses_reports_each_refusal(tmp_path) -> None:
    venue = refusing_venue()

    result = await run(venue, tmp_path)

    findings = by_item(result)
    statuses = {item: f.status for item, f in findings.items()}
    assert [statuses[item] for item in range(1, 6)] == [vec.DIFFERS] * 5
    assert findings[1].detail == (
        "Nautilus: rejected: TRADING_DISABLED",
        "broker: ProtoOAOrderErrorEvent TRADING_DISABLED",
    )
    assert findings[4].detail[0] == "Nautilus: rejected: TRADING_BAD_STOPS"
    # Refused commands settle nothing about what an accepted one does.
    assert [statuses[item] for item in (6, 7, 10, 13, 14, 15, 16)] == [vec.UNKNOWN] * 7
    assert (statuses[9], statuses[12]) == (vec.OK, vec.DIFFERS)
    assert vec.exit_code(result) == 1
    assert any("pending order" in line and "cancel it by hand" in line for line in result.left_open)
    # The pending order was never cancelled, so only the missing id's details are asked.
    assert len(received(venue, oa.ProtoOAOrderDetailsReq)) == 1


def lists_no_pending(venue: ExecutionVenue) -> None:
    for order in list(venue.snapshot.order):
        if order.orderId == PENDING:
            venue.snapshot.order.remove(order)


def lists_two_pending(venue: ExecutionVenue) -> None:
    venue.snapshot.order.append(pending_order(OPENED, orderId=PENDING + 1))


def node_placed_the_pending(venue: ExecutionVenue) -> None:
    for order in venue.snapshot.order:
        if order.orderId == PENDING:
            labelled(order)


def node_opened_the_position(venue: ExecutionVenue) -> None:
    labelled(venue.position_orders[POSITION][0])


def lists_no_position(venue: ExecutionVenue) -> None:
    del venue.snapshot.position[:]


def position_is_larger(venue: ExecutionVenue) -> None:
    venue.snapshot.position[0].tradeData.volume = RAISED


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        (lists_no_pending, "0 pending orders"),
        (lists_two_pending, "2 pending orders"),
        (node_placed_the_pending, "placed by the node"),
        (lists_no_position, "0 positions"),
        (position_is_larger, "the position's volume is 0.02"),
        (node_opened_the_position, "opened by the node"),
    ],
)
async def test_a_run_refuses_to_act_unless_it_finds_exactly_the_owners_objects(
    tmp_path, change, reason: str
) -> None:
    venue = owner_venue()
    change(venue)

    result = await run(venue, tmp_path)

    assert result.findings == []
    assert any(reason in line for line in result.refusal), result.refusal
    assert vec.exit_code(result) == 1
    for command in vec.COMMAND_REQUESTS:
        assert received(venue, command) == []
    assert "Refused to act; nothing was changed at the broker:" in vec.format_report(result)


def commands_received(venue: ExecutionVenue) -> list[Message]:
    return [m for m in venue.server.received if type(m) in vec.COMMAND_REQUESTS]


async def test_a_run_that_fails_midway_still_says_what_is_left_open(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    command = vec.Check._command

    async def failing(self, label, *args, **kwargs):
        if label == "stop-loss move":
            raise RuntimeError("the run broke")
        return await command(self, label, *args, **kwargs)

    monkeypatch.setattr(vec.Check, "_command", failing)

    result = await run(owner_venue(), tmp_path)

    assert result.failure == "RuntimeError"
    assert [f.item for f in result.findings] == [1, 2, 3]
    assert any("stays open with its stop-loss at 84900.0" in line for line in result.left_open)
    assert vec.exit_code(result) == 1
    assert "The run ended early on RuntimeError" in vec.format_report(result)


def dropping_venue() -> ExecutionVenue:
    """The owner's objects at a venue that drops the connection on the first order details read.

    The session then reconnects, and a request made meanwhile fails before the guard sees it.
    """
    venue = owner_venue()
    answer = venue.replies[om.PROTO_OA_ORDER_DETAILS_REQ]
    dropped: list[bool] = []

    def drop_once(request):
        if dropped:
            return answer(request)
        dropped.append(True)
        asyncio.get_running_loop().create_task(venue.server.drop_connections())
        return None

    venue.server.on(om.PROTO_OA_ORDER_DETAILS_REQ, drop_once)
    return venue


async def test_a_connection_dropped_midway_still_ends_with_what_is_left_open(tmp_path) -> None:
    venue = dropping_venue()

    result = await run(venue, tmp_path, answer_wait_secs=2.0)

    assert result.failure is None
    findings = by_item(result)
    assert sorted(findings) == list(range(1, 19))
    assert [findings[item].status for item in (1, 2, 3)] == [vec.OK] * 3
    # The ended order's details were lost with the connection; the cancel of the missing id came
    # while the session was reconnecting.
    assert findings[10] == vec.Finding(
        10, findings[10].title, vec.UNKNOWN, ("no answer: CTraderConnectionError",)
    )
    assert findings[9] == vec.Finding(
        9, findings[9].title, vec.UNKNOWN, ("not sent: CTraderConnectionError",)
    )
    assert any("stays open with its stop-loss" in line for line in result.left_open)


async def test_a_connection_no_longer_behind_the_guard_stops_every_command(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(vec.Guard, "covers", lambda _self, _connection: False)
    venue = owner_venue()

    result = await run(venue, tmp_path)

    assert commands_received(venue) == []
    assert result.guard.refused == ["the session's connection no longer sends through the guard"]
    assert all(by_item(result)[item].status == vec.UNKNOWN for item in range(1, 6))
    assert any("pending order" in line for line in result.left_open)
    assert vec.exit_code(result) == 1


async def test_the_first_guard_refusal_stops_the_commands(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    refusal = vec.Guard._refusal

    def refusing_amends(self, payload):
        if isinstance(payload, oa.ProtoOAAmendOrderReq):
            return "ProtoOAAmendOrderReq refused for the test"
        return refusal(self, payload)

    monkeypatch.setattr(vec.Guard, "_refusal", refusing_amends)
    venue = owner_venue()

    result = await run(venue, tmp_path, answer_wait_secs=1.0)

    assert commands_received(venue) == []
    assert result.guard.refused == ["ProtoOAAmendOrderReq refused for the test"]
    findings = by_item(result)
    for item in range(2, 6):
        assert findings[item].detail == (
            "not run: the guard refused a request; no further command is sent",
        )
    assert findings[9].status == vec.UNKNOWN
    assert any("pending order" in line for line in result.left_open)
    assert any("stays open with its stop-loss" in line for line in result.left_open)
    assert vec.exit_code(result) == 1


async def test_no_secret_is_printed_logged_or_recorded(tmp_path, capsys) -> None:
    venue = owner_venue()
    server = venue.server
    await server.start()
    try:
        result = await vec.run_check(
            settings(tmp_path),
            CREDENTIALS,
            TRADER_LOGIN,
            address=vec.Address(server.host, server.host, server.port, tls=False),
        )
    finally:
        await server.stop()
    print(vec.format_report(result))

    out, err = capsys.readouterr()
    logged = "\n".join(line for _, line in result.logger.lines)
    raw = result.raw.read_text(encoding="utf-8")
    assert result.findings
    assert ">>> " in out
    for secret in FAKE_SECRETS.values():
        for text in (out, err, logged, raw):
            assert secret not in text
    # Account identifiers are allowed at run time, but the report and progress name none.
    for identifier in (str(ACCOUNT_ID), str(TRADER_LOGIN)):
        assert identifier not in out


# -- Watching a partial close --------------------------------------------------------------

# The watched position's volume, in the venue's hundredths of a unit, and the half to close.
WHOLE, HALF = 2, 1
CLOSE, CLOSE_DEAL = 6_900_003, 7_900_002


def watched_position(opened: int) -> om.ProtoOAPosition:
    position = owner_position(opened)
    position.tradeData.volume = WHOLE
    position.trailingStopLoss = False
    return position


def watched_protective(opened: int) -> om.ProtoOAOrder:
    order = protective_order(opened)
    order.tradeData.volume = WHOLE
    order.trailingStopLoss = False
    return order


def watch_venue() -> ExecutionVenue:
    """The fake venue holding the owner's protected position of twice the minimum, alone."""
    venue = ExecutionVenue()
    opened = int(time.time() * 1000) - 600_000
    entry = make_order(ENTRY, POSITION, volume=WHOLE, utc=opened, symbol=US100_SYMBOL_ID)
    entry.orderStatus = om.ORDER_STATUS_FILLED
    deal = make_deal(
        7_900_001, ENTRY, POSITION, side=om.BUY, volume=WHOLE, price=85000.0, ts=opened
    )
    deal.symbolId = US100_SYMBOL_ID
    venue.snapshot.position.append(watched_position(opened))
    venue.snapshot.order.append(watched_protective(opened))
    venue.position_orders = {POSITION: [entry, watched_protective(opened)]}
    venue.position_deals = {POSITION: [deal]}
    venue.deals = [deal]
    return venue


def owner_closes_half(venue: ExecutionVenue, *, replaced: bool = True) -> list[Message]:
    """The owner's close of half the position from the terminal, as the broker reports it.

    The venue's lists change with it; the protective order is reduced only when `replaced`.
    """
    now = int(time.time() * 1000)
    position = venue.snapshot.position[0]
    before = om.ProtoOAPosition()
    before.CopyFrom(position)
    position.tradeData.volume = HALF
    position.utcLastUpdateTimestamp = now
    close = make_order(
        CLOSE, POSITION, side=om.SELL, closing=True, volume=HALF, utc=now, symbol=US100_SYMBOL_ID
    )
    deal = make_deal(CLOSE_DEAL, CLOSE, POSITION, side=om.SELL, volume=HALF, price=85050.0, ts=now)
    deal.symbolId = US100_SYMBOL_ID
    messages = [
        make_event(om.ORDER_ACCEPTED, close, position=before),
        make_event(om.ORDER_FILLED, close, position=position, deal=deal),
    ]
    venue.position_orders[POSITION].append(close)
    venue.position_deals[POSITION].append(deal)
    venue.deals.append(deal)
    if replaced:
        protective = venue.snapshot.order[0]
        protective.tradeData.volume = HALF
        protective.utcLastUpdateTimestamp = now + 1
        messages.append(make_event(om.ORDER_REPLACED, protective, position=position, server=True))
    for message in messages:
        message.ctidTraderAccountId = ACCOUNT_ID
    return messages


def only_reads_received(venue: ExecutionVenue) -> bool:
    # The account list goes over the account client's own pre-connection, not the guarded one.
    reads = vec.READ_REQUESTS | {oa.ProtoOAGetAccountListByAccessTokenReq}
    return all(type(m) in reads for m in venue.server.received)


def test_the_watch_refuses_without_its_flag(
    nothing_may_be_built: list[str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def no_input(_prompt: str = "") -> str:
        nothing_may_be_built.append("input")
        raise AssertionError("input was asked")

    monkeypatch.setattr(builtins, "input", no_input)

    assert vec.main(["--symbol", SYMBOL, "--close-wait-secs", "60"]) == 2
    assert nothing_may_be_built == []
    out, err = capsys.readouterr()
    assert "--watch-partial-close" in err
    assert out == ""


def test_the_two_modes_cannot_be_asked_for_together(nothing_may_be_built: list[str]) -> None:
    argv = ["--symbol", SYMBOL, "--send-commands", "--watch-partial-close"]

    assert vec.main(argv) == 2
    assert nothing_may_be_built == []


def test_the_watch_refuses_a_mistyped_symbol_after_printing_its_plan(
    nothing_may_be_built: list[str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(builtins, "input", lambda _prompt="": "EURUSD")

    assert vec.main(["--symbol", SYMBOL, "--watch-partial-close"]) == 2
    assert nothing_may_be_built == []
    assert vec.watch_plan(SYMBOL, 120.0) in capsys.readouterr().out


def test_the_watch_plan_says_what_to_place_and_that_nothing_is_changed() -> None:
    text = vec.watch_plan(SYMBOL, 90.0)

    assert SYMBOL in text
    assert "changes nothing at the broker" in text
    assert "it sends no order, amend, cancel or close" in text
    assert "at least twice the minimum volume, with a stop-loss and a" in text
    assert "nothing else on that symbol" in text
    assert "listens up to 90 s" in text
    assert "STAYS OPEN" in text and "close it by hand" in text


def test_main_runs_the_watch_with_its_window(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    seen: list = []

    async def canned(settings, credentials, trader_login, **kwargs):
        seen.append((settings, kwargs))
        return vec.Result(title=vec._WATCH_TITLE, findings=[vec.Finding(1, "t", vec.OK)])

    monkeypatch.setattr(builtins, "input", lambda _prompt="": SYMBOL)
    monkeypatch.setattr(vec.get_tokens, "load_env", lambda _path: dict(FAKE_ENV))
    monkeypatch.setattr(vec, "run_check", canned)

    argv = ["--symbol", SYMBOL, "--watch-partial-close", "--close-wait-secs", "45"]
    assert vec.main(argv) == 0
    ((given, kwargs),) = seen
    assert (given.close_wait_secs, kwargs) == (45.0, {"watch": True})
    assert vec._WATCH_TITLE in capsys.readouterr().out


def test_a_watch_stopped_by_hand_says_to_close_the_position(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    async def interrupted(*_args, **_kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(builtins, "input", lambda _prompt="": SYMBOL)
    monkeypatch.setattr(vec.get_tokens, "load_env", lambda _path: dict(FAKE_ENV))
    monkeypatch.setattr(vec, "run_check", interrupted)

    assert vec.main(["--symbol", SYMBOL, "--watch-partial-close"]) == 130
    out = capsys.readouterr().out
    assert "Stopped by hand" in out
    assert "close the position by hand" in out
    assert "pending order" not in out


def test_the_parser_gives_the_watch_two_minutes() -> None:
    args = vec.build_arg_parser().parse_args(["--symbol", SYMBOL, "--watch-partial-close"])

    assert args.watch_partial_close and not args.send_commands
    assert args.close_wait_secs == 120.0


@pytest.mark.parametrize(
    "cls",
    [c for c in all_requests() if c not in vec.READ_REQUESTS],
    ids=lambda c: c.__name__,
)
def test_the_watch_guard_admits_no_command_even_on_the_owners_objects(
    cls: type[Message],
) -> None:
    checked = vec.Guard(vec.Recorder(vec.record_execution.Recording(0)), vec.READ_REQUESTS)
    # Targets let a command through the other mode's guard.
    checked.targets = TARGETS

    with pytest.raises(vec.CommandRefused, match=cls.__name__):
        checked.check(cls())
    assert checked.refused


@pytest.mark.parametrize(
    "command",
    [
        cancel(PENDING),
        amend(orderId=PENDING, volume=MINIMUM, limitPrice=84000.0),
        levels(positionId=POSITION, stopLoss=84899.9, takeProfit=85100.0),
    ],
    ids=lambda c: type(c).__name__,
)
def test_the_watch_guard_refuses_a_command_the_other_mode_would_send(command: Message) -> None:
    guard().check(command)
    checked = vec.Guard(vec.Recorder(vec.record_execution.Recording(0)), vec.READ_REQUESTS)
    checked.targets = TARGETS

    with pytest.raises(vec.CommandRefused, match="is not a request this run may send"):
        checked.check(command)


def test_the_watch_guard_lets_through_authentication_and_reads() -> None:
    checked = vec.Guard(vec.Recorder(vec.record_execution.Recording(0)), vec.READ_REQUESTS)

    for read in (
        oa.ProtoOAApplicationAuthReq(clientId="x", clientSecret="y"),
        oa.ProtoOAAccountAuthReq(ctidTraderAccountId=ACCOUNT_ID, accessToken="z"),
        oa.ProtoOAReconcileReq(ctidTraderAccountId=ACCOUNT_ID),
        oa.ProtoOAOrderListByPositionIdReq(ctidTraderAccountId=ACCOUNT_ID, positionId=POSITION),
    ):
        checked.check(read)
    assert vec.COMMAND_REQUESTS.isdisjoint(vec.READ_REQUESTS)
    assert checked.refused == []


def watch_snapshot(*, orders=None, positions=None) -> oa.ProtoOAReconcileRes:
    found = oa.ProtoOAReconcileRes(ctidTraderAccountId=ACCOUNT_ID)
    found.order.extend([watched_protective(OPENED)] if orders is None else orders)
    found.position.extend([watched_position(OPENED)] if positions is None else positions)
    return found


def test_the_watched_position_is_chosen() -> None:
    chosen = vec.choose_watched(watch_snapshot(), symbol_id=US100_SYMBOL_ID, min_volume=MINIMUM)

    assert chosen == watched_position(OPENED)


@pytest.mark.parametrize(
    ("found", "reason"),
    [
        (watch_snapshot(positions=[]), "0 positions on the symbol"),
        (
            watch_snapshot(positions=[watched_position(OPENED), watched_position(OPENED)]),
            "2 positions",
        ),
        (watch_snapshot(positions=[owner_position(OPENED)]), "under twice the minimum, 0.02"),
        (
            watch_snapshot(positions=[changed_position(volume=WHOLE, stopLoss=None)]),
            "no stop-loss",
        ),
        (
            watch_snapshot(positions=[changed_position(volume=WHOLE, takeProfit=None)]),
            "no take-profit",
        ),
        (
            watch_snapshot(orders=[watched_protective(OPENED), pending_order(OPENED)]),
            "1 pending orders on the symbol, not none",
        ),
    ],
)
def test_anything_but_one_protected_position_alone_is_refused(found, reason: str) -> None:
    refusal = vec.choose_watched(found, symbol_id=US100_SYMBOL_ID, min_volume=MINIMUM)

    assert isinstance(refusal, vec.Refusal)
    assert any(reason in line for line in refusal.reasons), refusal.reasons


def test_the_watched_positions_legs_are_confirmed_in_nautilus() -> None:
    position, protective = watched_position(OPENED), watched_protective(OPENED)
    watched = vec.confirm_watched(position, protective, [protective, ENTRY_ORDER], NAUTILUS[1:])

    assert isinstance(watched, vec.Watched)
    assert (watched.stop_loss.value, watched.take_profit.value) == ("SL", "TP")
    assert watched.protective == protective

    for orders, nautilus, reason in (
        ([protective], NAUTILUS, "names no entry"),
        ([labelled(make_order(ENTRY, POSITION))], NAUTILUS, "opened by the node"),
        ([ENTRY_ORDER], [NAUTILUS[2]], "0 open stop-loss legs"),
    ):
        refusal = vec.confirm_watched(position, protective, orders, nautilus)
        assert isinstance(refusal, vec.Refusal)
        assert any(reason in line for line in refusal.reasons), refusal.reasons


def test_half_is_rounded_down_to_a_volume_step() -> None:
    assert vec.half_of(2, 1) == 1
    assert vec.half_of(300, 100) == 100
    assert vec.half_of(400, 100) == 200


def arrived(t: float, message: Message) -> vec.Inbound:
    return vec.Inbound(t, message, pushed=True)


def deal_event(*, volume: int = HALF, left: int | None = HALF) -> oa.ProtoOAExecutionEvent:
    close = make_order(CLOSE, POSITION, side=om.SELL, closing=True, volume=volume)
    deal = make_deal(CLOSE_DEAL, CLOSE, POSITION, side=om.SELL, volume=volume, price=1.0, ts=1)
    position = None if left is None else changed_position(volume=left)
    return make_event(om.ORDER_FILLED, close, position=position, deal=deal)


def reduction(*, kind: int = om.ORDER_REPLACED, volume: int = HALF, **fields):
    order = watched_protective(OPENED)
    order.tradeData.volume = volume
    for name, value in fields.items():
        setattr(order, name, value)
    return make_event(kind, order, position=changed_position(volume=HALF), server=True)


def test_the_close_and_the_reduction_are_picked_out_of_what_arrived() -> None:
    inbound = [
        vec.Inbound(0.5, deal_event(), pushed=False),
        arrived(0.7, trailing_move(84895.0)),
        arrived(1.0, deal_event()),
        arrived(1.4, reduction()),
    ]
    since = 0.1

    assert vec.closing_deal(inbound, position_id=POSITION, since=since).t == 1.0
    reduced = vec.reduced_protective(inbound, position_id=POSITION, start_volume=WHOLE, since=since)
    assert reduced.t == 1.4
    assert vec.first_drop(inbound, position_id=POSITION, start_volume=WHOLE, since=since).t == 1.0
    assert vec.first_drop(inbound, position_id=POSITION, start_volume=WHOLE, since=1.1).t == 1.4
    assert vec.last_position_volume(inbound, position_id=POSITION, since=since) == HALF
    # A protective order at the whole volume, or one that fills, is no reduction.
    whole = [arrived(1.0, reduction(volume=WHOLE)), arrived(1.1, reduction(kind=om.ORDER_FILLED))]
    assert vec.reduced_protective(whole, position_id=POSITION, start_volume=WHOLE, since=0) is None
    assert vec.closing_deal(whole, position_id=POSITION, since=0) is None


@pytest.mark.parametrize(
    ("deal", "remaining", "status", "line"),
    [
        (arrived(1.0, deal_event()), HALF, vec.OK, "deal volume 0.01, filled 0.01"),
        (arrived(1.0, deal_event(left=None)), HALF, vec.OK, "the event carries no position"),
        (arrived(1.0, deal_event()), 0, vec.DIFFERS, "went from 0.02 to 0"),
        (None, HALF, vec.DIFFERS, "no execution event carried a closing deal"),
        (None, WHOLE, vec.UNKNOWN, "did not drop within 120 s"),
        (None, None, vec.UNKNOWN, "did not drop within 120 s"),
    ],
)
def test_the_close_should_arrive_with_its_deal(deal, remaining, status, line) -> None:
    decided, detail = vec.decide_closing_deal(
        deal, start_volume=WHOLE, remaining=remaining, wait_secs=120.0
    )

    assert decided == status
    assert any(line in text for text in detail), detail


DEAL_AT = arrived(1.0, deal_event())


@pytest.mark.parametrize(
    ("reduced", "deal", "closed", "status", "line"),
    [
        (arrived(1.25, reduction()), DEAL_AT, True, vec.OK, "0.25 s after the deal"),
        (arrived(0.5, reduction()), DEAL_AT, True, vec.DIFFERS, "0.50 s before the deal"),
        (
            arrived(1.25, reduction(kind=om.ORDER_ACCEPTED, orderId=PROTECTIVE + 1)),
            DEAL_AT,
            True,
            vec.DIFFERS,
            "a new protective order id",
        ),
        (arrived(1.25, reduction()), None, True, vec.DIFFERS, "no closing deal arrived"),
        (None, DEAL_AT, True, vec.DIFFERS, "the broker now lists it at 0.02"),
        (None, None, False, vec.UNKNOWN, "did not drop"),
    ],
)
def test_the_reduced_volume_should_arrive_replaced_after_the_deal(
    reduced, deal, closed: bool, status: str, line: str
) -> None:
    decided, detail = vec.decide_reduction_arrives(
        reduced,
        deal,
        closed=closed,
        protective_id=PROTECTIVE,
        listed=watched_protective(OPENED),
        settle_secs=10.0,
        wait_secs=120.0,
    )

    assert decided == status
    assert any(line in text for text in detail), detail
    if status == vec.OK:
        assert "the same protective order id as at the start" in detail


@pytest.mark.parametrize(
    ("reduced", "start", "remaining", "status", "line"),
    [
        (arrived(1.0, reduction()), WHOLE, HALF, vec.OK, "the reduced total, not the volume"),
        (arrived(1.0, reduction(volume=1)), 3, 2, vec.DIFFERS, "the volume closed, not what"),
        (arrived(1.0, reduction(volume=1)), 4, 2, vec.DIFFERS, "neither"),
        (None, WHOLE, HALF, vec.UNKNOWN, "no protective order with a smaller volume"),
        (arrived(1.0, reduction()), WHOLE, None, vec.UNKNOWN, "not read"),
    ],
)
def test_the_reduced_volume_should_be_what_is_left(
    reduced, start: int, remaining, status: str, line: str
) -> None:
    decided, detail = vec.decide_reduced_total(reduced, start_volume=start, remaining=remaining)

    assert decided == status
    assert any(line in text for text in detail), detail


def listed_at(volume: int, **fields) -> om.ProtoOAOrder:
    order = watched_protective(OPENED)
    order.tradeData.volume = volume
    for name, value in fields.items():
        setattr(order, name, value)
    return order


@pytest.mark.parametrize(
    ("reduced", "listed", "status", "line"),
    [
        (arrived(1.0, reduction()), listed_at(HALF), vec.OK, "executedVolume not set"),
        # Set, but agreeing with what is left: the adapter still reads the rest right.
        (None, listed_at(WHOLE, executedVolume=HALF), vec.OK, "executedVolume 0.01"),
        (
            arrived(1.0, reduction(executedVolume=HALF)),
            None,
            vec.DIFFERS,
            "the adapter reads 0 left",
        ),
        (None, listed_at(WHOLE), vec.DIFFERS, "the adapter reads 0.02 left"),
        (None, None, vec.UNKNOWN, "neither pushed smaller nor listed"),
    ],
)
def test_executed_volume_should_leave_remaining_of_reading_what_is_left(
    reduced, listed, status, line
) -> None:
    decided, detail = vec.decide_executed_volume(
        reduced, listed, closed=True, remaining=HALF, wait_secs=120.0
    )

    assert decided == status
    assert any(line in text for text in detail), detail


def test_nothing_closed_settles_nothing_about_executed_volume() -> None:
    decided = vec.decide_executed_volume(
        None, listed_at(WHOLE), closed=False, remaining=WHOLE, wait_secs=120.0
    )

    assert decided[0] == vec.UNKNOWN


def legs(stop=Decimal("0.01"), target=Decimal("0.01"), *, is_open=True) -> list[vec.LegState]:
    return [
        vec.LegState("stop-loss leg", stop, is_open),
        vec.LegState("take-profit leg", target, is_open),
    ]


@pytest.mark.parametrize(
    ("held", "errors", "closed", "status", "line"),
    [
        (legs(), 0, True, vec.OK, "0 ERROR or overfill lines logged"),
        (legs(stop=Decimal("0.02")), 0, True, vec.DIFFERS, "stop-loss leg: quantity 0.02"),
        (legs(), 1, True, vec.DIFFERS, "1 ERROR or overfill lines logged (see stderr)"),
        (legs(is_open=False), 0, True, vec.DIFFERS, ", not open"),
        ([vec.LegState("stop-loss leg", None, False)], 0, True, vec.DIFFERS, "not held"),
        (legs(), 0, False, vec.UNKNOWN, "did not drop"),
    ],
)
def test_the_legs_should_follow_the_remaining_volume(held, errors, closed, status, line) -> None:
    decided, detail = vec.decide_legs_follow(
        held, closed=closed, remaining=HALF, errors=errors, wait_secs=120.0
    )

    assert decided == status
    assert any(line in text for text in detail), detail


@pytest.mark.parametrize(
    ("remaining", "last_event", "closed", "status", "line"),
    [
        (HALF, HALF, True, vec.OK, "the broker lists 0.01"),
        (HALF, None, True, vec.OK, "from 0.02, 0.01 asked to close"),
        (HALF, WHOLE, True, vec.DIFFERS, "the last execution event carried 0.02"),
        (0, 0, True, vec.DIFFERS, "the whole position was closed"),
        (WHOLE, HALF, True, vec.DIFFERS, "still lists the whole volume"),
        (WHOLE, None, False, vec.UNKNOWN, "did not drop"),
        (None, HALF, True, vec.UNKNOWN, "not read at the end"),
    ],
)
def test_the_remaining_volume_is_read_from_the_broker(
    remaining, last_event, closed, status, line
) -> None:
    decided, detail = vec.decide_remaining(
        start_volume=WHOLE,
        half=HALF,
        closed=closed,
        remaining=remaining,
        last_event=last_event,
        wait_secs=120.0,
    )

    assert decided == status
    assert any(line in text for text in detail), detail


def test_what_is_left_of_the_watched_position_tells_the_owner_to_close_it() -> None:
    left = watch_snapshot(positions=[changed_position(volume=HALF, trailingStopLoss=False)])

    (line,) = vec.left_open_watched(left, symbol=SYMBOL, position_id=POSITION)
    assert line == (
        f"The position on {SYMBOL} stays open at 0.01 with its stop-loss at 84900.0, take-profit "
        "at 85100.0. Close it by hand in the terminal."
    )
    gone = vec.left_open_watched(watch_snapshot(positions=[]), symbol=SYMBOL, position_id=POSITION)
    assert gone == (f"The position on {SYMBOL} is closed; nothing is left open.",)
    (unread,) = vec.left_open_watched(None, symbol=SYMBOL, position_id=POSITION)
    assert "close the position by hand" in unread


def test_the_report_names_what_each_finding_settles() -> None:
    result = vec.Result(
        title=vec._WATCH_TITLE,
        findings=[vec.Finding(1, "first", vec.OK, ("detail",), "the open question")],
    )

    text = vec.format_report(result)

    assert text.startswith(vec._WATCH_TITLE)
    assert "OK       1. first\n         detail\n         settles: the open question" in text


async def test_a_watch_reports_the_owners_partial_close(tmp_path) -> None:
    venue = watch_venue()

    result = await run(venue, tmp_path, while_listening=owner_closes_half, watch=True)

    assert (result.refusal, result.failure, result.guard.refused) == ((), None, [])
    assert result.guard.allowed == vec.READ_REQUESTS
    findings = by_item(result)
    statuses = {item: f.status for item, f in findings.items()}
    assert statuses == dict.fromkeys(range(1, 7), vec.OK), result.findings
    assert all(f.settles for f in result.findings)
    assert findings[1].detail[1:] == (
        "deal volume 0.01, filled 0.01",
        "the event carries the position, at 0.01",
    )
    assert "ORDER_REPLACED of a closing STOP_LOSS_TAKE_PROFIT order" in findings[2].detail[0]
    assert findings[2].detail[1].endswith("s after the deal")
    assert findings[2].detail[2] == "the same protective order id as at the start"
    assert findings[3].detail[1] == "the reduced total, not the volume closed"
    assert findings[5].detail[:2] == (
        "stop-loss leg: quantity 0.01",
        "take-profit leg: quantity 0.01",
    )
    assert "0 ERROR or overfill lines logged" in findings[5].detail
    assert findings[6].detail[1] == "the broker lists 0.01"
    assert vec.exit_code(result) == 0

    assert only_reads_received(venue)
    assert commands_received(venue) == []
    assert result.left_open == (
        f"The position on {SYMBOL} stays open at 0.01 with its stop-loss at 84900.0, take-profit "
        "at 85100.0. Close it by hand in the terminal.",
    )
    assert result.raw.name.startswith("watch_partial_close-")
    recording, _, _ = vec.record_execution.decode_raw(result.raw.read_bytes())
    kinds = {
        m.executionType for m in recording.messages() if isinstance(m, oa.ProtoOAExecutionEvent)
    }
    assert {om.ORDER_FILLED, om.ORDER_REPLACED} <= kinds
    assert vec.format_report(result).startswith(vec._WATCH_TITLE)


async def test_a_watch_where_the_protective_order_is_not_reduced_says_so(tmp_path) -> None:
    venue = watch_venue()

    result = await run(
        venue,
        tmp_path,
        while_listening=lambda v: owner_closes_half(v, replaced=False),
        watch=True,
    )

    statuses = {item: f.status for item, f in by_item(result).items()}
    assert statuses == {
        1: vec.OK,
        2: vec.DIFFERS,
        3: vec.UNKNOWN,
        4: vec.DIFFERS,
        5: vec.DIFFERS,
        6: vec.OK,
    }, result.findings
    assert by_item(result)[2].detail[1] == "the broker now lists it at 0.02"
    assert vec.exit_code(result) == 1
    assert commands_received(venue) == []
    assert "stays open at 0.01" in result.left_open[0]


async def test_a_watch_where_the_owner_closes_nothing_settles_nothing(tmp_path) -> None:
    venue = watch_venue()

    result = await run(venue, tmp_path, close_wait_secs=0.5, watch=True)

    assert [f.status for f in result.findings] == [vec.UNKNOWN] * 6, result.findings
    assert all("did not drop within 0.5 s" in f.detail[-1] for f in result.findings[:2])
    assert vec.exit_code(result) == 0
    assert only_reads_received(venue)
    assert "stays open at 0.02" in result.left_open[0]


def watched_by_the_node(venue: ExecutionVenue) -> None:
    labelled(venue.position_orders[POSITION][0])


def a_pending_order_beside(venue: ExecutionVenue) -> None:
    venue.snapshot.order.append(pending_order(OPENED))


def a_second_position(venue: ExecutionVenue) -> None:
    venue.snapshot.position.append(changed_position(volume=WHOLE, positionId=POSITION + 1))


def at_the_minimum(venue: ExecutionVenue) -> None:
    venue.snapshot.position[0].tradeData.volume = MINIMUM


def without_a_take_profit(venue: ExecutionVenue) -> None:
    venue.snapshot.position[0].ClearField("takeProfit")


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        (lists_no_position, "0 positions"),
        (a_second_position, "2 positions"),
        (at_the_minimum, "under twice the minimum"),
        (without_a_take_profit, "no take-profit"),
        (a_pending_order_beside, "1 pending orders"),
        (watched_by_the_node, "opened by the node"),
    ],
)
async def test_a_watch_refuses_unless_it_finds_exactly_the_owners_position(
    tmp_path, change, reason: str
) -> None:
    venue = watch_venue()
    change(venue)

    result = await run(venue, tmp_path, watch=True)

    assert result.findings == []
    assert any(reason in line for line in result.refusal), result.refusal
    assert vec.exit_code(result) == 1
    assert only_reads_received(venue)
    assert "Refused to act; nothing was changed at the broker:" in vec.format_report(result)


async def test_a_watch_that_fails_midway_still_says_what_is_left_open(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def failing(self, *_args):
        raise RuntimeError("the watch broke")

    monkeypatch.setattr(vec.Watch, "_watch", failing)

    result = await run(watch_venue(), tmp_path, watch=True)

    assert result.failure == "RuntimeError"
    assert any("stays open at 0.02" in line for line in result.left_open)
    assert vec.exit_code(result) == 1
    assert "The run ended early on RuntimeError" in vec.format_report(result)


async def test_a_watch_stops_listening_at_the_first_guard_refusal(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(vec.Guard, "covers", lambda _self, _connection: False)
    venue = watch_venue()
    loop = asyncio.get_running_loop()
    started = loop.time()

    result = await run(venue, tmp_path, close_wait_secs=30.0, watch=True)

    assert loop.time() - started < 20.0
    assert result.guard.refused == ["the session's connection no longer sends through the guard"]
    assert [f.status for f in result.findings] == [vec.UNKNOWN] * 6
    assert "stays open at 0.02" in result.left_open[0]
    assert vec.exit_code(result) == 1
    assert only_reads_received(venue)


async def test_a_watch_prints_logs_and_records_no_secret(tmp_path, capsys) -> None:
    venue = watch_venue()
    server = venue.server
    await server.start()
    listening = asyncio.Event()
    try:
        task = asyncio.create_task(
            vec.run_check(
                settings(tmp_path),
                CREDENTIALS,
                TRADER_LOGIN,
                address=vec.Address(server.host, server.host, server.port, tls=False),
                listening=listening,
                watch=True,
            ),
        )
        await wait_until(lambda: listening.is_set() or task.done(), timeout_secs=60.0)
        for message in owner_closes_half(venue):
            await server.push(message)
        result = await asyncio.wait_for(task, 60.0)
    finally:
        await server.stop()
    print(vec.format_report(result))

    out, err = capsys.readouterr()
    logged = "\n".join(line for _, line in result.logger.lines)
    raw = result.raw.read_text(encoding="utf-8")
    assert [f.status for f in result.findings] == [vec.OK] * 6
    assert ">>> " in out and "Now close 0.01 of the position by hand in the terminal" in out
    for secret in FAKE_SECRETS.values():
        for text in (out, err, logged, raw):
            assert secret not in text
    for identifier in (str(ACCOUNT_ID), str(TRADER_LOGIN)):
        assert identifier not in out
