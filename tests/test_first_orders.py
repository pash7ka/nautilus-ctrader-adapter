"""Tests for the owner's first-orders script.

`scripts/first_orders.py` sends real orders, so nothing here gets near a node or a connection:
the refusal tests check that nothing is built before the owner has confirmed twice, and the
step driver and the log lines are pure and fed hand-made Nautilus events.
"""

from __future__ import annotations

import builtins
import importlib.util
import json
import pathlib
import sys
import time
from decimal import Decimal

import pytest
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import MessageBus, TestClock
from nautilus_trader.common.factories import OrderFactory
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OmsType, OrderSide
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.identifiers import (
    AccountId,
    PositionId,
    StrategyId,
    TraderId,
    VenueOrderId,
)
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.position import Position
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.test_kit.stubs.events import TestEventStubs

from nautilus_ctrader import (
    CTRADER_VENUE,
    CTraderAccountActivity,
    CTraderLiveDataClientFactory,
    CTraderLiveExecClientFactory,
)

_SCRIPT_PATH = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "first_orders.py"
_SPEC = importlib.util.spec_from_file_location("first_orders", _SCRIPT_PATH)
first_orders = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = first_orders
_SPEC.loader.exec_module(first_orders)

fo = first_orders

ACCOUNT_ID = AccountId("CTRADER-001")
FAKE_LOGIN = 9_000_001
EURUSD = TestInstrumentProvider.default_fx_ccy("EURUSD", venue=CTRADER_VENUE)
DISTANCE = Decimal("0.00100")
WAIT_SECS = 600.0
# The test instrument trades in steps of 1 unit from a minimum of 1000.
SIZES = fo.bracket_sizes(EURUSD)
FIRST = Quantity.from_int(1000)
SECOND = Quantity.from_int(1001)


def new_steps() -> fo.Steps:
    return fo.Steps(distance=DISTANCE, manual_wait_secs=WAIT_SECS, sizes=SIZES)


# -- Refusals before anything is built ----------------------------------------------------


@pytest.fixture
def nothing_may_be_built(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Every way the script could reach an account, replaced by a call recorder."""
    calls: list[str] = []

    def recorder(name: str):
        def call(*_args, **_kwargs):
            calls.append(name)
            raise AssertionError(f"{name} was called")

        return call

    monkeypatch.setattr(
        CTraderLiveDataClientFactory, "create", staticmethod(recorder("data factory"))
    )
    monkeypatch.setattr(
        CTraderLiveExecClientFactory, "create", staticmethod(recorder("exec factory"))
    )
    monkeypatch.setattr(fo, "build_node", recorder("build_node"))
    monkeypatch.setattr(fo.get_tokens, "load_env", recorder("load_env"))
    return calls


def test_refuses_without_the_flag(
    nothing_may_be_built: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    def no_input(_prompt: str = "") -> str:
        nothing_may_be_built.append("input")
        raise AssertionError("input was asked")

    monkeypatch.setattr(builtins, "input", no_input)

    assert fo.main(["--symbol", "EURUSD", "--distance", "0.00100"]) == 2
    assert nothing_may_be_built == []


def test_refuses_a_mistyped_symbol(
    nothing_may_be_built: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(builtins, "input", lambda _prompt="": "GBPUSD")

    argv = ["--symbol", "EURUSD", "--distance", "0.00100", "--send-live-orders"]
    assert fo.main(argv) == 2
    assert nothing_may_be_built == []


def test_refuses_an_expired_access_token(
    nothing_may_be_built: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(builtins, "input", lambda _prompt="": "EURUSD")
    expired = {**FAKE_ENV, "CTRADER_TOKEN_EXPIRES_AT": str(int(time.time()) - 60)}
    monkeypatch.setattr(fo.get_tokens, "load_env", lambda _path: dict(expired))

    argv = ["--symbol", "EURUSD", "--distance", "0.00100", "--send-live-orders"]
    assert fo.main(argv) == 2
    assert nothing_may_be_built == []


@pytest.mark.parametrize(("expiry", "noticed"), [(None, True), ("4102444800", False)])
def test_an_unknown_token_expiry_is_noticed(
    expiry: str | None,
    noticed: bool,
    nothing_may_be_built: list[str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(builtins, "input", lambda _prompt="": "EURUSD")
    # No client id, so the run stops at the config, before anything is built.
    env = {key: value for key, value in FAKE_ENV.items() if key != "CTRADER_CLIENT_ID"}
    if expiry is not None:
        env["CTRADER_TOKEN_EXPIRES_AT"] = expiry
    monkeypatch.setattr(fo.get_tokens, "load_env", lambda _path: dict(env))

    argv = ["--symbol", "EURUSD", "--distance", "0.00100", "--send-live-orders"]
    assert fo.main(argv) == 1
    assert nothing_may_be_built == []
    assert ("expiry is unknown" in capsys.readouterr().err) == noticed


def test_a_token_expiry_that_is_absent_or_ahead_is_not_expired() -> None:
    assert not fo.token_expired(FAKE_ENV, now_secs=1_000.0)
    assert not fo.token_expired({**FAKE_ENV, "CTRADER_TOKEN_EXPIRES_AT": "1001"}, 1_000.0)
    assert fo.token_expired({**FAKE_ENV, "CTRADER_TOKEN_EXPIRES_AT": "1000"}, 1_000.0)
    with pytest.raises(fo.MissingCredentials):
        fo.token_expired({**FAKE_ENV, "CTRADER_TOKEN_EXPIRES_AT": "soon"}, 1_000.0)


def test_the_parser_reads_the_distance_as_a_decimal() -> None:
    args = fo.build_arg_parser().parse_args(
        ["--symbol", "EURUSD", "--distance", "0.00100", "--send-live-orders"],
    )

    assert args.distance == Decimal("0.00100")
    assert args.manual_wait_secs == 600.0
    assert args.log_dir == fo._REPO_ROOT / "tests" / "recordings"


@pytest.mark.parametrize("distance", ["0", "-0.001", "abc"])
def test_the_parser_refuses_a_distance_that_is_not_positive(distance: str) -> None:
    with pytest.raises(SystemExit):
        fo.build_arg_parser().parse_args(["--symbol", "EURUSD", "--distance", distance])


def test_the_second_bracket_is_one_size_step_over_the_minimum() -> None:
    assert SIZES.first == EURUSD.min_quantity == FIRST
    assert SIZES.second == SECOND
    assert SIZES.partial_close == "1 EUR (0.001 lots)"


@pytest.mark.parametrize("wait", ["0", "-1", "inf", "nan", "abc"])
def test_the_parser_refuses_a_manual_wait_that_is_not_finite_and_positive(wait: str) -> None:
    with pytest.raises(SystemExit):
        fo.build_arg_parser().parse_args(
            ["--symbol", "EURUSD", "--distance", "0.001", "--manual-wait-secs", wait],
        )


FAKE_ENV = {
    "CTRADER_CLIENT_ID": "fake-client-id",
    "CTRADER_CLIENT_SECRET": "fake-client-secret",
    "CTRADER_ACCESS_TOKEN": "fake-access-token",
    "CTRADER_REFRESH_TOKEN": "fake-refresh-token",
    "CTRADER_TRADER_LOGIN": str(FAKE_LOGIN),
}


def test_the_node_loads_the_one_symbol_and_never_refreshes_the_token() -> None:
    config = fo.node_config(FAKE_ENV, "EURUSD")

    data = config.data_clients["CTRADER"]
    execution = config.exec_clients["CTRADER"]
    assert data.instrument_provider.load_ids == frozenset(["EURUSD.CTRADER"])
    assert data.trader_login == execution.trader_login == FAKE_LOGIN
    assert data.refresh_token is None
    assert execution.refresh_token is None


@pytest.mark.parametrize(
    ("env", "named"),
    [
        ({k: v for k, v in FAKE_ENV.items() if k != "CTRADER_TRADER_LOGIN"}, "TRADER_LOGIN"),
        ({**FAKE_ENV, "CTRADER_TRADER_LOGIN": "not-a-login"}, "not a number"),
    ],
)
def test_the_node_config_refuses_bad_credentials_without_echoing_them(env, named) -> None:
    with pytest.raises(fo.MissingCredentials) as raised:
        fo.node_config(env, "EURUSD")

    assert named in str(raised.value)
    assert "not-a-login" not in str(raised.value)
    assert "fake-access-token" not in str(raised.value)


# -- The step driver ----------------------------------------------------------------------


class Venue:
    """Hand-made Nautilus events for one instrument, with the fixed Nautilus account id."""

    def __init__(self) -> None:
        self.factory = OrderFactory(
            trader_id=TraderId("TESTER-001"),
            strategy_id=StrategyId("S-001"),
            clock=TestClock(),
        )
        self._venue_ids = 0
        self.positions: dict[PositionId, Position] = {}

    def quote(self, bid: str, ask: str) -> QuoteTick:
        return QuoteTick(
            instrument_id=EURUSD.id,
            bid_price=Price.from_str(bid),
            ask_price=Price.from_str(ask),
            bid_size=Quantity.from_int(0),
            ask_size=Quantity.from_int(0),
            ts_event=0,
            ts_init=0,
        )

    def bracket(self, action: fo.SubmitBracket):
        orders = self.factory.bracket(
            instrument_id=EURUSD.id,
            order_side=OrderSide.BUY,
            quantity=action.quantity,
            sl_trigger_price=EURUSD.make_price(action.stop_loss),
            tp_price=EURUSD.make_price(action.take_profit),
        )
        entry, stop_loss, take_profit = orders.orders
        submitted = fo.BracketSubmitted(
            entry=entry.client_order_id,
            stop_loss=stop_loss.client_order_id,
            take_profit=take_profit.client_order_id,
        )
        return entry, stop_loss, take_profit, submitted

    def accepted(self, order):
        self._venue_ids += 1
        return TestEventStubs.order_accepted(
            order, account_id=ACCOUNT_ID, venue_order_id=VenueOrderId(str(self._venue_ids))
        )

    def opened(self, entry, position_id: str, price: str):
        fill = TestEventStubs.order_filled(
            entry,
            EURUSD,
            account_id=ACCOUNT_ID,
            position_id=PositionId(position_id),
            last_px=Price.from_str(price),
        )
        position = Position(EURUSD, fill)
        self.positions[position.id] = position
        return fill, TestEventStubs.position_opened(position)

    def closed(self, position_id: str, price: str):
        position = self.positions[PositionId(position_id)]
        close = self.factory.market(EURUSD.id, OrderSide.SELL, position.quantity, reduce_only=True)
        fill = TestEventStubs.order_filled(
            close,
            EURUSD,
            account_id=ACCOUNT_ID,
            position_id=position.id,
            last_px=Price.from_str(price),
        )
        position.apply(fill)
        return fill, TestEventStubs.position_closed(position)


def activity(action: str, kind: str = "manual_change") -> CTraderAccountActivity:
    return CTraderAccountActivity(
        kind=kind,
        symbol="EURUSD",
        subject="position",
        side="BUY",
        volume=Decimal(1000),
        action=action,
        ts_event=0,
        ts_init=0,
    )


def run_to_manual_wait(steps: fo.Steps, venue: Venue) -> None:
    """Drive `steps` through the first bracket and the second one's acceptance."""
    (submit,) = steps.on(venue.quote("1.10000", "1.10010"))
    entry, stop_loss, take_profit, submitted = venue.bracket(submit)
    steps.on(submitted)
    steps.on(venue.accepted(entry))
    for event in venue.opened(entry, "P-1", "1.10010"):
        steps.on(event)
    steps.on(venue.accepted(stop_loss))
    steps.on(venue.accepted(take_profit))
    steps.on(TestEventStubs.order_canceled(take_profit, account_id=ACCOUNT_ID))
    fill, closed = venue.closed("P-1", "1.10000")
    steps.on(fill)
    (submit,) = steps.on(closed)

    entry, stop_loss, take_profit, submitted = venue.bracket(submit)
    steps.on(submitted)
    steps.on(venue.accepted(entry))
    for event in venue.opened(entry, "P-2", "1.10020"):
        steps.on(event)
    steps.on(venue.accepted(stop_loss))
    (prompt,) = steps.on(venue.accepted(take_profit))
    assert isinstance(prompt, fo.Prompt)
    assert prompt.wait_secs == WAIT_SECS


def test_steps_run_the_fixed_sequence() -> None:
    steps = new_steps()
    venue = Venue()

    # 1-2. A fresh quote, then a bracket measured from its ask.
    assert steps.on(venue.quote("1.10000", "1.10010")) == [
        fo.SubmitBracket(FIRST, stop_loss=Decimal("1.09910"), take_profit=Decimal("1.10110")),
    ]
    entry, stop_loss, take_profit, submitted = venue.bracket(
        fo.SubmitBracket(FIRST, stop_loss=Decimal("1.09910"), take_profit=Decimal("1.10110")),
    )
    assert steps.on(submitted) == []
    assert steps.on(venue.accepted(entry)) == []
    for event in venue.opened(entry, "P-1", "1.10010"):
        assert steps.on(event) == []

    # 3. Both legs accepted: cancel the take-profit.
    assert steps.on(venue.accepted(stop_loss)) == []
    assert steps.on(venue.accepted(take_profit)) == [fo.CancelLeg(take_profit.client_order_id)]

    # 4. The take-profit cancelled: close the position.
    assert steps.on(TestEventStubs.order_canceled(take_profit, account_id=ACCOUNT_ID)) == [
        fo.ClosePosition(PositionId("P-1")),
    ]

    # 5. The position closed: a second bracket from the latest quote.
    assert steps.on(venue.quote("1.10010", "1.10020")) == []
    fill, closed = venue.closed("P-1", "1.10000")
    assert steps.on(fill) == []
    second = fo.SubmitBracket(SECOND, stop_loss=Decimal("1.09920"), take_profit=Decimal("1.10120"))
    assert steps.on(closed) == [second]
    assert steps.on(TestEventStubs.order_canceled(stop_loss, account_id=ACCOUNT_ID)) == []

    # 6. The second bracket's legs accepted: the owner is asked to act by hand.
    entry, stop_loss, take_profit, submitted = venue.bracket(second)
    assert steps.on(submitted) == []
    assert steps.on(venue.accepted(entry)) == []
    for event in venue.opened(entry, "P-2", "1.10020"):
        assert steps.on(event) == []
    assert steps.on(venue.accepted(stop_loss)) == []
    (prompt,) = steps.on(venue.accepted(take_profit))
    assert isinstance(prompt, fo.Prompt)
    assert prompt.wait_secs == WAIT_SECS
    assert "close 1 EUR (0.001 lots) of it" in prompt.text

    # 7-8. Both manual changes seen: close the rest, then stop.
    assert steps.on(activity("level_moved")) == []
    assert steps.on(activity("partially_closed")) == [fo.ClosePosition(PositionId("P-2"))]
    fill, closed = venue.closed("P-2", "1.10030")
    assert steps.on(fill) == []
    assert steps.on(closed) == [fo.Done()]
    assert steps.finished


def test_steps_wait_for_both_manual_activities() -> None:
    steps = new_steps()
    run_to_manual_wait(steps, Venue())

    assert steps.on(activity("partially_closed")) == []
    assert steps.on(activity("level_moved", kind="unloaded_symbol")) == []
    assert steps.on(activity("partially_closed")) == []
    assert steps.on(activity("level_moved")) == [fo.ClosePosition(PositionId("P-2"))]
    assert steps.on(fo.ManualWaitExpired()) == []


def test_steps_close_the_rest_when_the_manual_wait_runs_out() -> None:
    steps = new_steps()
    run_to_manual_wait(steps, Venue())
    assert steps.on(activity("level_moved")) == []

    prompt, close = steps.on(fo.ManualWaitExpired())

    assert isinstance(prompt, fo.Prompt)
    assert "partially_closed" in prompt.text
    assert "level_moved" not in prompt.text
    assert close == fo.ClosePosition(PositionId("P-2"))


def test_steps_stop_when_the_owner_closes_everything_by_hand() -> None:
    steps = new_steps()
    venue = Venue()
    run_to_manual_wait(steps, venue)

    fill, closed = venue.closed("P-2", "1.10030")
    steps.on(fill)
    prompt, done = steps.on(closed)

    assert isinstance(prompt, fo.Prompt)
    assert done == fo.Done()
    assert steps.on(fo.ManualWaitExpired()) == []


def test_steps_stop_on_a_rejection() -> None:
    steps = new_steps()
    venue = Venue()
    (submit,) = steps.on(venue.quote("1.10000", "1.10010"))
    entry, stop_loss, _take_profit, submitted = venue.bracket(submit)
    steps.on(submitted)

    prompt, done = steps.on(TestEventStubs.order_rejected(entry, account_id=ACCOUNT_ID))

    assert isinstance(prompt, fo.Prompt)
    assert "ORDER_REJECTED" in prompt.text
    assert done == fo.Done()
    assert steps.finished
    # The legs the adapter cancels after the rejection, and later quotes, change nothing.
    assert steps.on(TestEventStubs.order_canceled(stop_loss, account_id=ACCOUNT_ID)) == []
    assert steps.on(venue.quote("1.10000", "1.10010")) == []


def test_steps_stop_when_the_entry_ends_without_a_fill() -> None:
    steps = new_steps()
    venue = Venue()
    (submit,) = steps.on(venue.quote("1.10000", "1.10010"))
    entry, *_legs, submitted = venue.bracket(submit)
    steps.on(submitted)

    prompt, done = steps.on(TestEventStubs.order_canceled(entry, account_id=ACCOUNT_ID))

    assert isinstance(prompt, fo.Prompt)
    assert done == fo.Done()


def test_steps_stop_when_the_first_position_closes_before_the_script_closes_it() -> None:
    steps = new_steps()
    venue = Venue()
    (submit,) = steps.on(venue.quote("1.10000", "1.10010"))
    entry, stop_loss, take_profit, submitted = venue.bracket(submit)
    steps.on(submitted)
    for event in venue.opened(entry, "P-1", "1.10010"):
        steps.on(event)
    steps.on(venue.accepted(stop_loss))
    steps.on(venue.accepted(take_profit))
    fill, closed = venue.closed("P-1", "1.09910")
    steps.on(fill)

    prompt, done = steps.on(closed)

    assert isinstance(prompt, fo.Prompt)
    assert done == fo.Done()
    assert steps.on(TestEventStubs.order_canceled(take_profit, account_id=ACCOUNT_ID)) == []


def second_bracket_without_its_position(steps: fo.Steps, venue: Venue) -> None:
    """Run to the manual stage, with no PositionOpened ever reported for the second entry."""
    (submit,) = steps.on(venue.quote("1.10000", "1.10010"))
    entry, stop_loss, take_profit, submitted = venue.bracket(submit)
    steps.on(submitted)
    for event in venue.opened(entry, "P-1", "1.10010"):
        steps.on(event)
    steps.on(venue.accepted(stop_loss))
    steps.on(venue.accepted(take_profit))
    steps.on(TestEventStubs.order_canceled(take_profit, account_id=ACCOUNT_ID))
    _fill, closed = venue.closed("P-1", "1.10000")
    (submit,) = steps.on(closed)
    _entry, stop_loss, take_profit, submitted = venue.bracket(submit)
    steps.on(submitted)
    steps.on(venue.accepted(stop_loss))
    (prompt,) = steps.on(venue.accepted(take_profit))
    assert prompt.wait_secs == WAIT_SECS


def test_steps_end_the_run_when_the_second_position_was_never_reported() -> None:
    steps = new_steps()
    second_bracket_without_its_position(steps, Venue())
    steps.on(activity("level_moved"))

    prompt, done = steps.on(activity("partially_closed"))

    assert isinstance(prompt, fo.Prompt)
    assert "terminal" in prompt.text
    assert done == fo.Done()
    assert steps.finished


def test_steps_end_the_run_when_the_wait_runs_out_with_no_second_position() -> None:
    steps = new_steps()
    second_bracket_without_its_position(steps, Venue())

    prompt, done = steps.on(fo.ManualWaitExpired())

    assert isinstance(prompt, fo.Prompt)
    assert "terminal" in prompt.text
    assert done == fo.Done()
    assert steps.finished


# -- The event log ------------------------------------------------------------------------


def test_log_lines_carry_no_account_identifiers(tmp_path: pathlib.Path) -> None:
    venue = Venue()
    entry, stop_loss, _take_profit, _submitted = venue.bracket(
        fo.SubmitBracket(FIRST, stop_loss=Decimal("1.09910"), take_profit=Decimal("1.10110")),
    )
    fill, opened = venue.opened(entry, "P-1", "1.10010")
    rejected = OrderRejected(
        trader_id=stop_loss.trader_id,
        strategy_id=stop_loss.strategy_id,
        instrument_id=stop_loss.instrument_id,
        client_order_id=stop_loss.client_order_id,
        account_id=ACCOUNT_ID,
        reason=f"TRADING_BAD_STOPS: account {FAKE_LOGIN} refused",
        ts_event=0,
        event_id=fill.id,
        ts_init=0,
    )
    events = [venue.accepted(entry), fill, opened, rejected, activity("level_moved")]

    path = tmp_path / "first_orders.jsonl"
    with fo.EventLog(path, hidden=(str(FAKE_LOGIN),)) as log:
        for event in events:
            log.write(event, status="FILLED")

    text = path.read_text(encoding="utf-8")
    assert ACCOUNT_ID.value not in text
    assert str(FAKE_LOGIN) not in text
    lines = [json.loads(line) for line in text.splitlines()]
    assert [line["event"] for line in lines] == [
        "OrderAccepted",
        "OrderFilled",
        "PositionOpened",
        "OrderRejected",
        "CTraderAccountActivity",
    ]
    assert lines[0]["venue_order_id"] == "1"
    assert lines[1]["client_order_id"] == entry.client_order_id.value
    assert lines[1]["last_px"] == "1.10010"
    assert lines[1]["last_qty"] == str(EURUSD.min_quantity)
    assert lines[1]["order_side"] == "BUY"
    assert lines[1]["status"] == "FILLED"
    assert lines[2]["position_id"] == "P-1"
    assert lines[3]["reason"] == "TRADING_BAD_STOPS: account <hidden> refused"
    assert lines[4]["action"] == "level_moved"
    assert all("time" in line for line in lines)


# -- The driver, with every command it sends recorded instead ------------------------------


class RecordingDriver:
    """A registered `_Driver` whose order commands and shutdown are recorded, not sent."""

    def __init__(self, log: fo.EventLog) -> None:
        self.clock = TestClock()
        self.cache = Cache()
        self.cache.add_instrument(EURUSD)
        msgbus = MessageBus(trader_id=TraderId("TESTER-001"), clock=self.clock)
        self.driver = fo._Driver(EURUSD.id, distance=DISTANCE, manual_wait_secs=WAIT_SECS, log=log)
        self.driver.register(
            trader_id=TraderId("TESTER-001"),
            portfolio=Portfolio(msgbus=msgbus, cache=self.cache, clock=self.clock),
            msgbus=msgbus,
            cache=self.cache,
            clock=self.clock,
        )
        self.sent: list[tuple[str, object]] = []
        self.driver.submit_order_list = self._submit_order_list
        self.driver.cancel_order = lambda order: self.sent.append(("cancel", order))
        self.driver.close_position = lambda position: self.sent.append(("close", position))
        self.driver.shutdown_system = lambda reason=None: self.sent.append(("shutdown", reason))

    def _submit_order_list(self, orders) -> None:
        for order in orders.orders:
            self.cache.add_order(order)
        self.sent.append(("bracket", orders))

    def open(self, venue: Venue, entry, position_id: str, price: str) -> None:
        fill, opened = venue.opened(entry, position_id, price)
        self.cache.add_position(venue.positions[PositionId(position_id)], OmsType.HEDGING)
        self.driver.on_event(fill)
        self.driver.on_event(opened)

    def advance(self, secs: float) -> None:
        for handler in self.clock.advance_time(self.clock.timestamp_ns() + int(secs * 1e9)):
            handler.handle()


def test_the_driver_carries_out_the_sequence(tmp_path: pathlib.Path, capsys) -> None:
    path = tmp_path / "first_orders.jsonl"
    venue = Venue()
    with fo.EventLog(path, hidden=(str(FAKE_LOGIN),)) as log:
        r = RecordingDriver(log)
        r.driver.on_start()
        r.driver.on_quote_tick(venue.quote("1.10000", "1.10010"))

        (kind, orders), *_ = r.sent
        assert kind == "bracket"
        entry, stop_loss, take_profit = orders.orders
        assert entry.side == OrderSide.BUY
        assert entry.quantity == EURUSD.min_quantity
        assert stop_loss.trigger_price == Price.from_str("1.09910")
        assert take_profit.price == Price.from_str("1.10110")

        r.open(venue, entry, "P-1", "1.10010")
        r.driver.on_event(venue.accepted(stop_loss))
        r.driver.on_event(venue.accepted(take_profit))
        assert r.sent[-1] == ("cancel", take_profit)

        r.driver.on_event(TestEventStubs.order_canceled(take_profit, account_id=ACCOUNT_ID))
        assert r.sent[-1] == ("close", r.cache.position(PositionId("P-1")))

        _fill, closed = venue.closed("P-1", "1.10000")
        r.driver.on_event(closed)
        kind, orders = r.sent[-1]
        assert kind == "bracket"
        entry, stop_loss, take_profit = orders.orders
        assert entry.quantity == stop_loss.quantity == take_profit.quantity == SECOND
        r.open(venue, entry, "P-2", "1.10010")
        r.driver.on_event(venue.accepted(stop_loss))
        r.driver.on_event(venue.accepted(take_profit))
        assert "close 1 EUR (0.001 lots) of it" in capsys.readouterr().out

        # Nothing by hand within the wait: the rest is closed anyway.
        r.advance(WAIT_SECS + 1)
        assert r.sent[-1] == ("close", r.cache.position(PositionId("P-2")))
        _fill, closed = venue.closed("P-2", "1.10000")
        r.driver.on_event(closed)

    assert r.sent[-1][0] == "shutdown"
    assert r.driver.finished
    events = [json.loads(line)["event"] for line in path.read_text(encoding="utf-8").splitlines()]
    assert events.count("PositionClosed") == 2
