"""Tests for the read-only live verification script.

`scripts/verify_live_data.py` is developer tooling, not part of the installed package, so it is
imported by file path - the same pattern `tests/test_record_fixtures.py` uses. Everything the
script decides *from* an observation is checked here against a canned one; what the live run
alone can show is the observation itself.

The read-only guarantee is checked twice: that the requester refuses a payload outside its
allow-list, and that the script's own source never names an order request at all.
"""

from __future__ import annotations

import asyncio
import functools
import importlib.util
import pathlib
import sys
import time

import pytest

from nautilus_ctrader.common.connection import CTraderConnection
from nautilus_ctrader.common.errors import CTraderRequestError
from nautilus_ctrader.common.rate_limit import RateLimiter
from nautilus_ctrader.constants import BUCKET_DEFAULT, BUCKET_HISTORICAL, SYMBOL_BY_ID_BATCH
from nautilus_ctrader.enums import PERIOD_SECS
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests import account_venue
from tests.execution_replay import make_deal, make_order
from tests.fake_server import FakeCTraderServer
from tests.polling import wait_until

_SCRIPT_PATH = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "verify_live_data.py"
_SPEC = importlib.util.spec_from_file_location("verify_live_data", _SCRIPT_PATH)
verify_live_data = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = verify_live_data
_SPEC.loader.exec_module(verify_live_data)

v = verify_live_data
OK, DIFFERS, UNKNOWN = v.OK, v.DIFFERS, v.UNKNOWN

EURUSD = "EURUSD"
XAUUSD = "XAUUSD"


def settings(**overrides) -> v.Settings:
    values = {
        "trader_login": account_venue.TRADER_LOGIN,
        "symbols": (EURUSD, XAUUSD),
        "minutes": 0.02,
        "grace_secs": 1.0,
        "history_budget": 400,
        "symbol_batch": 2,
    }
    values.update(overrides)
    return v.Settings(**values)


# -- The read-only guarantee ----------------------------------------------------------------


class _StubConnection:
    """Records what reached the socket, and replies from a canned list."""

    def __init__(self, replies: list[object] | None = None) -> None:
        self.sent: list[object] = []
        self.buckets: list[str] = []
        self._replies = list(replies or [])

    def set_event_handler(self, handler) -> None:
        pass

    async def request(self, payload, *, bucket=BUCKET_DEFAULT, timeout_secs=None):
        self.sent.append(payload)
        self.buckets.append(bucket)
        reply = self._replies.pop(0) if self._replies else oa.ProtoOAApplicationAuthRes()
        if isinstance(reply, Exception):
            raise reply
        return reply


def test_the_requester_refuses_an_order_request() -> None:
    connection = _StubConnection()
    requester = v.ReadOnlyRequester(connection, history_budget=10)

    with pytest.raises(v.ReadOnlyViolation):
        asyncio.run(requester.request(oa.ProtoOANewOrderReq(ctidTraderAccountId=1, symbolId=1)))

    assert connection.sent == []
    assert requester.requests == 0


def test_the_read_only_set_holds_nothing_that_changes_account_state() -> None:
    forbidden = ("Order", "Position", "Amend", "Cancel", "Close", "Deal")
    # The account's history lists name orders and deals, but only list them.
    listing = {"ProtoOADealListReq", "ProtoOAOrderListReq"}
    named = sorted(cls.__name__ for cls in v.READ_ONLY_REQUESTS)
    assert named
    assert listing <= set(named)
    assert not [
        name for name in named if name not in listing and any(word in name for word in forbidden)
    ]


def test_the_script_never_names_an_order_request() -> None:
    """The allow-list is only half of it: an order type the module does not even import
    cannot be reached by a later edit that bypasses the requester."""
    source = _SCRIPT_PATH.read_text(encoding="utf-8")
    for name in ("NewOrderReq", "AmendOrder", "AmendPosition", "ClosePositionReq", "CancelOrder"):
        assert name not in source, name


def test_the_historical_budget_bounds_the_whole_run() -> None:
    connection = _StubConnection()
    requester = v.ReadOnlyRequester(connection, history_budget=2)

    async def run() -> None:
        for _ in range(2):
            await requester.request(oa.ProtoOAGetTrendbarsReq(), bucket=BUCKET_HISTORICAL)
        with pytest.raises(v.RequestBudgetExhausted):
            await requester.request(oa.ProtoOAGetTrendbarsReq(), bucket=BUCKET_HISTORICAL)
        # The cap is on historical requests only; the rest of the run carries on.
        await requester.request(oa.ProtoOASymbolsListReq())

    asyncio.run(run())
    assert requester.historical_requests == 2
    assert requester.requests == 3
    assert requester.history_remaining == 0


def test_a_rate_limit_block_is_waited_out_and_counted() -> None:
    blocked = CTraderRequestError("BLOCKED_PAYLOAD_TYPE", retry_after_secs=0)
    connection = _StubConnection([blocked, oa.ProtoOAGetTrendbarsRes()])
    requester = v.ReadOnlyRequester(connection, history_budget=10)

    asyncio.run(requester.request(oa.ProtoOAGetTrendbarsReq(), bucket=BUCKET_HISTORICAL))

    assert len(connection.sent) == 2
    assert requester.rate_limit_blocks == 1


# -- The command line -----------------------------------------------------------------------


def test_parse_symbols_keeps_the_venues_own_spelling() -> None:
    assert v.parse_symbols(" EURUSD , GER40.cash ") == ("EURUSD", "GER40.cash")


def test_parse_symbols_rejects_an_empty_list() -> None:
    with pytest.raises(Exception, match="at least one symbol"):
        v.parse_symbols(" , ")


def test_the_defaults_watch_two_major_symbols_for_five_minutes() -> None:
    args = v.build_arg_parser().parse_args(["--trader-login", "42"])
    parsed = v.settings_from_args(args)
    assert parsed.trader_login == 42
    assert parsed.minutes == 5.0
    assert parsed.symbols == ("EURUSD", "XAUUSD")
    # The batch the adapter itself uses, so a default run verifies the number it depends on.
    assert parsed.symbol_batch == SYMBOL_BY_ID_BATCH
    assert parsed.history_budget > 0


def test_the_trader_login_is_required() -> None:
    with pytest.raises(SystemExit):
        v.build_arg_parser().parse_args([])


def test_every_setting_can_be_overridden() -> None:
    args = v.build_arg_parser().parse_args(
        [
            "--trader-login",
            "42",
            "--minutes",
            "1.5",
            "--symbols",
            "GBPUSD,USDJPY",
            "--bar-close-grace-secs",
            "2",
            "--history-budget",
            "50",
            "--symbol-batch",
            "100",
        ],
    )
    parsed = v.settings_from_args(args)
    assert (parsed.minutes, parsed.grace_secs) == (1.5, 2.0)
    assert parsed.symbols == ("GBPUSD", "USDJPY")
    assert (parsed.history_budget, parsed.symbol_batch) == (50, 100)


# -- Decisions ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("error_code", "expected"),
    [("NOT_SUBSCRIBED_TO_SPOTS", OK), ("NO_QUOTES", DIFFERS), (None, DIFFERS)],
)
def test_item_1_wants_exactly_the_not_subscribed_rejection(error_code, expected) -> None:
    status, detail = v.decide_trendbar_without_spots(error_code)
    assert status == expected
    assert detail


def test_item_2_reads_a_millisecond_timestamp_as_ok() -> None:
    local = 1_700_000_000.0
    status, detail = v.decide_spot_timestamp(int(local * 1000) + 250, local)
    assert status == OK
    assert "milliseconds" in detail[0]


def test_item_2_names_the_unit_that_actually_fits() -> None:
    local = 1_700_000_000.0
    status, detail = v.decide_spot_timestamp(int(local), local)
    assert status == DIFFERS
    assert "seconds" in detail[0]


def test_item_2_calls_a_plausibly_stale_stream_unknown_rather_than_a_wrong_unit() -> None:
    """A market closed on Friday keeps serving that last tick; the unit was never wrong."""
    local = 1_700_000_000.0
    status, detail = v.decide_spot_timestamp(int((local - 150_800) * 1000), local)
    assert status == UNKNOWN
    assert "stale" in detail[0]
    assert "41.9 h" in detail[1]


@pytest.mark.parametrize(
    "timestamp",
    [
        1,  # 1970, as far out as a unit can be
        1_700_000_000_000_000_000,  # tens of thousands of years into the future
    ],
)
def test_item_2_reports_a_timestamp_no_unit_explains(timestamp) -> None:
    status, _ = v.decide_spot_timestamp(timestamp, 1_700_000_000.0)
    assert status == DIFFERS


def test_item_2_without_a_timestamp_is_unknown() -> None:
    assert v.decide_spot_timestamp(None, 1_700_000_000.0)[0] == UNKNOWN


def _aligned(symbol: str, period_name: str, period_secs: int, *, offset: int = 0):
    base = 1_700_000_000 // period_secs * period_secs
    return v.AlignmentObservation(
        symbol,
        period_name,
        period_secs,
        open_secs=tuple(base - i * period_secs + offset for i in range(3)),
    )


def test_an_account_id_mistaken_for_a_login_is_named_as_such() -> None:
    """The two identifiers look alike, so the bare refusal reads as a token problem."""
    granted = [oa_model.ProtoOACtidTraderAccount(ctidTraderAccountId=111, traderLogin=222)]
    message = v._no_such_account_message(granted, 111)
    assert "traderLogin" in message and "ctidTraderAccountId" in message
    assert "111" not in message and "222" not in message


def test_a_login_the_token_does_not_grant_is_reported_without_it() -> None:
    granted = [oa_model.ProtoOACtidTraderAccount(ctidTraderAccountId=111, traderLogin=222)]
    message = v._no_such_account_message(granted, 999)
    assert "999" not in message and "111" not in message and "222" not in message


def test_item_3_accepts_aligned_intraday_periods() -> None:
    status, detail = v.decide_alignment(
        [_aligned(EURUSD, "M1", 60), _aligned(EURUSD, "H1", 3600)],
    )
    assert status == OK
    assert len(detail) == 2


def test_item_3_flags_an_open_time_off_the_period() -> None:
    """A 7 s phase is consistent, but no trading day can impose it, so it stays a surprise."""
    status, detail = v.decide_alignment([_aligned(EURUSD, "M15", 900, offset=7)])
    assert status == DIFFERS
    assert "not a whole hour" in detail[0]


def test_item_3_accepts_a_whole_hour_phase_from_the_trading_day() -> None:
    """H4 carries the trading day's phase: 21:00 is not a multiple of four hours."""
    status, detail = v.decide_alignment([_aligned(EURUSD, "H4", 14_400, offset=3600)])
    assert status == OK
    assert "whole-hour phase" in detail[0]


def test_item_3_flags_bars_that_keep_more_than_one_phase() -> None:
    mixed = v.AlignmentObservation(
        EURUSD,
        "H4",
        14_400,
        open_secs=(1_700_006_400, 1_700_020_800 + 1800),
    )
    assert v.decide_alignment([mixed])[0] == DIFFERS


def test_item_3_reports_the_d1_boundary_time_instead_of_midnight_alignment() -> None:
    """A trading day need not start at 00:00 UTC, so D1 is judged on being consistent."""
    status, detail = v.decide_alignment([_aligned(EURUSD, "D1", 86_400, offset=21 * 3600)])
    assert status == OK
    assert "21:00 UTC" in detail[0]


def test_item_3_flags_d1_bars_that_open_at_different_times() -> None:
    observation = v.AlignmentObservation(EURUSD, "D1", 86_400, open_secs=(86_400, 172_800 + 3600))
    assert v.decide_alignment([observation])[0] == DIFFERS


def test_item_3_without_any_bars_is_unknown() -> None:
    observation = v.AlignmentObservation(EURUSD, "M1", 60, error_code="NO_QUOTES")
    status, detail = v.decide_alignment([observation])
    assert status == UNKNOWN
    assert "NO_QUOTES" in detail[0]


def test_item_4_is_ok_when_the_stream_closed_every_bar_and_history_had_it() -> None:
    closes = [v.BarClose(EURUSD, 1_700_000_040, "stream", 0.5, 2)]
    status, detail = v.decide_bar_closes(closes, 1_700_000_105.0)
    assert status == OK
    assert "closed by stream" in detail[0]


def test_item_4_flags_a_timer_close_and_a_bar_history_never_served() -> None:
    closes = [
        v.BarClose(EURUSD, 1_700_000_040, "stream", 0.5, 2),
        v.BarClose(EURUSD, 1_700_000_100, "timer", None, 40),
    ]
    status, detail = v.decide_bar_closes(closes, 1_700_000_165.0)
    assert status == DIFFERS
    assert "1 of 2 needed the timer" in detail[-1]


def test_item_4_calls_bars_far_older_than_the_run_unknown() -> None:
    """With the market shut the run follows the last bar before it; that proves nothing."""
    closes = [v.BarClose(EURUSD, 1_700_000_040, "timer", 150_800.0, 40)]
    status, detail = v.decide_bar_closes(closes, 1_700_000_040 + 150_800.0)
    assert status == UNKNOWN
    assert "41.9 h older than the run" in detail[-1]


def test_item_4_without_a_closed_bar_is_unknown() -> None:
    assert v.decide_bar_closes([], 1_700_000_105.0)[0] == UNKNOWN


def test_item_5a_takes_the_largest_count_served_in_full() -> None:
    probes = [
        v.CountProbe(500, 499, has_more=True),
        v.CountProbe(1000, 1000),
        v.CountProbe(5000, 1000),
    ]
    status, detail = v.decide_count_cap(probes)
    assert status == OK
    assert detail[-1].endswith("1000")


def test_item_5a_differs_when_the_page_size_the_adapter_uses_is_not_honoured() -> None:
    probes = [v.CountProbe(500, 100), v.CountProbe(1000, 100), v.CountProbe(5000, 100)]
    assert v.decide_count_cap(probes)[0] == DIFFERS


def test_item_5a_is_unknown_when_no_count_was_served() -> None:
    probes = [v.CountProbe(c, None, error_code="INCORRECT_BOUNDARIES") for c in (500, 1000)]
    status, detail = v.decide_count_cap(probes)
    assert status == UNKNOWN
    assert "INCORRECT_BOUNDARIES" in detail[0]


def test_item_5b_wants_the_wide_window_rejected() -> None:
    assert v.decide_wide_boundaries("INCORRECT_BOUNDARIES", None)[0] == OK
    assert v.decide_wide_boundaries("NO_QUOTES", None)[0] == DIFFERS
    # Accepting a wide window and truncating it is this venue's confirmed behaviour.
    status, lines = v.decide_wide_boundaries(None, 500)
    assert status == OK
    assert "500 bars served" in lines[0]


def test_item_6_wants_every_requested_id_answered() -> None:
    assert v.decide_symbol_batch(100, 100, 100, None)[0] == OK
    assert v.decide_symbol_batch(100, 100, 80, None)[0] == DIFFERS
    assert v.decide_symbol_batch(100, 100, None, "INVALID_REQUEST")[0] == DIFFERS
    assert v.decide_symbol_batch(100, 0, None, None)[0] == UNKNOWN


def test_item_6_reports_the_batch_an_account_with_fewer_symbols_did_reach() -> None:
    status, detail = v.decide_symbol_batch(200, 166, 166, None)
    assert status == OK
    assert "166 ids in one request accepted" in detail[0]
    assert "only 166 of the 200" in detail[1]


def test_item_7_reports_the_prices_that_do_not_fit_the_symbols_digits() -> None:
    assert v.decide_price_digits(100, [])[0] == OK
    assert v.decide_price_digits(0, [])[0] == UNKNOWN
    status, detail = v.decide_price_digits(100, ["EURUSD bid 1 at 2 digits"])
    assert status == DIFFERS
    assert "1 of 100" in detail[0]


# The live answers, for an exact window from 11:33 to 11:34 with count=10: ten bars back from
# each window's toTimestamp, reaching past its fromTimestamp.
_LIVE_FIRST = 1_791_372_780  # 2026-10-07 11:33Z
_LIVE_SECOND = _LIVE_FIRST + 60


def _counted_back(newest: int, count: int = 10) -> tuple[int, ...]:
    """`count` consecutive M1 open times ending at `newest`."""
    return tuple(range(newest - (count - 1) * 60, newest + 1, 60))


def _edge_observation(**overrides) -> v.WindowEdgeObservation:
    values = {
        "symbol": EURUSD,
        "searched": "the last 30 minutes",
        "bar_secs": _LIVE_FIRST,
        "edge_open_secs": _counted_back(_LIVE_SECOND),
        "control_open_secs": _counted_back(_LIVE_FIRST),
        "positive_open_secs": _counted_back(_LIVE_SECOND),
    }
    values.update(overrides)
    return v.WindowEdgeObservation(**values)


def test_item_8_confirms_the_answers_the_live_venue_gave() -> None:
    status, detail = v.decide_window_edges(_edge_observation())
    assert status == OK
    assert detail[-1] == (
        "answers count bars back from toTimestamp by open time, inclusive; fromTimestamp does "
        "not bound the answer"
    )
    text = "\n".join(detail)
    assert "count=10" in text
    # The boundaries come out as UTC times, never as an account detail.
    assert "2026-10-07 11:33Z to 2026-10-07 11:34Z" in text
    assert "exact window: bars opening at 2026-10-07 11:25Z" in text
    assert "control, 1 ms inside both edges: bars opening at 2026-10-07 11:24Z" in text


@pytest.mark.parametrize(
    ("overrides", "named"),
    [
        # A bar opening after toTimestamp.
        ({"edge_open_secs": _counted_back(_LIVE_SECOND + 60)}, "exact window"),
        # toTimestamp exclusive: the bar opening on it is missing.
        ({"edge_open_secs": _counted_back(_LIVE_FIRST)}, "exact window"),
        # The control's toTimestamp is 1 ms before the second bar opens.
        ({"control_open_secs": _counted_back(_LIVE_SECOND)}, "control"),
        # Fewer than count while the control shows older bars exist.
        ({"edge_open_secs": _counted_back(_LIVE_SECOND, 5)}, "exact window"),
        # A bar skipped inside the answer.
        ({"edge_open_secs": (*_counted_back(_LIVE_SECOND)[1:], _LIVE_FIRST - 600)}, "exact"),
        ({"positive_open_secs": ()}, "positive control"),
    ],
)
def test_item_8_differs_when_an_answer_is_not_count_bars_back_from_its_end(
    overrides,
    named,
) -> None:
    status, detail = v.decide_window_edges(_edge_observation(**overrides))
    assert status == DIFFERS
    assert detail[-1].startswith(f"the {named}")
    assert "counted back from its toTimestamp by open time" in detail[-1]


def test_item_8_accepts_an_answer_one_oldest_bar_short() -> None:
    """`count = N` answered with `N - 1` bars is a recorded venue habit, not a contradiction."""
    status, detail = v.decide_window_edges(
        _edge_observation(edge_open_secs=_counted_back(_LIVE_SECOND, 9)),
    )
    assert status == OK
    assert any(
        line.startswith("the exact window returned one bar fewer than count") for line in detail
    )


def test_item_8_differs_when_from_timestamp_bounds_the_answer_while_older_bars_exist() -> None:
    """The answers the adapter used to assume, with the search showing history goes further."""
    status, detail = v.decide_window_edges(
        _edge_observation(
            searched_open_secs=_counted_back(_LIVE_SECOND + 120, 30),
            edge_open_secs=(_LIVE_FIRST, _LIVE_SECOND),
            control_open_secs=(),
            positive_open_secs=(_LIVE_FIRST, _LIVE_SECOND),
        ),
    )
    assert status == DIFFERS
    assert any(line.startswith("the exact window returned") for line in detail)


def test_item_8_is_unknown_when_nothing_shows_from_timestamp_is_ignored() -> None:
    # The control would show it, by serving nothing although the first bar opens before its end.
    status, detail = v.decide_window_edges(
        _edge_observation(
            edge_open_secs=(_LIVE_FIRST, _LIVE_SECOND),
            control_open_secs=(),
            control_error="INVALID_REQUEST",
            positive_open_secs=(_LIVE_FIRST, _LIVE_SECOND),
        ),
    )
    assert status == UNKNOWN
    assert "cannot be told" in detail[-1]


@pytest.mark.parametrize(
    ("overrides", "named"),
    [
        ({"control_open_secs": (), "control_error": "INVALID_REQUEST"}, "control"),
        ({"positive_open_secs": (), "positive_error": "INVALID_REQUEST"}, "positive control"),
    ],
)
def test_item_8_keeps_its_verdict_when_a_control_is_refused(overrides, named) -> None:
    status, detail = v.decide_window_edges(_edge_observation(**overrides))
    assert status == OK
    assert f"{named} refused: INVALID_REQUEST" in detail


def test_item_8_without_a_pair_of_bars_is_unknown_with_the_reason() -> None:
    status, detail = v.decide_window_edges(
        _edge_observation(bar_secs=None, edge_open_secs=(), searched="the last 3 days"),
    )
    assert status == UNKNOWN
    assert "the last 3 days" in detail[0]
    assert "consecutive" in detail[0]


def test_item_8_carries_the_error_code_of_a_refused_search() -> None:
    status, detail = v.decide_window_edges(
        _edge_observation(bar_secs=None, searched="the last 3 days", error_code="NO_QUOTES"),
    )
    assert status == UNKNOWN
    assert "NO_QUOTES" in detail[0]


@pytest.mark.parametrize(
    ("opens", "expected"),
    [
        # The newest bar may still be forming, so the pair is the two before it.
        ([0, 60, 120, 180], 60),
        # Order and duplicates in the response do not matter.
        ([180, 60, 0, 120, 120], 60),
        # A gap before the newest bar: the newest consecutive pair is further back.
        ([0, 60, 240, 300], 0),
        ([0, 60, 120, 600], 60),
        # Nothing consecutive, too few bars, or only the forming bar.
        ([0, 120, 240], None),
        ([0, 60], None),
        ([60], None),
        ([], None),
    ],
)
def test_the_pair_is_the_newest_consecutive_closed_bars(opens, expected) -> None:
    assert v.find_consecutive_closed_pair(opens) == expected


_NOW = 1_700_000_030
_END = _NOW // 60 * 60
_SYMBOL_ID = 7
_DAY = 86_400


def _bars(*open_secs: int) -> oa.ProtoOAGetTrendbarsRes:
    return oa.ProtoOAGetTrendbarsRes(
        trendbar=[
            om.ProtoOATrendbar(utcTimestampInMinutes=s // 60, low=110_000, volume=10)
            for s in open_secs
        ],
    )


def _edge_verifier(replies: list[object], monkeypatch) -> tuple[v.Verifier, _StubConnection]:
    monkeypatch.setattr(time, "time", lambda: _NOW)
    connection = _StubConnection(replies)
    requester = v.ReadOnlyRequester(connection, history_budget=10)
    verifier = v.Verifier(requester, settings(), account_venue.ACCOUNT_ID)
    verifier._symbol_ids = {EURUSD: _SYMBOL_ID}
    return verifier, connection


def _windows(connection: _StubConnection) -> list[tuple[int, int]]:
    return [(p.fromTimestamp, p.toTimestamp) for p in connection.sent]


_PAIR_SEARCH = _bars(*range(_END - 1800, _END, 60))
_PAIR = _END - 180


def _back_from(newest: int) -> oa.ProtoOAGetTrendbarsRes:
    """Ten bars counted back from `newest`, as the live venue answers a window ending there."""
    return _bars(*_counted_back(newest))


async def test_item_8_asks_the_exact_millisecond_windows_and_nothing_but_history(
    monkeypatch,
) -> None:
    verifier, connection = _edge_verifier(
        [_PAIR_SEARCH, _back_from(_PAIR + 60), _back_from(_PAIR), _back_from(_PAIR + 60)],
        monkeypatch,
    )

    status, detail = await verifier._probe_window_edges()

    assert status == OK
    assert {type(p) for p in connection.sent} == {oa.ProtoOAGetTrendbarsReq}
    assert oa.ProtoOAGetTrendbarsReq in v.READ_ONLY_REQUESTS
    assert connection.buckets == [BUCKET_HISTORICAL] * 4
    assert all(p.symbolId == _SYMBOL_ID and p.period == om.M1 for p in connection.sent)
    assert all(p.ctidTraderAccountId == account_venue.ACCOUNT_ID for p in connection.sent)
    assert _windows(connection) == [
        ((_END - 1800) * 1000, _END * 1000),
        (_PAIR * 1000, (_PAIR + 60) * 1000),
        (_PAIR * 1000 + 1, (_PAIR + 60) * 1000 - 1),
        (_PAIR * 1000 - 1, (_PAIR + 60) * 1000 + 1),
    ]
    assert [p.count for p in connection.sent[1:]] == [10, 10, 10]
    assert "fromTimestamp does not bound the answer" in detail[-1]


async def test_item_8_never_uses_the_newest_bar_as_an_edge_bar(monkeypatch) -> None:
    """The newest bar returned may be forming, so a pair ending on it is not used."""
    verifier, connection = _edge_verifier([_bars(_END - 120, _END - 60), _bars()], monkeypatch)

    status, _ = await verifier._probe_window_edges()

    # Both windows held at most one closed bar; the search widened once, then gave up.
    assert status == UNKNOWN
    assert _windows(connection) == [
        ((_END - 1800) * 1000, _END * 1000),
        ((_END - 3 * _DAY) * 1000, _END * 1000),
    ]


async def test_item_8_widens_once_when_the_market_is_closed(monkeypatch) -> None:
    shut = _END - 2 * _DAY
    bar = shut - 120
    verifier, connection = _edge_verifier(
        [
            _bars(),
            _bars(*range(bar - 1200, shut + 1, 60)),
            _back_from(bar + 60),
            _back_from(bar),
            _back_from(bar + 60),
        ],
        monkeypatch,
    )

    status, _ = await verifier._probe_window_edges()

    assert status == OK
    assert _windows(connection) == [
        ((_END - 1800) * 1000, _END * 1000),
        ((_END - 3 * _DAY) * 1000, _END * 1000),
        (bar * 1000, (bar + 60) * 1000),
        (bar * 1000 + 1, (bar + 60) * 1000 - 1),
        (bar * 1000 - 1, (bar + 60) * 1000 + 1),
    ]


async def test_item_8_gives_up_after_one_widening(monkeypatch) -> None:
    verifier, connection = _edge_verifier([_bars(), _bars()], monkeypatch)

    status, detail = await verifier._probe_window_edges()

    assert status == UNKNOWN
    assert len(connection.sent) == 2
    assert "the last 3 days" in detail[0]


async def test_item_8_widens_when_the_narrow_window_is_refused(monkeypatch) -> None:
    shut = _END - _DAY
    bar = shut - 120
    verifier, connection = _edge_verifier(
        [
            CTraderRequestError("NO_QUOTES"),
            _bars(*range(bar - 1200, shut + 1, 60)),
            _back_from(bar + 60),
            _back_from(bar),
            _back_from(bar + 60),
        ],
        monkeypatch,
    )

    status, _ = await verifier._probe_window_edges()

    assert status == OK
    assert len(connection.sent) == 5


async def test_item_8_reports_the_code_when_every_search_is_refused(monkeypatch) -> None:
    verifier, _ = _edge_verifier(
        [CTraderRequestError("NO_QUOTES"), CTraderRequestError("NO_QUOTES")],
        monkeypatch,
    )

    status, detail = await verifier._probe_window_edges()

    assert status == UNKNOWN
    assert "NO_QUOTES" in detail[0]


async def test_item_8_keeps_its_verdict_when_the_control_is_refused(monkeypatch) -> None:
    verifier, connection = _edge_verifier(
        [
            _PAIR_SEARCH,
            _back_from(_PAIR + 60),
            CTraderRequestError("INVALID_REQUEST"),
            _back_from(_PAIR + 60),
        ],
        monkeypatch,
    )

    status, detail = await verifier._probe_window_edges()

    assert status == OK
    assert len(connection.sent) == 4
    assert "control refused: INVALID_REQUEST" in detail


async def test_item_8_is_unknown_with_the_code_when_the_edge_window_is_refused(
    monkeypatch,
) -> None:
    verifier, connection = _edge_verifier(
        [_PAIR_SEARCH, CTraderRequestError("INVALID_REQUEST")],
        monkeypatch,
    )

    status, detail = await verifier._safely(verifier._probe_window_edges)

    assert status == UNKNOWN
    assert "INVALID_REQUEST" in detail[0]
    # Nothing is left to decide, so the controls are not asked.
    assert len(connection.sent) == 2


async def test_item_8_differs_when_the_positive_control_misses_the_bar_at_its_end(
    monkeypatch,
) -> None:
    verifier, _ = _edge_verifier(
        [_PAIR_SEARCH, _back_from(_PAIR + 60), _back_from(_PAIR), _back_from(_PAIR)],
        monkeypatch,
    )

    status, detail = await verifier._probe_window_edges()

    assert status == DIFFERS
    assert detail[-1].startswith("the positive control returned")


async def test_item_8_differs_when_the_control_serves_a_bar_past_its_end(monkeypatch) -> None:
    verifier, _ = _edge_verifier(
        [_PAIR_SEARCH, _back_from(_PAIR + 60), _back_from(_PAIR + 60), _back_from(_PAIR + 60)],
        monkeypatch,
    )

    status, detail = await verifier._probe_window_edges()

    assert status == DIFFERS
    assert detail[-1].startswith("the control returned")


async def test_item_8_differs_when_the_venue_stops_at_from_timestamp(monkeypatch) -> None:
    """The search showed older history, so a window holding only its own bars is cut short."""
    verifier, _ = _edge_verifier(
        [_PAIR_SEARCH, _bars(_PAIR, _PAIR + 60), _bars(_PAIR), _bars(_PAIR, _PAIR + 60)],
        monkeypatch,
    )

    status, _ = await verifier._probe_window_edges()

    assert status == DIFFERS


async def test_item_8_without_a_symbol_is_unknown(monkeypatch) -> None:
    verifier, connection = _edge_verifier([], monkeypatch)
    verifier._symbol_ids = {}

    status, _ = await verifier._probe_window_edges()

    assert status == UNKNOWN
    assert connection.sent == []


# -- Item 9: history windows that end in the future -----------------------------------------

DEAL_LIST, ORDER_LIST = "deal list", "order list"
_ANSWERS = {DEAL_LIST: "ProtoOADealListRes", ORDER_LIST: "ProtoOAOrderListRes"}


def _served(kind: str, window: str, *ids: int, has_more: bool = False) -> v.ListWindowProbe:
    return v.ListWindowProbe(kind, window, answer=_ANSWERS[kind], ids=ids, has_more=has_more)


def _all_served(**overrides) -> list[v.ListWindowProbe]:
    """Every window answered; `overrides` replaces a probe, keyed `<deal|order>_<to_now|...>`."""
    probes = []
    for kind in (DEAL_LIST, ORDER_LIST):
        for key, window, ids in (
            ("to_now", v._WINDOW_TO_NOW, (1, 2)),
            ("past_now", v._WINDOW_PAST_NOW, (1, 2, 3)),
            ("future", v._WINDOW_FUTURE, ()),
        ):
            name = f"{kind.split()[0]}_{key}"
            probes.append(overrides.get(name, _served(kind, window, *ids)))
    return probes


def test_item_9_is_ok_when_every_window_is_answered_and_covers_the_control() -> None:
    status, detail = v.decide_future_windows(_all_served())

    assert status == OK
    assert "deal list, last day to a day past now: answered, 3 served" in detail
    assert "order list: the window past now served everything served up to now" in detail


@pytest.mark.parametrize("window", [v._WINDOW_PAST_NOW, v._WINDOW_FUTURE])
def test_item_9_differs_when_a_window_ending_in_the_future_is_refused(window) -> None:
    key = "past_now" if window == v._WINDOW_PAST_NOW else "future"
    refused = v.ListWindowProbe(DEAL_LIST, window, error_code="INVALID_REQUEST")

    status, detail = v.decide_future_windows(_all_served(**{f"deal_{key}": refused}))

    assert status == DIFFERS
    assert f"deal list, {window}: refused with INVALID_REQUEST" in detail


def test_item_9_differs_when_answered_with_another_type() -> None:
    other = v.ListWindowProbe(ORDER_LIST, v._WINDOW_FUTURE, answer="ProtoOAErrorRes")

    status, detail = v.decide_future_windows(_all_served(order_future=other))

    assert status == DIFFERS
    assert "order list, an hour past now to a day past now: answered with ProtoOAErrorRes" in detail


def test_item_9_differs_when_the_window_past_now_misses_what_the_control_served() -> None:
    short = _served(DEAL_LIST, v._WINDOW_PAST_NOW, 1)

    status, detail = v.decide_future_windows(_all_served(deal_past_now=short))

    assert status == DIFFERS
    assert "deal list: 1 served up to now are missing from the window past now" in detail


def test_item_9_compares_nothing_across_a_page_cut_short() -> None:
    cut = _served(DEAL_LIST, v._WINDOW_PAST_NOW, 1, has_more=True)

    status, detail = v.decide_future_windows(_all_served(deal_past_now=cut))

    assert status == OK
    assert not any(line.startswith("deal list: ") for line in detail)


def test_item_9_keeps_its_verdict_when_the_control_is_refused() -> None:
    refused = v.ListWindowProbe(ORDER_LIST, v._WINDOW_TO_NOW, error_code="INVALID_REQUEST")

    status, detail = v.decide_future_windows(_all_served(order_to_now=refused))

    assert status == OK
    assert "order list, last day to now: refused with INVALID_REQUEST" in detail
    assert not any(line.startswith("order list: ") for line in detail)


def test_item_9_is_unknown_when_a_future_window_was_not_asked() -> None:
    probes = [p for p in _all_served() if p.window != v._WINDOW_FUTURE]

    status, detail = v.decide_future_windows(probes)

    assert status == UNKNOWN
    assert detail[-1] == "not every window ending in the future was asked"


async def test_item_9_asks_both_lists_over_the_three_windows_read_only(monkeypatch) -> None:
    now_ms = _NOW * 1000
    day_ms = _DAY * 1000
    deals = oa.ProtoOADealListRes(deal=[om.ProtoOADeal(dealId=5)], hasMore=False)
    orders = oa.ProtoOAOrderListRes(order=[om.ProtoOAOrder(orderId=6)], hasMore=False)
    refused = CTraderRequestError("INVALID_REQUEST", description="secret description")
    verifier, connection = _edge_verifier(
        [deals, deals, oa.ProtoOADealListRes(), orders, orders, refused], monkeypatch
    )

    status, detail = await verifier._probe_future_windows()

    assert [type(p) for p in connection.sent] == [oa.ProtoOADealListReq] * 3 + [
        oa.ProtoOAOrderListReq
    ] * 3
    assert all(type(p) in v.READ_ONLY_REQUESTS for p in connection.sent)
    assert connection.buckets == [BUCKET_HISTORICAL] * 6
    assert all(p.ctidTraderAccountId == account_venue.ACCOUNT_ID for p in connection.sent)
    windows = [
        (now_ms - day_ms, now_ms),
        (now_ms - day_ms, now_ms + day_ms),
        (now_ms + 3_600_000, now_ms + day_ms),
    ]
    assert _windows(connection) == windows * 2
    assert status == DIFFERS
    assert "order list, an hour past now to a day past now: refused with INVALID_REQUEST" in detail
    assert not any("secret description" in line for line in detail)


# -- The report -----------------------------------------------------------------------------


def test_the_report_numbers_items_in_order_and_counts_the_statuses() -> None:
    findings = [
        v.Finding("5a", "counts", DIFFERS, ("count=500: 100 bars",)),
        v.Finding("1", "no spots", OK),
        v.Finding("2", "timestamp", UNKNOWN),
    ]
    report = v.format_report(
        findings,
        settings=settings(),
        requests=12,
        historical_requests=4,
        rate_limit_blocks=1,
    )
    item_lines = [
        line
        for line in report.splitlines()
        if line.startswith((OK, DIFFERS, UNKNOWN)) and "." in line
    ]
    assert [line.split(".")[0].split()[-1] for line in item_lines] == ["1", "2", "5a"]
    assert "count=500: 100 bars" in report
    assert report.endswith(f"{OK} 1, {DIFFERS} 1, {UNKNOWN} 1")


def test_the_report_names_symbols_the_account_does_not_offer() -> None:
    report = v.format_report(
        [],
        settings=settings(),
        unresolved_symbols=("NOPE",),
        requests=1,
        historical_requests=0,
        rate_limit_blocks=0,
    )
    assert "not offered by this account: NOPE" in report


# -- A whole run against the fake venue -----------------------------------------------------


def _trendbars(
    request: oa.ProtoOAGetTrendbarsReq,
    *,
    count_cap: int,
    max_window_days: int,
) -> oa.ProtoOAGetTrendbarsRes:
    """Period-aligned bars counted back from `toTimestamp`, capped like a real venue."""
    span_secs = (request.toTimestamp - request.fromTimestamp) // 1000
    if span_secs > max_window_days * 86_400:
        return oa.ProtoOAErrorRes(
            ctidTraderAccountId=request.ctidTraderAccountId,
            errorCode="INCORRECT_BOUNDARIES",
        )
    period_secs = PERIOD_SECS[request.period]
    served = min(request.count, count_cap)
    last = request.toTimestamp // 1000 // period_secs * period_secs - period_secs
    return oa.ProtoOAGetTrendbarsRes(
        ctidTraderAccountId=request.ctidTraderAccountId,
        period=request.period,
        hasMore=request.count > served,
        trendbar=[
            om.ProtoOATrendbar(
                utcTimestampInMinutes=(last - i * period_secs) // 60,
                low=110_000,
                deltaOpen=1,
                deltaHigh=2,
                deltaClose=1,
                volume=10,
            )
            for i in range(served)
        ],
    )


def _edge_window_bars(
    request: oa.ProtoOAGetTrendbarsReq,
    *,
    bounded: bool,
) -> oa.ProtoOAGetTrendbarsRes:
    """M1 bars counted back from `toTimestamp` by open time, inclusive, as the live venue does.

    With `bounded`, the answer also stops at `fromTimestamp`, which the live venue does not do.
    """
    last_minute = request.toTimestamp // 60_000
    first_minute = last_minute - request.count + 1
    if bounded:
        first_minute = max(first_minute, -(-request.fromTimestamp // 60_000))
    return oa.ProtoOAGetTrendbarsRes(
        ctidTraderAccountId=request.ctidTraderAccountId,
        period=request.period,
        trendbar=[
            om.ProtoOATrendbar(
                utcTimestampInMinutes=minute,
                low=110_000,
                deltaOpen=1,
                deltaHigh=2,
                deltaClose=1,
                volume=10,
            )
            for minute in reversed(range(first_minute, last_minute + 1))
        ],
    )


def verify_venue(
    *,
    count_cap: int = 1000,
    max_window_days: int = 30,
    bounded_edges: bool = False,
) -> FakeCTraderServer:
    """The reference venue, plus history and subscriptions shaped like the live one.

    A window of about a minute is served as `_edge_window_bars` describes; wider windows are
    served as in `_trendbars`.

    A live trendbar subscription is refused until spots are subscribed, which is the very
    thing item 1 exists to confirm.
    """
    server = account_venue.venue(is_live=True)
    spots_held = False

    def on_subscribe_spots(request):
        nonlocal spots_held
        spots_held = True
        return oa.ProtoOASubscribeSpotsRes(ctidTraderAccountId=request.ctidTraderAccountId)

    def on_subscribe_trendbar(request):
        if not spots_held:
            return oa.ProtoOAErrorRes(
                ctidTraderAccountId=request.ctidTraderAccountId,
                errorCode="NOT_SUBSCRIBED_TO_SPOTS",
            )
        return oa.ProtoOASubscribeLiveTrendbarRes(ctidTraderAccountId=request.ctidTraderAccountId)

    server.on(om.PROTO_OA_SUBSCRIBE_SPOTS_REQ, on_subscribe_spots)
    server.on(om.PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_REQ, on_subscribe_trendbar)
    for payload_type, response_class in (
        (om.PROTO_OA_UNSUBSCRIBE_SPOTS_REQ, oa.ProtoOAUnsubscribeSpotsRes),
        (om.PROTO_OA_UNSUBSCRIBE_LIVE_TRENDBAR_REQ, oa.ProtoOAUnsubscribeLiveTrendbarRes),
    ):
        server.on(
            payload_type,
            lambda r, cls=response_class: cls(ctidTraderAccountId=r.ctidTraderAccountId),
        )

    def on_trendbars(request):
        if request.toTimestamp - request.fromTimestamp <= 60_002:
            return _edge_window_bars(request, bounded=bounded_edges)
        return _trendbars(request, count_cap=count_cap, max_window_days=max_window_days)

    server.on(om.PROTO_OA_GET_TRENDBARS_REQ, on_trendbars)

    # One deal and one order an hour old, listed by any window that holds them.
    an_hour_ago_ms = int(time.time() * 1000) - 3_600_000

    def within(request) -> bool:
        return request.fromTimestamp <= an_hour_ago_ms <= request.toTimestamp

    server.on(
        om.PROTO_OA_DEAL_LIST_REQ,
        lambda r: oa.ProtoOADealListRes(
            ctidTraderAccountId=r.ctidTraderAccountId,
            deal=[
                make_deal(5, 6, 7, side=om.BUY, volume=100, price=1.1, ts=an_hour_ago_ms),
            ]
            if within(r)
            else [],
            hasMore=False,
        ),
    )
    server.on(
        om.PROTO_OA_ORDER_LIST_REQ,
        lambda r: oa.ProtoOAOrderListRes(
            ctidTraderAccountId=r.ctidTraderAccountId,
            order=[make_order(6, 7, utc=an_hour_ago_ms)] if within(r) else [],
            hasMore=False,
        ),
    )
    return server


async def _push_spots_once_subscribed(server: FakeCTraderServer, symbol_id: int) -> None:
    await wait_until(
        lambda: any(isinstance(m, oa.ProtoOASubscribeSpotsReq) for m in server.received),
        timeout_secs=10.0,
        description="a spot subscription",
    )
    await server.push(
        oa.ProtoOASpotEvent(
            ctidTraderAccountId=account_venue.ACCOUNT_ID,
            symbolId=symbol_id,
            bid=110_123,
            ask=110_125,
            timestamp=int(time.time() * 1000),
        ),
    )


async def test_a_whole_run_reports_every_item_and_never_prints_an_identifier() -> None:
    server = verify_venue()
    await server.start()
    eurusd_id = next(
        s.symbolId for s in account_venue.RECORDED["symbols"][0].symbol if s.symbolName == EURUSD
    )
    pusher = asyncio.create_task(_push_spots_once_subscribed(server, eurusd_id))
    try:
        findings, report = await v.verify(
            server.host,
            server.port,
            tls=False,
            credentials=v.Credentials("client-id", "client-secret", "access-token"),
            settings=settings(),
            account_id=account_venue.ACCOUNT_ID,
            # The fake venue is local; the live budget would only slow the test down.
            rate_limiter=RateLimiter({BUCKET_DEFAULT: 1000.0, BUCKET_HISTORICAL: 1000.0}),
        )
        await pusher
    finally:
        pusher.cancel()
        await server.stop()

    by_item = {f.item: f for f in findings}
    assert sorted(by_item) == ["0", "1", "2", "3", "4", "5a", "5b", "6", "7", "8", "9"]
    assert by_item["0"].status == OK
    assert by_item["1"].status == OK
    assert by_item["2"].status == OK
    assert by_item["3"].status == OK
    assert by_item["5a"].status == OK
    assert by_item["5b"].status == OK
    assert by_item["6"].status == OK
    assert by_item["7"].status == OK
    assert by_item["8"].status == OK
    assert by_item["9"].status == OK
    # No M1 bar can close inside a window this short, and that is an UNKNOWN, not a failure.
    assert by_item["4"].status == UNKNOWN

    assert str(account_venue.ACCOUNT_ID) not in report
    assert str(account_venue.TRADER_LOGIN) not in report
    assert "access-token" not in report
    assert "client-id" not in report


@pytest.mark.parametrize(
    ("bounded_edges", "status", "last_line"),
    [
        (False, OK, "fromTimestamp does not bound the answer"),
        (True, DIFFERS, "counted back from its toTimestamp by open time"),
    ],
    ids=["as live", "bounded by fromTimestamp"],
)
async def test_a_whole_run_reports_how_the_venue_answered_a_window(
    bounded_edges,
    status,
    last_line,
) -> None:
    server = verify_venue(bounded_edges=bounded_edges)
    await server.start()
    try:
        findings, report = await v.verify(
            server.host,
            server.port,
            tls=False,
            credentials=v.Credentials("client-id", "client-secret", "access-token"),
            settings=settings(symbols=(EURUSD,), minutes=0.005),
            account_id=account_venue.ACCOUNT_ID,
            rate_limiter=RateLimiter({BUCKET_DEFAULT: 1000.0, BUCKET_HISTORICAL: 1000.0}),
        )
    finally:
        await server.stop()

    item = next(f for f in findings if f.item == "8")
    assert item.status == status
    assert last_line in item.detail[-1]
    assert last_line in report


@pytest.mark.parametrize("is_live", [True, False], ids=["live", "demo"])
async def test_resolving_the_account_waits_out_a_rate_limit_block(
    monkeypatch: pytest.MonkeyPatch,
    is_live: bool,
) -> None:
    """The lookup goes through the read-only requester, so a `BLOCKED_PAYLOAD_TYPE` on the
    account list is waited out and retried instead of failing the run."""
    server = account_venue.venue(is_live=is_live)
    answers = iter(
        [
            oa.ProtoOAErrorRes(errorCode="BLOCKED_PAYLOAD_TYPE", retryAfter=0),
            account_venue.account_list(is_live=is_live),
        ],
    )
    server.on(oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ, lambda _r: next(answers))
    await server.start()
    monkeypatch.setattr(v, "DEMO_HOST", server.host)
    monkeypatch.setattr(v, "PROTOBUF_PORT", server.port)
    monkeypatch.setattr(v, "CTraderConnection", functools.partial(CTraderConnection, tls=False))
    try:
        resolved = await v._resolve_account(
            account_venue.TRADER_LOGIN,
            v.Credentials("client-id", "client-secret", "access-token"),
        )
    finally:
        await server.stop()

    host = v.LIVE_HOST if is_live else server.host
    assert resolved == (host, account_venue.ACCOUNT_ID)
    assert len(account_venue.received(server, oa.ProtoOAGetAccountListByAccessTokenReq)) == 2
