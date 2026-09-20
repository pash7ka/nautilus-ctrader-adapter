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
import importlib.util
import pathlib
import sys
import time

import pytest

from nautilus_ctrader.common.errors import CTraderRequestError
from nautilus_ctrader.common.rate_limit import RateLimiter
from nautilus_ctrader.constants import BUCKET_DEFAULT, BUCKET_HISTORICAL, SYMBOL_BY_ID_BATCH
from nautilus_ctrader.enums import PERIOD_SECS
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests import account_venue
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
        "account_id": account_venue.ACCOUNT_ID,
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
    named = sorted(cls.__name__ for cls in v.READ_ONLY_REQUESTS)
    assert named
    assert not [name for name in named if any(word in name for word in forbidden)]


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
    args = v.build_arg_parser().parse_args(["--account-id", "42"])
    parsed = v.settings_from_args(args)
    assert parsed.account_id == 42
    assert parsed.minutes == 5.0
    assert parsed.symbols == ("EURUSD", "XAUUSD")
    # The batch the adapter itself uses, so a default run verifies the number it depends on.
    assert parsed.symbol_batch == SYMBOL_BY_ID_BATCH
    assert parsed.history_budget > 0


def test_the_account_id_is_required() -> None:
    with pytest.raises(SystemExit):
        v.build_arg_parser().parse_args([])


def test_every_setting_can_be_overridden() -> None:
    args = v.build_arg_parser().parse_args(
        [
            "--account-id",
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


def test_item_3_accepts_aligned_intraday_periods() -> None:
    status, detail = v.decide_alignment(
        [_aligned(EURUSD, "M1", 60), _aligned(EURUSD, "H1", 3600)],
    )
    assert status == OK
    assert len(detail) == 2


def test_item_3_flags_an_open_time_off_the_period() -> None:
    status, detail = v.decide_alignment([_aligned(EURUSD, "M15", 900, offset=7)])
    assert status == DIFFERS
    assert "not multiples of 900" in detail[0]


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
    assert v.decide_wide_boundaries(None, 500)[0] == DIFFERS


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


def verify_venue(*, count_cap: int = 1000, max_window_days: int = 30) -> FakeCTraderServer:
    """The reference venue, plus history and subscriptions shaped like the live one.

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
    server.on(
        om.PROTO_OA_GET_TRENDBARS_REQ,
        lambda r: _trendbars(r, count_cap=count_cap, max_window_days=max_window_days),
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


async def test_a_whole_run_reports_every_item_and_never_prints_the_account_id() -> None:
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
            # The fake venue is local; the live budget would only slow the test down.
            rate_limiter=RateLimiter({BUCKET_DEFAULT: 1000.0, BUCKET_HISTORICAL: 1000.0}),
        )
        await pusher
    finally:
        pusher.cancel()
        await server.stop()

    by_item = {f.item: f for f in findings}
    assert sorted(by_item) == ["0", "1", "2", "3", "4", "5a", "5b", "6", "7"]
    assert by_item["0"].status == OK
    assert by_item["1"].status == OK
    assert by_item["2"].status == OK
    assert by_item["3"].status == OK
    assert by_item["5a"].status == OK
    assert by_item["5b"].status == OK
    assert by_item["6"].status == OK
    assert by_item["7"].status == OK
    # No M1 bar can close inside a window this short, and that is an UNKNOWN, not a failure.
    assert by_item["4"].status == UNKNOWN

    assert str(account_venue.ACCOUNT_ID) not in report
    assert "access-token" not in report
    assert "client-id" not in report


async def test_a_venue_that_refuses_history_leaves_the_other_items_decided() -> None:
    """One failed probe must not take the rest of the report with it."""
    server = verify_venue()
    server.on(
        om.PROTO_OA_GET_TRENDBARS_REQ,
        lambda r: oa.ProtoOAErrorRes(
            ctidTraderAccountId=r.ctidTraderAccountId,
            errorCode="NO_QUOTES",
        ),
    )
    await server.start()
    try:
        findings, report = await v.verify(
            server.host,
            server.port,
            tls=False,
            credentials=v.Credentials("client-id", "client-secret", "access-token"),
            settings=settings(minutes=0.005),
            rate_limiter=RateLimiter({BUCKET_DEFAULT: 1000.0, BUCKET_HISTORICAL: 1000.0}),
        )
    finally:
        await server.stop()

    by_item = {f.item: f for f in findings}
    assert by_item["3"].status == UNKNOWN
    assert by_item["5a"].status == UNKNOWN
    assert by_item["5b"].status == DIFFERS  # rejected, but with the wrong code
    assert by_item["1"].status == OK
    assert by_item["6"].status == OK
    assert "NO_QUOTES" in report


async def test_every_subscription_is_released_before_the_connection_closes() -> None:
    server = verify_venue()
    await server.start()
    try:
        await v.verify(
            server.host,
            server.port,
            tls=False,
            credentials=v.Credentials("client-id", "client-secret", "access-token"),
            settings=settings(symbols=(EURUSD,), minutes=0.005),
            rate_limiter=RateLimiter({BUCKET_DEFAULT: 1000.0, BUCKET_HISTORICAL: 1000.0}),
        )
    finally:
        await server.stop()

    subscribed = sum(1 for m in server.received if isinstance(m, oa.ProtoOASubscribeSpotsReq))
    released = sum(1 for m in server.received if isinstance(m, oa.ProtoOAUnsubscribeSpotsReq))
    assert subscribed == released == 1
    live_bars = [m for m in server.received if isinstance(m, oa.ProtoOASubscribeLiveTrendbarReq)]
    dropped = [m for m in server.received if isinstance(m, oa.ProtoOAUnsubscribeLiveTrendbarReq)]
    # One refused probe for item 1, one real subscription; only the real one needs releasing.
    assert len(live_bars) == 2
    assert len(dropped) == 1
