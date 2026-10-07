"""Read-only live verification of the market-data assumptions this adapter is built on.

Connects to the account's own host, asks history a bounded number of times, subscribes to spots
and live M1 trendbars for a few minutes, and prints one `OK` / `DIFFERS` / `UNKNOWN` line per
assumption with the values it actually observed.

**Read-only by construction.** The account lookup uses the package's account-list helper,
which sends only application authentication and the list. Every other request goes through
`ReadOnlyRequester.request`, which refuses any payload class outside `READ_ONLY_REQUESTS`; that
set holds authentication, symbol, history and subscription requests only, and no order request
type is imported or named anywhere in this module. Subscriptions taken here are released before
the connection closes.

One item checks how history answers a trendbar window: with up to `count` bars counted back
from `toTimestamp` by open time, a bar opening exactly on it included, and `fromTimestamp` not
bounding the answer. It asks for the exact one-minute window of two consecutive closed M1 bars
of the first symbol, plus the same window narrowed and widened by 1 ms at each end.

Run from the repository root, while the symbols' market is open:

    uv run python scripts/verify_live_data.py --trader-login <login> --minutes 5

The trader login is the account number the broker gave you; the `ctidTraderAccountId` every
request carries is looked up from it. Neither is ever printed, in the report or in any error
message; a venue rejection is reported by error code only, never by its description.

Exit code is 1 if any item is `DIFFERS` or the run could not start at all, 0 otherwise - an
`UNKNOWN` item is a probe that could not run, not a failure. One failing probe never stops the
rest, and the report is printed from whatever was decided before the run ended.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import importlib.util
import pathlib
import sys
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field

from google.protobuf.message import Message

from nautilus_ctrader.common.account import AccountRecord, account_host, list_granted_accounts
from nautilus_ctrader.common.connection import CTraderConnection
from nautilus_ctrader.common.errors import CTraderProtocolError, CTraderRequestError
from nautilus_ctrader.common.parsing import bar_boundary_secs, price_from_raw
from nautilus_ctrader.common.rate_limit import RateLimiter
from nautilus_ctrader.constants import (
    BUCKET_DEFAULT,
    BUCKET_HISTORICAL,
    DEMO_HOST,
    LIVE_HOST,
    PROTOBUF_PORT,
    SYMBOL_BY_ID_BATCH,
)
from nautilus_ctrader.enums import PERIOD_SECS
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

# scripts/ is not a package; get_tokens.py is loaded by file path, exactly as
# tests/test_get_tokens.py does, to reuse its load_env() without duplicating it.
_GET_TOKENS_SPEC = importlib.util.spec_from_file_location(
    "get_tokens",
    pathlib.Path(__file__).resolve().with_name("get_tokens.py"),
)
get_tokens = importlib.util.module_from_spec(_GET_TOKENS_SPEC)
sys.modules[_GET_TOKENS_SPEC.name] = get_tokens
_GET_TOKENS_SPEC.loader.exec_module(get_tokens)


# The only payload classes this script is allowed to send. Nothing that creates, amends or
# closes an order is listed, so a read-only run is a property of this set rather than of every
# call site below.
READ_ONLY_REQUESTS: frozenset[type[Message]] = frozenset(
    {
        oa.ProtoOAApplicationAuthReq,
        oa.ProtoOAAccountAuthReq,
        oa.ProtoOAGetAccountListByAccessTokenReq,
        oa.ProtoOASymbolsListReq,
        oa.ProtoOASymbolByIdReq,
        oa.ProtoOAGetTrendbarsReq,
        oa.ProtoOASubscribeSpotsReq,
        oa.ProtoOAUnsubscribeSpotsReq,
        oa.ProtoOASubscribeLiveTrendbarReq,
        oa.ProtoOAUnsubscribeLiveTrendbarReq,
    },
)

OK = "OK"
DIFFERS = "DIFFERS"
UNKNOWN = "UNKNOWN"

Decision = tuple[str, tuple[str, ...]]

# The poll cadence item 4 asks for is 4/s, just under the documented historical budget of 5/s;
# a burst with no limiter at all is what once earned a live BLOCKED_PAYLOAD_TYPE.
_RATE_LIMITS = {BUCKET_DEFAULT: 5.0, BUCKET_HISTORICAL: 4.0}
_MAX_RATE_LIMIT_RETRIES = 3
_RATE_LIMIT_FALLBACK_WAIT_SECS = 2.0

_HISTORY_POLL_INTERVAL_SECS = 0.25
_HISTORY_POLL_WINDOW_SECS = 10.0
_HISTORY_REQUEST_TIMEOUT_SECS = 30.0

# Item 2: a unit fits if it puts the venue's timestamp within this of the local clock.
_TIMESTAMP_TOLERANCE_SECS = 5.0
_TIMESTAMP_UNITS = (("milliseconds", 1e-3), ("seconds", 1.0), ("microseconds", 1e-6))
# Items 2 and 4: how far into the past a closed market can leave the last tick it served. A
# weekend plus a holiday fits in a week; anything older is a wrong unit, not a shut market.
_STALE_STREAM_MAX_AGE_SECS = 7 * 86_400.0
# Item 4: an M1 bar followed live is seconds old, so a much older one was replayed from a
# stream that had stopped moving.
_STALE_BAR_AGE_SECS = 3600.0

# Item 3: bars per period, and how wide a window to ask for them over. Three periods' worth
# covers a weekend without the window itself becoming the thing under test.
# H4 and H12 are here because they are the periods in doubt: a 21:00 trading day is a whole
# number of hours, so everything up to H1 keeps its epoch alignment either way, while
# 75600 s is not a multiple of 4 or 12 hours.
_ALIGNMENT_PERIODS = ("M1", "M15", "H1", "H4", "H12", "D1")
_ALIGNMENT_COUNT = 10
_ALIGNMENT_WINDOW_FACTOR = 3

# Item 5a: one window for every count tried, wide enough to hold 5000 M1 bars of an open
# market and still under a week, so a per-period range cap cannot be what answers.
_COUNT_PROBE_COUNTS = (500, 1000, 5000)
_COUNT_PROBE_WINDOW_SECS = int(6.9 * 86_400)
# The adapter pages history at this size, so this is the count a run has to honour.
_REQUIRED_COUNT = 500

# Item 5b: far wider than any documented per-period range.
_WIDE_WINDOW_SECS = 400 * 86_400

# Item 8: the windows searched for two consecutive closed M1 bars, narrowest first. The wide one
# is for a closed market, where the newest bars are days old.
_EDGE_SEARCH_WINDOWS = ((1800, "the last 30 minutes"), (3 * 86_400, "the last 3 days"))
# Item 8: more than a one-minute window holds, so an answer that reaches back past the window's
# start shows that `fromTimestamp` does not bound it.
_EDGE_PROBE_COUNT = 10

_MAX_LISTED_DETAILS = 8

# The spot window plus room for authentication, the history probes and the release of every
# subscription. A run that overruns this is stuck, not slow, and reports what it already has.
_RUN_OVERHEAD_SECS = 300.0
_RESOLVE_ACCOUNT_TIMEOUT_SECS = 60.0

_M1_SECS = PERIOD_SECS[om.M1]
_SECS_PER_HOUR = 3600
_SECS_PER_DAY = 86_400


class ReadOnlyViolation(RuntimeError):
    """A payload outside `READ_ONLY_REQUESTS` reached the requester."""


class RequestBudgetExhausted(RuntimeError):
    """The run's historical-request budget is spent; the caller stops probing."""


@dataclass(frozen=True)
class Finding:
    """One report line: the item's verdict and the values it was reached from."""

    item: str
    title: str
    status: str
    detail: tuple[str, ...] = ()


@dataclass(frozen=True)
class Settings:
    trader_login: int
    symbols: tuple[str, ...]
    minutes: float
    grace_secs: float
    history_budget: int
    symbol_batch: int


@dataclass(frozen=True)
class Credentials:
    client_id: str
    client_secret: str
    access_token: str


@dataclass(frozen=True)
class AlignmentObservation:
    """Open times history served for one symbol and period."""

    symbol: str
    period_name: str
    period_secs: int
    open_secs: tuple[int, ...] = ()
    error_code: str | None = None


@dataclass(frozen=True)
class BarClose:
    """How one M1 bar closed, and when history first served it."""

    symbol: str
    boundary_secs: int
    closed_by: str
    history_delay_secs: float | None
    polls: int
    error_code: str | None = None


@dataclass(frozen=True)
class CountProbe:
    count: int
    returned: int | None
    has_more: bool = False
    error_code: str | None = None


@dataclass(frozen=True)
class WindowEdgeObservation:
    """What history served for exact-edge windows around one closed M1 bar.

    `bar_secs` is the open time of the earlier bar of the pair, `None` when the search found
    no pair, and `searched_open_secs` what the search that found it served. The edge window is
    `[bar, bar + 1 min]`; the control window is the same one narrowed by 1 ms at each end, the
    positive control widened by 1 ms at each end. `error_code` is the venue's refusal of the
    search, the `*_error` fields of a control.
    """

    symbol: str
    searched: str
    bar_secs: int | None = None
    searched_open_secs: tuple[int, ...] = ()
    edge_open_secs: tuple[int, ...] = ()
    control_open_secs: tuple[int, ...] = ()
    control_error: str | None = None
    positive_open_secs: tuple[int, ...] = ()
    positive_error: str | None = None
    error_code: str | None = None


@dataclass
class SpotObservations:
    """What the spot stream showed during the run."""

    timestamp: int | None = None
    timestamp_local_secs: float = 0.0
    prices_checked: int = 0
    price_failures: list[str] = field(default_factory=list)
    bar_closes: list[BarClose] = field(default_factory=list)


class QuietLogger:
    """Enough of the Nautilus `Logger` interface for `CTraderConnection`.

    Debug and info are dropped so the report is the only thing on stdout; warnings and errors
    go to stderr, where they cannot be mistaken for a report line.
    """

    def debug(self, message: str) -> None:
        pass

    def info(self, message: str) -> None:
        pass

    def warning(self, message: str) -> None:
        print(f"warning: {message}", file=sys.stderr)

    def error(self, message: str) -> None:
        print(f"error: {message}", file=sys.stderr)

    def exception(self, message: str, ex: BaseException) -> None:
        print(f"error: {message}: {type(ex).__name__}", file=sys.stderr)


class ReadOnlyRequester:
    """The one place this script sends anything, and the read-only guarantee itself.

    A payload whose class is not in `READ_ONLY_REQUESTS` is refused before it reaches the
    socket. Historical requests are additionally capped at `history_budget` for the whole run,
    so the polling in item 4 cannot turn into an unbounded stream of requests, and a
    `BLOCKED_PAYLOAD_TYPE` is waited out rather than retried immediately.
    """

    def __init__(self, connection: CTraderConnection, *, history_budget: int) -> None:
        self._connection = connection
        self._history_budget = history_budget
        self.requests = 0
        self.historical_requests = 0
        self.rate_limit_blocks = 0

    @property
    def history_remaining(self) -> int:
        return max(0, self._history_budget - self.historical_requests)

    def set_event_handler(self, handler: Callable[[Message], None]) -> None:
        self._connection.set_event_handler(handler)

    async def request(
        self,
        payload: Message,
        *,
        bucket: str = BUCKET_DEFAULT,
        timeout_secs: float | None = None,
    ) -> Message:
        if type(payload) not in READ_ONLY_REQUESTS:
            raise ReadOnlyViolation(
                f"{type(payload).__name__} is not one of this script's read-only requests",
            )
        if bucket == BUCKET_HISTORICAL:
            if self.history_remaining == 0:
                raise RequestBudgetExhausted(
                    f"the run's budget of {self._history_budget} historical requests is spent",
                )
            self.historical_requests += 1
        self.requests += 1

        for attempt in range(_MAX_RATE_LIMIT_RETRIES + 1):
            try:
                return await self._connection.request(
                    payload,
                    bucket=bucket,
                    timeout_secs=timeout_secs,
                )
            except CTraderRequestError as e:
                if e.error_code != "BLOCKED_PAYLOAD_TYPE":
                    raise
                self.rate_limit_blocks += 1
                if attempt == _MAX_RATE_LIMIT_RETRIES:
                    raise
                await asyncio.sleep(
                    e.retry_after_secs
                    if e.retry_after_secs is not None
                    else _RATE_LIMIT_FALLBACK_WAIT_SECS,
                )
        raise AssertionError("unreachable: the loop above always returns or raises")


# -- Decisions ------------------------------------------------------------------------------
#
# Each one turns an observation into a verdict and the lines that justify it, with no I/O, so
# the report a live run prints can be checked offline against a canned observation.


def decide_trendbar_without_spots(error_code: str | None) -> Decision:
    if error_code == "NOT_SUBSCRIBED_TO_SPOTS":
        return OK, ("rejected with NOT_SUBSCRIBED_TO_SPOTS",)
    if error_code is None:
        return DIFFERS, ("accepted without a spot subscription",)
    return DIFFERS, (f"rejected with {error_code}, not NOT_SUBSCRIBED_TO_SPOTS",)


def decide_spot_timestamp(timestamp: int | None, local_secs: float) -> Decision:
    if timestamp is None:
        return UNKNOWN, ("no spot event carried a timestamp field",)
    skews = {name: timestamp * scale - local_secs for name, scale in _TIMESTAMP_UNITS}
    fitting = [name for name, skew in skews.items() if abs(skew) <= _TIMESTAMP_TOLERANCE_SECS]
    as_ms = f"as milliseconds it is {skews['milliseconds']:+.3f} s from the local clock"
    if fitting[:1] == ["milliseconds"]:
        return OK, (f"timestamp present, {as_ms}",)
    if fitting:
        return DIFFERS, (f"timestamp present, the unit that fits is {fitting[0]}", as_ms)
    age_secs = -skews["milliseconds"]
    if _TIMESTAMP_TOLERANCE_SECS < age_secs <= _STALE_STREAM_MAX_AGE_SECS:
        # A closed market keeps serving the last tick before the close, which is minutes to
        # days old. The unit is not what is wrong then, and a stale stream cannot prove one.
        return UNKNOWN, (
            "the spot stream looks stale: the market was probably closed",
            f"read as milliseconds the last tick is {age_secs / 3600:.1f} h before the run, "
            "a plausible last trading time, so no unit is decided either way",
        )
    return DIFFERS, ("timestamp present, no known unit lands within 5 s of local time", as_ms)


def decide_alignment(observations: Sequence[AlignmentObservation]) -> Decision:
    """Whether each period's open times keep one consistent phase, and what that phase is.

    Alignment to the Unix epoch is not the thing worth testing: the venue's trading day starts
    at an offset into the calendar day, so a period that does not divide that offset carries the
    offset instead. What matters to a consumer is that a period has *one* phase, whatever it is,
    because a single phase is what makes a boundary predictable. A period whose bars all open on
    a multiple of their own length is reported as aligned; one with a single other phase is
    reported with the times of day its bars open at; more than one phase is a real surprise.
    """
    if not any(o.open_secs for o in observations):
        codes = sorted({o.error_code or "no bars" for o in observations}) or ["no probe ran"]
        return UNKNOWN, (f"history served nothing to check: {', '.join(codes)}",)

    detail: list[str] = []
    differs = False
    for observation in observations:
        prefix = f"{observation.symbol} {observation.period_name}"
        if not observation.open_secs:
            detail.append(f"{prefix}: {observation.error_code or 'no bars'}")
            continue
        phases = {s % observation.period_secs for s in observation.open_secs}
        times = ", ".join(
            _hhmm_of_day(o) for o in sorted({s % _SECS_PER_DAY for s in observation.open_secs})
        )
        if phases == {0}:
            detail.append(f"{prefix}: {len(observation.open_secs)} open times aligned")
        elif len(phases) == 1 and next(iter(phases)) % _SECS_PER_HOUR == 0:
            # A trading day starts on the hour, so the only phase it can impose is a whole
            # number of hours. That is the venue's day showing through, not a surprise.
            detail.append(f"{prefix}: one whole-hour phase, bars open at {times} UTC")
        elif len(phases) == 1:
            differs = True
            detail.append(
                f"{prefix}: one phase, but {next(iter(phases))} s is not a whole hour, so the "
                f"trading day does not explain it; bars open at {times} UTC",
            )
        else:
            differs = True
            detail.append(f"{prefix}: more than one phase, bars open at {times} UTC")
    return (DIFFERS if differs else OK), tuple(detail)


def decide_bar_closes(closes: Sequence[BarClose], local_secs: float) -> Decision:
    if not closes:
        return UNKNOWN, ("no M1 bar closed and was polled for during the run",)
    detail = [
        f"{c.symbol} {_utc_hhmm(c.boundary_secs)}: closed by {c.closed_by}, "
        + (
            f"history served it {c.history_delay_secs:.2f} s after the boundary ({c.polls} polls)"
            if c.history_delay_secs is not None
            else f"history had not served it after {c.polls} polls"
            + (f" ({c.error_code})" if c.error_code else "")
        )
        for c in closes[:_MAX_LISTED_DETAILS]
    ]
    if len(closes) > _MAX_LISTED_DETAILS:
        detail.append(f"... and {len(closes) - _MAX_LISTED_DETAILS} more")
    newest_age_secs = local_secs - max(c.boundary_secs for c in closes)
    if newest_age_secs > _STALE_BAR_AGE_SECS:
        # Every bar followed was the last one before a close: how long history took to serve
        # it is the age of that close, and says nothing about a bar closing in a live market.
        detail.append(
            f"the newest bar followed is {newest_age_secs / 3600:.1f} h older than the run: "
            "the stream was stale and the market was probably closed",
        )
        return UNKNOWN, tuple(detail)
    timer_closed = sum(1 for c in closes if c.closed_by != "stream")
    unserved = sum(1 for c in closes if c.history_delay_secs is None)
    if timer_closed or unserved:
        detail.append(
            f"{timer_closed} of {len(closes)} needed the timer, {unserved} were never served "
            f"by history within {_HISTORY_POLL_WINDOW_SECS:g} s",
        )
        return DIFFERS, tuple(detail)
    return OK, tuple(detail)


def decide_count_cap(probes: Sequence[CountProbe]) -> Decision:
    served = [p for p in probes if p.returned is not None]
    if not served:
        codes = sorted({p.error_code or "no response" for p in probes})
        return UNKNOWN, (f"no count was served: {', '.join(codes)}",)
    detail = [
        f"count={p.count}: "
        + (
            f"{p.returned} bars, hasMore={p.has_more}"
            if p.returned is not None
            else f"rejected with {p.error_code}"
        )
        for p in probes
    ]
    # The venue counts back from `toTimestamp` and has been seen to serve one bar short of a
    # full page, so one short still counts as honoured.
    largest = max((p.count for p in served if p.returned >= p.count - 1), default=0)
    detail.append(f"largest count honoured: {largest or 'none'}")
    return (OK if largest >= _REQUIRED_COUNT else DIFFERS), tuple(detail)


def decide_wide_boundaries(error_code: str | None, returned: int | None) -> Decision:
    days = _WIDE_WINDOW_SECS // _SECS_PER_DAY
    if error_code == "INCORRECT_BOUNDARIES":
        return OK, (f"a {days}-day window is rejected with INCORRECT_BOUNDARIES",)
    if error_code is not None:
        return DIFFERS, (f"a {days}-day window is rejected with {error_code}",)
    # Serving a short page for a wide window is this venue's confirmed behaviour, not a
    # surprise. Either answer is legitimate; the item exists to record which one is given.
    return OK, (
        f"a {days}-day window is accepted, {returned} bars served",
        "a short page therefore says nothing about whether older bars exist",
    )


def decide_symbol_batch(
    wanted: int,
    sent: int,
    returned: int | None,
    error_code: str | None,
) -> Decision:
    """`wanted` is the batch size asked for, `sent` the ids the account actually offers."""
    if sent < 1:
        return UNKNOWN, ("no symbol ids were available to ask for",)
    # An account with fewer symbols than the batch size still verifies everything up to its
    # own count, which is the number worth reporting.
    short = () if sent >= wanted else (f"only {sent} of the {wanted} ids asked for are offered",)
    if error_code is not None:
        return DIFFERS, (f"{sent} ids in one request rejected with {error_code}", *short)
    if returned != sent:
        return DIFFERS, (f"{sent} ids sent, {returned} symbols returned", *short)
    return OK, (f"{sent} ids in one request accepted, {returned} symbols returned", *short)


def decide_price_digits(checked: int, failures: Sequence[str]) -> Decision:
    if checked == 0:
        return UNKNOWN, ("no spot price was observed",)
    if not failures:
        return OK, (f"{checked} spot prices are representable at their symbol's digits",)
    detail = [f"{len(failures)} of {checked} spot prices are not representable"]
    detail.extend(failures[:_MAX_LISTED_DETAILS])
    return DIFFERS, tuple(detail)


def find_consecutive_closed_pair(open_secs: Sequence[int]) -> int | None:
    """The open time of the earlier bar of the newest two consecutive M1 bars, or `None`.

    The newest bar served may still be forming, so a pair is only taken if its later bar is
    older than the newest one.
    """
    opens = set(open_secs)
    if not opens:
        return None
    newest = max(opens)
    return next(
        (b for b in sorted(opens, reverse=True) if b + _M1_SECS < newest and b + _M1_SECS in opens),
        None,
    )


def decide_window_edges(observation: WindowEdgeObservation) -> Decision:
    """Whether history answers a window with `count` bars counted back from `toTimestamp`.

    That is the behaviour confirmed live: bars are selected by open time, one opening exactly
    on `toTimestamp` is served, and `fromTimestamp` does not bound the answer. Each window's
    answer is compared with the newest `count` bars at or before its `toTimestamp` among every
    bar the run saw, so a bar after `toTimestamp`, a bar skipped, or fewer bars than `count`
    while older ones exist is `DIFFERS` - except an answer missing only the oldest bar, since
    `count = N` answered with `N - 1` bars has been seen before and is noted instead. A refused
    control only withdraws its own evidence. If no answer reaches back past its
    `fromTimestamp`, nothing shows the venue ignores it, and the verdict is `UNKNOWN`.
    """
    if observation.bar_secs is None:
        reason = observation.error_code or "the venue served no such pair"
        return UNKNOWN, (
            f"no two consecutive closed M1 bars of {observation.symbol} found in "
            f"{observation.searched}: {reason}",
        )
    first = observation.bar_secs
    second = first + _M1_SECS
    windows = (
        ("exact window", "exact window", 0, observation.edge_open_secs, None),
        (
            "control",
            "control, 1 ms inside both edges",
            1,
            observation.control_open_secs,
            observation.control_error,
        ),
        (
            "positive control",
            "positive control, 1 ms outside both edges",
            -1,
            observation.positive_open_secs,
            observation.positive_error,
        ),
    )
    seen = set(observation.searched_open_secs)
    for _, _, _, served, _ in windows:
        seen.update(served)
    detail = [
        f"{observation.symbol} M1, count={_EDGE_PROBE_COUNT}, exact window "
        f"{_utc_stamp(first)} to {_utc_stamp(second)}",
    ]
    anomalies = []
    reaches_back = False
    for name, label, inset_ms, served, error in windows:
        if error:
            detail.append(f"{name} refused: {error}")
            continue
        detail.append(f"{label}: {_opens_text(served)}")
        from_ms = first * 1000 + inset_ms
        to_ms = second * 1000 - inset_ms
        expected = sorted(b for b in seen if b * 1000 <= to_ms)[-_EDGE_PROBE_COUNT:]
        answered = sorted(set(served))
        if len(expected) == _EDGE_PROBE_COUNT and answered == expected[1:]:
            detail.append(
                f"the {name} returned one bar fewer than count, missing only the oldest; "
                "answers of count - 1 bars are a known habit of the venue",
            )
        elif answered != expected:
            anomalies.append(
                f"the {name} returned {_opens_text(served)}, where {_EDGE_PROBE_COUNT} bars "
                f"counted back from its toTimestamp by open time are {_opens_text(expected)}",
            )
        reaches_back = reaches_back or any(b * 1000 < from_ms for b in served)
    if anomalies:
        return DIFFERS, (*detail, *anomalies)
    if not reaches_back:
        detail.append(
            "no answer reached back past its fromTimestamp, so whether fromTimestamp bounds the "
            "answer or history held no older bar cannot be told",
        )
        return UNKNOWN, tuple(detail)
    detail.append(
        "answers count bars back from toTimestamp by open time, inclusive; fromTimestamp does "
        "not bound the answer",
    )
    return OK, tuple(detail)


def _opens_text(open_secs: Sequence[int]) -> str:
    if not open_secs:
        return "no bars"
    return "bars opening at " + ", ".join(_utc_stamp(s) for s in sorted(open_secs))


def _utc_stamp(epoch_secs: int) -> str:
    return time.strftime("%Y-%m-%d %H:%MZ", time.gmtime(epoch_secs))


def _utc_hhmm(epoch_secs: int) -> str:
    return time.strftime("%H:%MZ", time.gmtime(epoch_secs))


def _hhmm_of_day(secs: int) -> str:
    return f"{secs // 3600:02d}:{secs % 3600 // 60:02d}"


# -- The run --------------------------------------------------------------------------------


class Verifier:
    """Runs every item against one authenticated connection, collecting findings as it goes.

    `findings` is filled item by item, so a run cut short by the hard time bound still has
    everything already decided. A probe that raises records `UNKNOWN` and the next item runs.
    """

    def __init__(
        self,
        requester: ReadOnlyRequester,
        settings: Settings,
        account_id: int,
    ) -> None:
        self._requester = requester
        self._settings = settings
        self._account_id = account_id
        self.findings: list[Finding] = []
        self.unresolved_symbols: tuple[str, ...] = ()
        self._symbol_ids: dict[str, int] = {}
        self._digits: dict[int, int] = {}
        self._names: dict[int, str] = {}
        self._all_symbol_ids: tuple[int, ...] = ()
        self._spots = SpotObservations()
        self._m1_boundary: dict[int, int] = {}
        self._m1_rolled_at: dict[tuple[int, int], float] = {}
        self._rolled = asyncio.Event()

    async def run(self) -> None:
        await self._item("0", "symbols and their specifications", self._load_symbols)
        await self._item(
            "1",
            "live trendbar without a spot subscription",
            self._probe_trendbar_without_spots,
        )
        await self._item("3", "trendbar open times are period-aligned", self._probe_alignment)
        await self._item("5a", "largest historical count honoured", self._probe_count_cap)
        await self._item("5b", "a very wide from/to is rejected", self._probe_wide_boundaries)
        await self._item(
            "6",
            f"{self._settings.symbol_batch} symbol ids in one request",
            self._probe_symbol_batch,
        )
        await self._item(
            "8",
            "trendbar window: count bars back from toTimestamp, fromTimestamp not bounding",
            self._probe_window_edges,
        )
        await self._run_spot_window()

    # -- Item plumbing ----------------------------------------------------------------------

    async def _item(self, item: str, title: str, probe: Callable[[], Awaitable[Decision]]) -> None:
        status, detail = await self._safely(probe)
        self.findings.append(Finding(item, title, status, detail))

    async def _safely(self, probe: Callable[[], Awaitable[Decision]]) -> Decision:
        try:
            return await probe()
        except CTraderRequestError as e:
            # The venue's own description is never printed: it is the one part of a rejection
            # that could echo something identifying back.
            return UNKNOWN, (f"the probe failed with {e.error_code}",)
        except Exception as e:
            # Deliberately broad: one failed probe must never stop the items after it.
            return UNKNOWN, (f"the probe failed with {type(e).__name__}",)

    async def _request(
        self,
        payload: Message,
        *,
        bucket: str = BUCKET_DEFAULT,
        timeout_secs: float | None = None,
    ) -> Message:
        return await self._requester.request(payload, bucket=bucket, timeout_secs=timeout_secs)

    # -- Symbols ----------------------------------------------------------------------------

    async def _load_symbols(self) -> Decision:
        """Item 0. Resolves the requested names and their digits; every later item needs it."""
        listed = await self._request(
            oa.ProtoOASymbolsListReq(ctidTraderAccountId=self._account_id),
        )
        by_name = {s.symbolName: s.symbolId for s in listed.symbol}
        self._all_symbol_ids = tuple(s.symbolId for s in listed.symbol if s.enabled)
        self._symbol_ids = {
            name: by_name[name] for name in self._settings.symbols if name in by_name
        }
        self.unresolved_symbols = tuple(
            name for name in self._settings.symbols if name not in by_name
        )
        self._names = {symbol_id: name for name, symbol_id in self._symbol_ids.items()}
        offered = f"{len(self._all_symbol_ids)} enabled symbols offered"
        if not self._symbol_ids:
            return UNKNOWN, (offered, "none of the requested symbols is among them")
        specs = await self._request(
            oa.ProtoOASymbolByIdReq(
                ctidTraderAccountId=self._account_id,
                symbolId=list(self._symbol_ids.values()),
            ),
        )
        self._digits = {s.symbolId: s.digits for s in specs.symbol}
        digits = ", ".join(
            f"{name} digits={self._digits.get(symbol_id, '?')}"
            for name, symbol_id in self._symbol_ids.items()
        )
        status = DIFFERS if self.unresolved_symbols else OK
        detail = [offered, digits]
        if self.unresolved_symbols:
            detail.append(f"not offered: {', '.join(self.unresolved_symbols)}")
        return status, tuple(detail)

    @property
    def _first_symbol(self) -> tuple[str, int] | None:
        return next(iter(self._symbol_ids.items()), None)

    # -- Probes -----------------------------------------------------------------------------

    async def _probe_trendbar_without_spots(self) -> Decision:
        """Item 1. Must run before anything subscribes spots, or it proves nothing."""
        first = self._first_symbol
        if first is None:
            return UNKNOWN, ("none of the requested symbols is offered",)
        _, symbol_id = first
        try:
            await self._request(
                oa.ProtoOASubscribeLiveTrendbarReq(
                    ctidTraderAccountId=self._account_id,
                    symbolId=symbol_id,
                    period=om.M1,
                ),
            )
        except CTraderRequestError as e:
            return decide_trendbar_without_spots(e.error_code)
        # It was accepted, so the subscription exists and has to be given back.
        with contextlib.suppress(Exception):
            await self._request(
                oa.ProtoOAUnsubscribeLiveTrendbarReq(
                    ctidTraderAccountId=self._account_id,
                    symbolId=symbol_id,
                    period=om.M1,
                ),
            )
        return decide_trendbar_without_spots(None)

    async def _probe_alignment(self) -> Decision:
        """Item 3."""
        if not self._symbol_ids:
            return UNKNOWN, ("none of the requested symbols is offered",)
        now_secs = int(time.time())
        observations: list[AlignmentObservation] = []
        for name, symbol_id in self._symbol_ids.items():
            for period_name in _ALIGNMENT_PERIODS:
                period = om.ProtoOATrendbarPeriod.Value(period_name)
                period_secs = PERIOD_SECS[period]
                window = _ALIGNMENT_COUNT * period_secs * _ALIGNMENT_WINDOW_FACTOR
                try:
                    response = await self._trendbars(
                        symbol_id,
                        period,
                        from_secs=now_secs - window,
                        to_secs=now_secs,
                        count=_ALIGNMENT_COUNT,
                    )
                except CTraderRequestError as e:
                    observations.append(
                        AlignmentObservation(
                            name, period_name, period_secs, error_code=e.error_code
                        ),
                    )
                    continue
                observations.append(
                    AlignmentObservation(
                        name,
                        period_name,
                        period_secs,
                        open_secs=tuple(t.utcTimestampInMinutes * 60 for t in response.trendbar),
                    ),
                )
        return decide_alignment(observations)

    async def _probe_count_cap(self) -> Decision:
        """Item 5a."""
        first = self._first_symbol
        if first is None:
            return UNKNOWN, ("none of the requested symbols is offered",)
        _, symbol_id = first
        now_secs = int(time.time())
        probes: list[CountProbe] = []
        for count in _COUNT_PROBE_COUNTS:
            try:
                response = await self._trendbars(
                    symbol_id,
                    om.M1,
                    from_secs=now_secs - _COUNT_PROBE_WINDOW_SECS,
                    to_secs=now_secs,
                    count=count,
                )
            except CTraderRequestError as e:
                probes.append(CountProbe(count, None, error_code=e.error_code))
                continue
            probes.append(CountProbe(count, len(response.trendbar), has_more=response.hasMore))
        return decide_count_cap(probes)

    async def _probe_wide_boundaries(self) -> Decision:
        """Item 5b."""
        first = self._first_symbol
        if first is None:
            return UNKNOWN, ("none of the requested symbols is offered",)
        _, symbol_id = first
        now_secs = int(time.time())
        try:
            response = await self._trendbars(
                symbol_id,
                om.M1,
                from_secs=now_secs - _WIDE_WINDOW_SECS,
                to_secs=now_secs,
                count=_REQUIRED_COUNT,
            )
        except CTraderRequestError as e:
            return decide_wide_boundaries(e.error_code, None)
        return decide_wide_boundaries(None, len(response.trendbar))

    async def _probe_symbol_batch(self) -> Decision:
        """Item 6."""
        wanted = self._settings.symbol_batch
        batch = list(self._all_symbol_ids[:wanted])
        if not batch:
            return decide_symbol_batch(wanted, 0, None, None)
        try:
            response = await self._request(
                oa.ProtoOASymbolByIdReq(
                    ctidTraderAccountId=self._account_id,
                    symbolId=batch,
                ),
            )
        except CTraderRequestError as e:
            return decide_symbol_batch(wanted, len(batch), None, e.error_code)
        returned = len(response.symbol) + len(response.archivedSymbol)
        return decide_symbol_batch(wanted, len(batch), returned, None)

    async def _probe_window_edges(self) -> Decision:
        """Item 8."""
        first = self._first_symbol
        if first is None:
            return UNKNOWN, ("none of the requested symbols is offered",)
        name, symbol_id = first
        end_secs = int(time.time()) // _M1_SECS * _M1_SECS
        bar_secs: int | None = None
        searched_opens: tuple[int, ...] = ()
        error_code: str | None = None
        searched = ""
        for window_secs, label in _EDGE_SEARCH_WINDOWS:
            searched = label
            try:
                response = await self._trendbars(
                    symbol_id,
                    om.M1,
                    from_secs=end_secs - window_secs,
                    to_secs=end_secs,
                    count=_REQUIRED_COUNT,
                )
            except CTraderRequestError as e:
                # A refused window is not the end of the search: the wider one may be served.
                error_code = e.error_code
                continue
            error_code = None
            searched_opens = _open_secs(response)
            bar_secs = find_consecutive_closed_pair(searched_opens)
            if bar_secs is not None:
                break
        if bar_secs is None:
            return decide_window_edges(WindowEdgeObservation(name, searched, error_code=error_code))

        # An edge window that fails leaves nothing to decide, so it propagates; a failed
        # control only withdraws its own evidence.
        edge = await self._trendbars_ms(
            symbol_id,
            om.M1,
            from_ms=bar_secs * 1000,
            to_ms=(bar_secs + _M1_SECS) * 1000,
            count=_EDGE_PROBE_COUNT,
        )
        control, control_error = await self._control_window(
            symbol_id,
            from_ms=bar_secs * 1000 + 1,
            to_ms=(bar_secs + _M1_SECS) * 1000 - 1,
        )
        positive, positive_error = await self._control_window(
            symbol_id,
            from_ms=bar_secs * 1000 - 1,
            to_ms=(bar_secs + _M1_SECS) * 1000 + 1,
        )
        return decide_window_edges(
            WindowEdgeObservation(
                name,
                searched,
                bar_secs=bar_secs,
                searched_open_secs=searched_opens,
                edge_open_secs=_open_secs(edge),
                control_open_secs=control,
                control_error=control_error,
                positive_open_secs=positive,
                positive_error=positive_error,
            ),
        )

    async def _control_window(
        self,
        symbol_id: int,
        *,
        from_ms: int,
        to_ms: int,
    ) -> tuple[tuple[int, ...], str | None]:
        """M1 open times served for a control window, or the venue's refusal code."""
        try:
            response = await self._trendbars_ms(
                symbol_id,
                om.M1,
                from_ms=from_ms,
                to_ms=to_ms,
                count=_EDGE_PROBE_COUNT,
            )
        except CTraderRequestError as e:
            return (), e.error_code
        return _open_secs(response), None

    # -- The spot window: items 2, 4 and 7 --------------------------------------------------

    async def _run_spot_window(self) -> None:
        """Subscribe, watch for `minutes`, then decide items 2, 4 and 7 from what arrived.

        The three share one subscription window, so they are decided together whether the
        window ran to its end or the subscription failed outright.
        """
        error: Decision | None
        if not self._symbol_ids:
            error = (UNKNOWN, ("none of the requested symbols is offered",))
        else:
            error = await self._safely(self._collect_spots)
            if error[0] == OK:
                error = None

        for item, title, decision in (
            (
                "2",
                "spot timestamp is present, and its unit",
                decide_spot_timestamp(self._spots.timestamp, self._spots.timestamp_local_secs),
            ),
            (
                "4",
                "M1 bar closes and when history serves them",
                decide_bar_closes(self._spots.bar_closes, time.time()),
            ),
            (
                "7",
                "spot prices are representable at the symbol's digits",
                decide_price_digits(self._spots.prices_checked, self._spots.price_failures),
            ),
        ):
            status, detail = error if error is not None else decision
            self.findings.append(Finding(item, title, status, detail))

    async def _collect_spots(self) -> Decision:
        """Run the subscription window.

        `OK` here means only that the window ran, so items 2, 4 and 7 can be decided from what
        it collected; anything else is the verdict all three get instead.
        """
        symbol_ids = list(self._symbol_ids.values())
        self._requester.set_event_handler(self._on_event)
        try:
            await self._request(
                oa.ProtoOASubscribeSpotsReq(
                    ctidTraderAccountId=self._account_id,
                    symbolId=symbol_ids,
                    subscribeToSpotTimestamp=True,
                ),
            )
        except CTraderRequestError as e:
            return UNKNOWN, (f"the spot subscription failed with {e.error_code}",)

        try:
            for symbol_id in symbol_ids:
                await self._request(
                    oa.ProtoOASubscribeLiveTrendbarReq(
                        ctidTraderAccountId=self._account_id,
                        symbolId=symbol_id,
                        period=om.M1,
                    ),
                )
            deadline_secs = time.time() + self._settings.minutes * 60
            name, symbol_id = next(iter(self._symbol_ids.items()))
            watcher = asyncio.create_task(
                self._watch_m1_closes(name, symbol_id, deadline_secs),
            )
            try:
                await asyncio.sleep(max(0.0, deadline_secs - time.time()))
            finally:
                watcher.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await watcher
        finally:
            await self._release(symbol_ids)
        return OK, ()

    async def _release(self, symbol_ids: Sequence[int]) -> None:
        """Give every subscription back, whatever happened to the window."""
        for symbol_id in symbol_ids:
            with contextlib.suppress(Exception):
                await self._request(
                    oa.ProtoOAUnsubscribeLiveTrendbarReq(
                        ctidTraderAccountId=self._account_id,
                        symbolId=symbol_id,
                        period=om.M1,
                    ),
                )
        with contextlib.suppress(Exception):
            await self._request(
                oa.ProtoOAUnsubscribeSpotsReq(
                    ctidTraderAccountId=self._account_id,
                    symbolId=list(symbol_ids),
                ),
            )

    def _on_event(self, message: Message) -> None:
        if not isinstance(message, oa.ProtoOASpotEvent):
            return
        now = time.time()
        if message.HasField("timestamp") and self._spots.timestamp is None:
            self._spots.timestamp = message.timestamp
            self._spots.timestamp_local_secs = now
        self._check_prices(message)
        self._track_m1(message, now)

    def _check_prices(self, event: oa.ProtoOASpotEvent) -> None:
        """Item 7, against the adapter's own converter rather than a rule restated here."""
        digits = self._digits.get(event.symbolId)
        if digits is None:
            return
        for name in ("bid", "ask"):
            if not event.HasField(name):
                continue
            raw = getattr(event, name)
            self._spots.prices_checked += 1
            try:
                price_from_raw(raw, digits)
            except CTraderProtocolError:
                if len(self._spots.price_failures) < _MAX_LISTED_DETAILS:
                    symbol = self._names.get(event.symbolId, str(event.symbolId))
                    self._spots.price_failures.append(f"{symbol} {name} {raw} at {digits} digits")

    def _track_m1(self, event: oa.ProtoOASpotEvent, now: float) -> None:
        """Note when the stream rolls its forming M1 bar - the moment it closes the last one."""
        for trendbar in event.trendbar:
            if trendbar.period != om.M1:
                continue
            boundary = bar_boundary_secs(trendbar.utcTimestampInMinutes)
            previous = self._m1_boundary.get(event.symbolId)
            if previous is not None and boundary <= previous:
                continue
            if previous is not None:
                self._m1_rolled_at.setdefault((event.symbolId, previous), now)
            self._m1_boundary[event.symbolId] = boundary
            self._rolled.set()

    async def _watch_m1_closes(self, symbol: str, symbol_id: int, deadline_secs: float) -> None:
        """Item 4, for the first requested symbol.

        Every bar the stream starts forming is followed to its boundary and then polled for in
        history. One symbol, because the poll window is the run's whole request budget.
        """
        handled: int | None = None
        while time.time() < deadline_secs:
            boundary = self._m1_boundary.get(symbol_id)
            if boundary is None or boundary == handled:
                # Nothing runs between the read above and this clear: a single event loop, and
                # no await in between, so no roll can be missed here.
                self._rolled.clear()
                with contextlib.suppress(TimeoutError):
                    async with asyncio.timeout(deadline_secs - time.time()):
                        await self._rolled.wait()
                continue
            handled = boundary
            await self._observe_close(symbol, symbol_id, boundary, deadline_secs)

    async def _observe_close(
        self,
        symbol: str,
        symbol_id: int,
        boundary: int,
        deadline_secs: float,
    ) -> None:
        period_secs = PERIOD_SECS[om.M1]
        end_secs = boundary + period_secs
        if end_secs + _HISTORY_POLL_WINDOW_SECS > deadline_secs:
            return
        await asyncio.sleep(max(0.0, end_secs - time.time()))
        delay, polls, error_code = await self._poll_for_bar(symbol_id, boundary)

        # The stream only counts as having closed the bar if it rolled before the timer in
        # `BarCloser` would have fired, so the verdict waits out the grace period.
        await asyncio.sleep(max(0.0, end_secs + self._settings.grace_secs - time.time()))
        rolled_at = self._m1_rolled_at.get((symbol_id, boundary))
        closed_by = (
            "stream"
            if rolled_at is not None and rolled_at < end_secs + self._settings.grace_secs
            else "timer"
        )
        self._spots.bar_closes.append(
            BarClose(symbol, boundary, closed_by, delay, polls, error_code),
        )

    async def _poll_for_bar(
        self,
        symbol_id: int,
        boundary: int,
    ) -> tuple[float | None, int, str | None]:
        """Ask history for the bar at `boundary` until it has it, or the window runs out."""
        period_secs = PERIOD_SECS[om.M1]
        started = time.time()
        polls = 0
        while time.time() - started < _HISTORY_POLL_WINDOW_SECS:
            polls += 1
            try:
                response = await self._trendbars(
                    symbol_id,
                    om.M1,
                    from_secs=boundary,
                    to_secs=boundary + period_secs,
                    # Two for a one-bar window, as the data client does: the venue counts
                    # `count` back from `toTimestamp`.
                    count=2,
                )
            except RequestBudgetExhausted:
                return None, polls - 1, "budget exhausted"
            except CTraderRequestError as e:
                return None, polls, e.error_code
            served = any(
                bar_boundary_secs(t.utcTimestampInMinutes) == boundary for t in response.trendbar
            )
            if served:
                return time.time() - (boundary + period_secs), polls, None
            await asyncio.sleep(_HISTORY_POLL_INTERVAL_SECS)
        return None, polls, None

    async def _trendbars(
        self,
        symbol_id: int,
        period: int,
        *,
        from_secs: int,
        to_secs: int,
        count: int,
    ) -> oa.ProtoOAGetTrendbarsRes:
        return await self._trendbars_ms(
            symbol_id,
            period,
            from_ms=from_secs * 1_000,
            to_ms=to_secs * 1_000,
            count=count,
        )

    async def _trendbars_ms(
        self,
        symbol_id: int,
        period: int,
        *,
        from_ms: int,
        to_ms: int,
        count: int,
    ) -> oa.ProtoOAGetTrendbarsRes:
        return await self._request(
            oa.ProtoOAGetTrendbarsReq(
                ctidTraderAccountId=self._account_id,
                symbolId=symbol_id,
                period=period,
                fromTimestamp=from_ms,
                toTimestamp=to_ms,
                count=count,
            ),
            bucket=BUCKET_HISTORICAL,
            timeout_secs=_HISTORY_REQUEST_TIMEOUT_SECS,
        )


def _open_secs(response: oa.ProtoOAGetTrendbarsRes) -> tuple[int, ...]:
    return tuple(t.utcTimestampInMinutes * 60 for t in response.trendbar)


def _item_sort_key(finding: Finding) -> tuple[int, str]:
    """Report the items in their own numbering, not in the order the run happened to reach."""
    digits = "".join(c for c in finding.item if c.isdigit())
    return (int(digits) if digits else 99, finding.item)


def format_report(
    findings: Sequence[Finding],
    *,
    settings: Settings,
    unresolved_symbols: Sequence[str] = (),
    requests: int,
    historical_requests: int,
    rate_limit_blocks: int,
) -> str:
    """The whole report. Carries symbol names, prices and timings, and no identifiers."""
    header = [
        "cTrader live market-data verification (read-only)",
        f"symbols: {', '.join(settings.symbols)}   spot window: {settings.minutes:g} minutes",
    ]
    if unresolved_symbols:
        header.append(f"not offered by this account: {', '.join(unresolved_symbols)}")
    header += [
        f"requests: {requests} ({historical_requests} historical, budget "
        f"{settings.history_budget}), rate-limit blocks: {rate_limit_blocks}",
        "",
    ]

    lines = list(header)
    for finding in sorted(findings, key=_item_sort_key):
        lines.append(f"{finding.status:<8} {finding.item}. {finding.title}")
        lines.extend(f"         {line}" for line in finding.detail)
    counts = Counter(f.status for f in findings)
    lines += ["", f"{OK} {counts[OK]}, {DIFFERS} {counts[DIFFERS]}, {UNKNOWN} {counts[UNKNOWN]}"]
    return "\n".join(lines)


async def verify(
    host: str,
    port: int,
    *,
    tls: bool,
    credentials: Credentials,
    settings: Settings,
    account_id: int,
    rate_limiter: RateLimiter | None = None,
) -> tuple[list[Finding], str]:
    """Authenticate, run every item, and return the findings with the formatted report.

    `account_id` is the `ctidTraderAccountId` `_resolve_account()` read off the account
    list; `settings.trader_login` is what the caller asked for and is never sent.

    The connection is closed whatever happens, and the report is built from whatever was
    decided before an error or the hard time bound cut the run short. `rate_limiter` defaults
    to the venue-shaped budget; a test against a local server has no reason to wait on it.
    """
    connection = CTraderConnection(
        host,
        port,
        logger=QuietLogger(),
        tls=tls,
        rate_limiter=RateLimiter(_RATE_LIMITS) if rate_limiter is None else rate_limiter,
    )
    requester = ReadOnlyRequester(connection, history_budget=settings.history_budget)
    verifier = Verifier(requester, settings, account_id)
    await connection.connect()
    try:
        await requester.request(
            oa.ProtoOAApplicationAuthReq(
                clientId=credentials.client_id,
                clientSecret=credentials.client_secret,
            ),
        )
        await requester.request(
            oa.ProtoOAAccountAuthReq(
                ctidTraderAccountId=account_id,
                accessToken=credentials.access_token,
            ),
        )
        try:
            async with asyncio.timeout(settings.minutes * 60 + _RUN_OVERHEAD_SECS):
                await verifier.run()
        except TimeoutError:
            verifier.findings.append(
                Finding(
                    "*", "the run hit its hard time bound", UNKNOWN, ("items below are missing",)
                ),
            )
    finally:
        await connection.close()
    report = format_report(
        verifier.findings,
        settings=settings,
        unresolved_symbols=verifier.unresolved_symbols,
        requests=requester.requests,
        historical_requests=requester.historical_requests,
        rate_limit_blocks=requester.rate_limit_blocks,
    )
    return verifier.findings, report


def _no_such_account_message(granted: Sequence[AccountRecord], trader_login: int) -> str:
    """Why the account was not found, naming no identifier.

    The two identifiers are of similar length, and only the login is the account number the
    interface shows, so giving the other one is easy and the bare refusal reads as a token
    problem.
    """
    if any(a.ctid_trader_account_id == trader_login for a in granted):
        return (
            "the value given is a ctidTraderAccountId, not a traderLogin; expected is the "
            "account number the cTrader interface shows, which this script resolves itself"
        )
    return f"the access token grants no account with that trader login (it grants {len(granted)})"


async def _resolve_account(trader_login: int, credentials: Credentials) -> tuple[str, int]:
    """The host the account lives on, and the `ctidTraderAccountId` every request carries.

    The account list is served on either host; it maps the login to the id, and its `isLive`
    flag decides where the account itself can be authenticated.
    """
    async with asyncio.timeout(_RESOLVE_ACCOUNT_TIMEOUT_SECS):
        listed = await list_granted_accounts(
            credentials.client_id,
            credentials.client_secret,
            credentials.access_token,
            host=DEMO_HOST,
            port=PROTOBUF_PORT,
            logger=QuietLogger(),
        )
    matched = [a for a in listed.accounts if a.trader_login == trader_login]
    if not matched:
        raise RuntimeError(_no_such_account_message(listed.accounts, trader_login))
    if len(matched) > 1:
        raise RuntimeError("more than one granted account has that trader login")
    account = matched[0]
    host = account_host(account.is_live, demo_host=DEMO_HOST, live_host=LIVE_HOST)
    return host, account.ctid_trader_account_id


def parse_symbols(text: str) -> tuple[str, ...]:
    """Split a comma-separated `--symbols` value, keeping the venue's own spelling."""
    symbols = tuple(name.strip() for name in text.split(",") if name.strip())
    if not symbols:
        raise argparse.ArgumentTypeError("at least one symbol name is required")
    return symbols


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Verify this adapter's market-data assumptions against a live account, read-only."
        ),
    )
    parser.add_argument(
        "--trader-login",
        type=int,
        required=True,
        help="the account number the broker gave you, as the cTrader interface shows it",
    )
    parser.add_argument(
        "--minutes",
        type=float,
        default=5.0,
        help="how long to watch the spot stream (default: 5)",
    )
    parser.add_argument(
        "--symbols",
        type=parse_symbols,
        default=parse_symbols("EURUSD,XAUUSD"),
        help=(
            "comma-separated venue symbol names; the first one is the one whose M1 bar closes "
            "are followed (default: EURUSD,XAUUSD)"
        ),
    )
    parser.add_argument(
        "--bar-close-grace-secs",
        type=float,
        default=1.0,
        help="how long after a bar's end the stream still counts as having closed it",
    )
    parser.add_argument(
        "--history-budget",
        type=int,
        default=400,
        help="hard cap on historical requests for the whole run (default: 400)",
    )
    parser.add_argument(
        "--symbol-batch",
        type=int,
        default=SYMBOL_BY_ID_BATCH,
        help=(
            "how many symbol ids to ask for in one request; the default is the batch size the "
            f"adapter itself uses ({SYMBOL_BY_ID_BATCH}). An account offering fewer ids is "
            "probed with the ids it has"
        ),
    )
    return parser


def settings_from_args(args: argparse.Namespace) -> Settings:
    return Settings(
        trader_login=args.trader_login,
        symbols=tuple(args.symbols),
        minutes=args.minutes,
        grace_secs=args.bar_close_grace_secs,
        history_budget=args.history_budget,
        symbol_batch=args.symbol_batch,
    )


async def _run(settings: Settings, env: dict[str, str]) -> tuple[list[Finding], str]:
    credentials = Credentials(
        client_id=env["CTRADER_CLIENT_ID"],
        client_secret=env["CTRADER_CLIENT_SECRET"],
        access_token=env["CTRADER_ACCESS_TOKEN"],
    )
    host, account_id = await _resolve_account(settings.trader_login, credentials)
    return await verify(
        host,
        PROTOBUF_PORT,
        tls=True,
        credentials=credentials,
        settings=settings,
        account_id=account_id,
    )


def main(argv: list[str] | None = None) -> int:
    settings = settings_from_args(build_arg_parser().parse_args(argv))
    try:
        env = get_tokens.load_env(_REPO_ROOT / ".env")
        findings, report = asyncio.run(_run(settings, env))
    except Exception as e:
        # An uncaught traceback would print the exception's own message, which for a venue
        # rejection carries the venue's description. Only the type name reaches the terminal.
        print(f"error: {type(e).__name__}: the verification run failed", file=sys.stderr)
        return 1
    print(report)
    return 1 if any(f.status == DIFFERS for f in findings) else 0


if __name__ == "__main__":
    sys.exit(main())
