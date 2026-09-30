"""Record a cTrader account's execution events while its owner trades by hand.

Read-only. The script listens for the events the venue pushes, asks for snapshots of the
account's positions and orders, and writes a scrubbed fixture. Every message it sends goes
through `send()`, which refuses any request class outside `READ_ONLY_REQUESTS`; no request that
places, changes or closes an order is named anywhere in this module.

    uv run python scripts/record_execution.py --trader-login <login>

While it runs, type a short note and press Enter to mark what you just did in the terminal
("moved the stop"); type `q` and Enter to stop. `--describe` prints a recording back.

The fixture never holds the account id, the trader login, a token, a balance, or the broker's
own order, position and deal ids: those are replaced consistently, so a position still lines up
with its orders and deals. A timestamp in milliseconds is shifted by one constant, which keeps
every interval; a time the venue gives in another unit keeps its value.

A session is kept however it ends. What was seen is first written unscrubbed to
`tests/recordings/`, which git ignores, and only then scrubbed, checked and written as the
fixture. If that second step fails the fixture is left alone, and the first file rebuilds it
later, with no connection:

    uv run python scripts/record_execution.py --rescrub tests/recordings/<name>.raw.json

The unscrubbed file is never printed, by `--describe` or otherwise. Neither file is replaced
without `--overwrite`, and a run that recorded nothing writes nothing. A refusal by the venue
ends the run at once and is reported by its error code.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import importlib.util
import json
import os
import pathlib
import re
import sys
import tempfile
import threading
import time
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

from google.protobuf import text_format
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import Message

from nautilus_ctrader.common import codec
from nautilus_ctrader.common.connection import CTraderConnection
from nautilus_ctrader.common.errors import CTraderError, CTraderRequestError
from nautilus_ctrader.common.rate_limit import RateLimiter
from nautilus_ctrader.constants import (
    BUCKET_DEFAULT,
    BUCKET_HISTORICAL,
    DEMO_HOST,
    LIVE_HOST,
    PROTOBUF_PORT,
)
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
_OUTPUT_PATH = _REPO_ROOT / "tests" / "fixtures" / "m3_execution_recorded.json"
# Ignored by git: what is written here is unscrubbed and holds the account's real identifiers.
_RAW_DIR = _REPO_ROOT / "tests" / "recordings"


def _load_sibling(name: str):
    """scripts/ is not a package: a sibling script is loaded by file path."""
    spec = importlib.util.spec_from_file_location(
        name,
        pathlib.Path(__file__).resolve().with_name(f"{name}.py"),
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


get_tokens = _load_sibling("get_tokens")
record_fixtures = _load_sibling("record_fixtures")

# The only payload classes this script may send. Each reads; none changes the account. The
# guarantee is this set, not the care taken at each call site.
READ_ONLY_REQUESTS: frozenset[type[Message]] = frozenset(
    {
        oa.ProtoOAApplicationAuthReq,
        oa.ProtoOAAccountAuthReq,
        oa.ProtoOAGetAccountListByAccessTokenReq,
        oa.ProtoOATraderReq,
        oa.ProtoOAReconcileReq,
        oa.ProtoOADealListReq,
        oa.ProtoOAOrderListReq,
        oa.ProtoOADealListByPositionIdReq,
        oa.ProtoOAOrderListByPositionIdReq,
    },
)

SCRUBBED_TEXT = "scrubbed"
# Stands in for a long number in free text that no field identified.
NUMBER_PLACEHOLDER = "<number>"
# An arbitrary fixed instant: every timestamp is moved so the recording starts here.
FAKE_EPOCH_MS = 1_600_000_000_000
FORMAT = 1

_RATE_LIMITS = {BUCKET_DEFAULT: 5.0, BUCKET_HISTORICAL: 1.0}
_REQUEST_TIMEOUT_SECS = 30.0
_TICK_SECS = 0.2
# A snapshot is taken this long after the last event of a burst, so one manual action yields
# one snapshot rather than one per event.
_SNAPSHOT_DEBOUNCE_SECS = 2.0
_RECONNECT_WAIT_SECS = (2.0, 5.0, 10.0, 30.0)
_CLOSING_LOOKBACK_MS = 3_600_000

# What `record()` tells its `status` callback. Fixed texts: no identifier can get into one.
STATUS_STARTED = "recording started"
STATUS_LOST = "connection lost, reconnecting"
STATUS_RECONNECTED = "reconnected"
STATUS_DEADLINE = "deadline reached"

# How the recorder's own markers begin when they name a list the recording lacks, in whole or
# in part: the owner has to read those before trusting it.
_PROBLEM_MARKERS = ("closing ",)
# What only the unscrubbed file holds, and so what tells it from a fixture.
_RAW_KEYS = frozenset({"started_wall_ms", "account_id", "login"})

_ID_KINDS = {"positionId": "position", "orderId": "order", "dealId": "deal"}
_ID_BASES = {"position": 5_000_000, "order": 6_000_000, "deal": 7_000_000}
# Free text the account's owner, or a robot of theirs, may have written.
_TEXT_FIELDS = frozenset({"label", "comment", "clientOrderId", "externalNote"})
# The venue's own wording: kept as evidence, with the numbers that identify taken out.
_VENUE_TEXT_FIELDS = frozenset({"description", "reason"})
# Money moved in or out of the account: nothing the order logic needs.
_CLEARED_MESSAGES = frozenset({"depositWithdraw", "bonusDepositWithdraw"})
_INT_TYPES = (FieldDescriptor.TYPE_INT64, FieldDescriptor.TYPE_UINT64)
_DIGIT_RUN = re.compile(r"\d+")
# Shorter than any id seen, longer than a price or a volume someone would type in a note.
_LONG_NUMBER_DIGITS = 7


class ReadOnlyViolation(RuntimeError):
    """A request outside `READ_ONLY_REQUESTS` was about to be sent."""


async def send(
    connection: CTraderConnection,
    payload: Message,
    *,
    bucket: str = BUCKET_DEFAULT,
) -> Message:
    """The one place this script sends anything."""
    if type(payload) not in READ_ONLY_REQUESTS:
        raise ReadOnlyViolation(
            f"{type(payload).__name__} is not one of this script's read-only requests",
        )
    return await connection.request(payload, bucket=bucket, timeout_secs=_REQUEST_TIMEOUT_SECS)


class IdMap:
    """The broker's order, position and deal ids, each replaced by a stable fake one."""

    def __init__(self) -> None:
        self._fake: dict[str, dict[int, int]] = {kind: {} for kind in _ID_BASES}

    def fake(self, kind: str, real: int) -> int:
        known = self._fake[kind]
        if real not in known:
            known[real] = _ID_BASES[kind] + len(known) + 1
        return known[real]

    def real_ids(self) -> list[int]:
        return [real for known in self._fake.values() for real in known]

    def text_fakes(self) -> dict[str, str]:
        """Each real id as it reads in text, with the fake id that replaces it there."""
        fakes: dict[str, str] = {}
        for known in self._fake.values():
            for real, fake in known.items():
                fakes.setdefault(str(real), str(fake))
        return fakes


def _in_decimal(text: str, start: int, end: int) -> bool:
    """Whether the digits `text[start:end]` are one side of a decimal number."""
    fraction_follows = text[end : end + 1] == "." and text[end + 1 : end + 2].isdigit()
    is_fraction = start >= 2 and text[start - 1] == "." and text[start - 2].isdigit()
    return fraction_follows or is_fraction


def clean_text(text: str, *, account_id: int, login: int | None, ids: IdMap) -> str:
    """Free text with the numbers that could identify the account taken out.

    Works on whole runs of digits: the digits of an id inside a longer number are not that id.

    - a known order, position or deal id becomes its fake id;
    - the account id and the trader login become the fake values their own fields get;
    - any other run of `_LONG_NUMBER_DIGITS` digits or more becomes `NUMBER_PLACEHOLDER`: an id
      quoted only in text was never seen in a field, so it cannot be mapped. A run that is the
      integer or the fractional part of a decimal number is a price or a rate, and is kept.
    """
    fakes = ids.text_fakes()
    fakes.setdefault(str(account_id), str(record_fixtures.FAKE_ACCOUNT_ID))
    if login is not None:
        fakes.setdefault(str(login), str(record_fixtures.FAKE_TRADER_LOGIN))
    known_fakes = set(fakes.values())

    def replace(match: re.Match[str]) -> str:
        run = match.group()
        if run in fakes:
            return fakes[run]
        if len(run) < _LONG_NUMBER_DIGITS or run in known_fakes or _in_decimal(text, *match.span()):
            return run
        return NUMBER_PLACEHOLDER

    return _DIGIT_RUN.sub(replace, text)


def scrub_execution(
    message: Message,
    *,
    account_id: int,
    login: int | None,
    ids: IdMap,
    shift_ms: int,
) -> Message:
    """`record_fixtures.scrub()`, then what execution messages add to it."""
    result = record_fixtures.scrub(message, account_id, login)
    # A field newer than these bindings is scrubbed by nothing here, and may hold anything.
    result.DiscardUnknownFields()

    def clean(text: str) -> str:
        return clean_text(text, account_id=account_id, login=login, ids=ids)

    _scrub_in_place(result, ids, shift_ms, clean)
    return result


def _scrub_in_place(
    message: Message,
    ids: IdMap,
    shift_ms: int,
    clean: Callable[[str], str],
) -> None:
    for descriptor, value in list(message.ListFields()):
        name = descriptor.name
        if name in _CLEARED_MESSAGES:
            message.ClearField(name)
        elif descriptor.type == FieldDescriptor.TYPE_MESSAGE:
            items = value if descriptor.label == FieldDescriptor.LABEL_REPEATED else (value,)
            for item in items:
                _scrub_in_place(item, ids, shift_ms, clean)
        elif descriptor.label == FieldDescriptor.LABEL_REPEATED:
            continue
        elif name in _ID_KINDS and descriptor.type in _INT_TYPES:
            # Zero or less stands for "no id": mapping it would replace every 0 in a text.
            if value > 0:
                setattr(message, name, ids.fake(_ID_KINDS[name], value))
        elif descriptor.type == FieldDescriptor.TYPE_STRING and name in _TEXT_FIELDS:
            setattr(message, name, SCRUBBED_TEXT)
        elif descriptor.type == FieldDescriptor.TYPE_STRING and name in _VENUE_TEXT_FIELDS:
            setattr(message, name, clean(value))
        # Below the shift it is not a wall time in milliseconds, and an unsigned field refuses
        # the negative result.
        elif (
            name.endswith("Timestamp")
            and descriptor.type in _INT_TYPES
            and value > max(shift_ms, 0)
        ):
            setattr(message, name, value - shift_ms)


def _has_unknown_fields(message: Message) -> bool:
    """Whether `message`, at any depth, carries a field these bindings do not know."""
    known = type(message)()
    known.CopyFrom(message)
    known.DiscardUnknownFields()
    return known.ByteSize() != message.ByteSize()


def _unknown_fields_note(recording: Recording) -> str:
    """The marker naming the message types that carried unknown fields; empty if none did."""
    types = sorted({type(m).__name__ for m in recording.messages() if _has_unknown_fields(m)})
    return f"unknown fields dropped from: {', '.join(types)}" if types else ""


@dataclass
class Entry:
    t: float
    kind: str  # "event", "snapshot" or "marker"
    note: str
    message: Message | None
    # A marker the owner typed, as against one the recorder wrote. Not part of the fixture.
    typed: bool = False


@dataclass
class Recording:
    """What was seen, unscrubbed; scrubbing happens once, when it is encoded."""

    started_wall_ms: int
    timeline: list[Entry] = field(default_factory=list)
    closing: dict[str, list[Message]] = field(
        default_factory=lambda: {
            "deals": [],
            "orders": [],
            "position_orders": [],
            "position_deals": [],
        },
    )

    def add(
        self,
        kind: str,
        note: str,
        message: Message | None,
        t: float,
        *,
        typed: bool = False,
    ) -> None:
        self.timeline.append(Entry(round(t, 3), kind, note, message, typed))

    def messages(self) -> Iterable[Message]:
        for entry in self.timeline:
            if entry.message is not None:
                yield entry.message
        for items in self.closing.values():
            yield from items


def _encode(message: Message, *, partial: bool = False) -> dict:
    # `partial` skips the check for unset required fields, so an odd message still goes out.
    body = message.SerializePartialToString() if partial else message.SerializeToString()
    return {"type": message.payloadType, "payload": base64.b64encode(body).decode("ascii")}


def _decode(item: dict) -> Message:
    message = codec.payload_class(item["type"])()
    message.ParseFromString(base64.b64decode(item["payload"]))
    return message


def encode_recording(
    recording: Recording,
    *,
    account_id: int,
    login: int | None,
) -> tuple[bytes, IdMap]:
    """The recording as the fixture's bytes, scrubbed, with the id map that scrubbing built.

    If any message carried a field these bindings do not know, one marker naming the message
    types is added at the end of the timeline: the fields themselves are dropped.
    """
    ids = IdMap()
    shift_ms = recording.started_wall_ms - FAKE_EPOCH_MS

    def clean(message: Message) -> Message:
        return scrub_execution(
            message,
            account_id=account_id,
            login=login,
            ids=ids,
            shift_ms=shift_ms,
        )

    # A first pass only to fill the id map, so a text quoting an id is fixed even where the id
    # itself first appears later in the recording.
    for message in recording.messages():
        clean(message)

    timeline = []
    for entry in recording.timeline:
        note = clean_text(entry.note, account_id=account_id, login=login, ids=ids)
        item: dict = {"t": entry.t, "kind": entry.kind, "note": note}
        if entry.message is not None:
            item.update(_encode(clean(entry.message)))
        timeline.append(item)
    closing = {
        key: [_encode(clean(message)) for message in items]
        for key, items in recording.closing.items()
    }
    unknown = _unknown_fields_note(recording)
    if unknown:
        last = timeline[-1]["t"] if timeline else 0.0
        timeline.append({"t": last, "kind": "marker", "note": unknown})
    output = {"format": FORMAT, "timeline": timeline, "closing": closing}
    return json.dumps(output, indent=2).encode("utf-8"), ids


def decode_recording(data: bytes) -> dict:
    """The fixture with every payload parsed back into its protobuf message."""
    raw = json.loads(data)
    return {
        "format": raw["format"],
        "timeline": [
            {
                "t": item["t"],
                "kind": item["kind"],
                "note": item["note"],
                "message": _decode(item) if "payload" in item else None,
            }
            for item in raw["timeline"]
        ],
        "closing": {
            key: [_decode(item) for item in items] for key, items in raw["closing"].items()
        },
    }


def encode_raw(recording: Recording, *, account_id: int, login: int | None) -> bytes:
    """The recording exactly as it was seen, with what `decode_raw()` needs to rebuild it.

    The fixture's shape, but nothing is scrubbed and nothing is checked, so neither step can
    stop it being written. It holds real identifiers: for a directory git ignores, only.
    """
    timeline = []
    for entry in recording.timeline:
        item: dict = {"t": entry.t, "kind": entry.kind, "note": entry.note}
        if entry.typed:
            item["typed"] = True
        if entry.message is not None:
            item.update(_encode(entry.message, partial=True))
        timeline.append(item)
    output = {
        "format": FORMAT,
        "started_wall_ms": recording.started_wall_ms,
        "account_id": account_id,
        "login": login,
        "timeline": timeline,
        "closing": {
            key: [_encode(message, partial=True) for message in items]
            for key, items in recording.closing.items()
        },
    }
    return json.dumps(output, indent=2).encode("utf-8")


def decode_raw(data: bytes) -> tuple[Recording, int, int | None]:
    """The recording, account id and trader login that `encode_raw()` wrote."""
    raw = json.loads(data)
    recording = Recording(raw["started_wall_ms"])
    recording.timeline = [
        Entry(
            item["t"],
            item["kind"],
            item["note"],
            _decode(item) if "payload" in item else None,
            item.get("typed", False),
        )
        for item in raw["timeline"]
    ]
    recording.closing = {
        key: [_decode(item) for item in items] for key, items in raw["closing"].items()
    }
    return recording, raw["account_id"], raw["login"]


def check_clean(
    data: bytes,
    recording: Recording,
    *,
    account_id: int,
    login: int | None,
    ids: IdMap,
    secrets: Iterable[str],
) -> None:
    """Raise `ScrubError` if a real identifier or secret survives in what is about to be written.

    Checked twice: each scrubbed message's own serialized bytes, where an int64 is a varint and
    no decimal search can see it, and the final JSON, where a text field or a note could hold it.
    """
    numbers = [n for n in (account_id, login, *ids.real_ids()) if n is not None]
    text = [str(n).encode() for n in numbers] + [s.encode() for s in secrets if s]
    varints = [record_fixtures._varint(n) for n in numbers]

    record_fixtures.assert_clean(data, text)
    decoded = decode_recording(data)
    messages = [item["message"] for item in decoded["timeline"] if item["message"] is not None]
    for items in decoded["closing"].values():
        messages.extend(items)
    if len(messages) != sum(1 for _ in recording.messages()):
        raise record_fixtures.ScrubError("the encoded recording lost a message")
    for message in messages:
        record_fixtures.assert_clean(message.SerializeToString(), text + varints)


def write_fixture(
    output: pathlib.Path,
    recording: Recording,
    *,
    account_id: int,
    login: int | None,
    secrets: Iterable[str],
) -> bytes:
    """Scrub `recording`, check the result, and only then write it to `output`.

    Returns the bytes written. Raises, with `output` untouched, if anything on the way fails.
    """
    data, ids = encode_recording(recording, account_id=account_id, login=login)
    check_clean(data, recording, account_id=account_id, login=login, ids=ids, secrets=secrets)
    _write_atomically(output, data)
    return data


def _write_atomically(path: pathlib.Path, data: bytes) -> None:
    """Replace `path` with `data` in one step: a failed write leaves the file as it was."""
    # The name ends like the file's own, so a leftover of a raw file is ignored by git too.
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".", suffix=f".{path.name}")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
        os.replace(temporary, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.remove(temporary)
        raise


class Refused(RuntimeError):
    """A condition `main` reports in the error's own words.

    The message is a fixed text, or names a path or an env key: never a login, an account id
    or a secret.
    """


def describe(data: bytes) -> str:
    """One line per timeline entry of a fixture.

    Raises `Refused` for an unscrubbed recording: what it holds is never printed.
    """
    if _RAW_KEYS & json.loads(data).keys():
        raise Refused(
            "this is an unscrubbed recording, which is never printed; "
            "build a fixture from it with --rescrub and describe that",
        )
    lines = []
    for item in decode_recording(data)["timeline"]:
        head = f"{item['t']:9.3f}  {item['kind']:<8}"
        message = item["message"]
        if message is None:
            lines.append(f"{head}  {item['note']}")
            continue
        body = text_format.MessageToString(message, as_one_line=True)
        note = f"[{item['note']}] " if item["note"] else ""
        lines.append(f"{head}  {note}{type(message).__name__} {body}")
    return "\n".join(lines)


def summary(recording: Recording) -> list[str]:
    """What the owner has to read about a recording.

    Each problem the run noted, then one last line with the counts - which says so in words
    if the recording holds no event at all. Only the recorder's own markers are quoted: they
    hold list names, error codes and type names, never an identifier.
    """
    kinds = Counter(entry.kind for entry in recording.timeline)
    problems = [
        entry.note
        for entry in recording.timeline
        if entry.kind == "marker" and not entry.typed and entry.note.startswith(_PROBLEM_MARKERS)
    ]
    unknown = _unknown_fields_note(recording)
    if unknown:
        problems.append(unknown)
    typed = sum(entry.typed for entry in recording.timeline)
    counts = f"{kinds['event']} events, {kinds['snapshot']} snapshots, {typed} notes"
    if not kinds["event"]:
        counts += " - NO EVENTS: nothing the venue pushed is in this recording"
    return [*problems, counts]


def events_status(names: Iterable[str]) -> str:
    """The `status` text for the events seen since the last snapshot: how many, of which types."""
    counts = Counter(names)
    types = ", ".join(f"{name} x{count}" for name, count in sorted(counts.items()))
    return f"events: {sum(counts.values())} ({types})"


def _position_ids(message: Message) -> set[int]:
    found: set[int] = set()
    for descriptor, value in message.ListFields():
        if descriptor.type == FieldDescriptor.TYPE_MESSAGE:
            items = value if descriptor.label == FieldDescriptor.LABEL_REPEATED else (value,)
            for item in items:
                found |= _position_ids(item)
        elif (
            descriptor.name == "positionId"
            and descriptor.label != FieldDescriptor.LABEL_REPEATED
            and value > 0
        ):
            found.add(value)
    return found


def _silent(_text: str) -> None:
    pass


async def record(
    host: str,
    port: int,
    *,
    tls: bool,
    client_id: str,
    client_secret: str,
    access_token: str,
    account_id: int,
    minutes: float,
    stop: asyncio.Event,
    markers: asyncio.Queue[str],
    rate_limiter: RateLimiter | None = None,
    snapshot_debounce_secs: float = _SNAPSHOT_DEBOUNCE_SECS,
    tick_secs: float = _TICK_SECS,
    reconnect_waits: tuple[float, ...] = _RECONNECT_WAIT_SECS,
    recording: Recording | None = None,
    status: Callable[[str], None] = _silent,
) -> Recording:
    """Listen until `stop` is set or `minutes` pass; reconnect if the connection drops.

    - `recording` lets the caller keep what was seen if this raises: it is filled in place.
    - `status` is called as the run changes state: with a `STATUS_*` text, with
      `events_status()` when a burst of events is over, or with the text of a
      `snapshot refused` marker.

    Raises `CTraderRequestError` if the venue refuses authentication, or the snapshot taken at
    the start or right after a reconnect: a refusal is not a lost connection, and asking again
    changes nothing. A snapshot refused later, after events, is only marked, and the run goes on.
    """
    recording = Recording(int(time.time() * 1000)) if recording is None else recording
    started = time.monotonic()
    deadline = started + minutes * 60

    def now() -> float:
        return time.monotonic() - started

    def finished() -> bool:
        return stop.is_set() or time.monotonic() >= deadline

    def drain_markers() -> None:
        while not markers.empty():
            recording.add("marker", markers.get_nowait(), None, now(), typed=True)

    async def snapshot(connection: CTraderConnection, note: str) -> None:
        # Asked both ways, and named by the flag alone.
        # TODO(verify): what `returnProtectionOrders` changes in the response; comparing the
        # two snapshots of a position that has a stop-loss shows it.
        for flag in (False, True):
            response = await send(
                connection,
                oa.ProtoOAReconcileReq(
                    ctidTraderAccountId=account_id,
                    returnProtectionOrders=flag,
                ),
            )
            shape = f"returnProtectionOrders={str(flag).lower()}"
            recording.add("snapshot", f"{note}; {shape}", response, now())

    async def closing(connection: CTraderConnection) -> None:
        async def ask(key: str, request: Message) -> None:
            try:
                response = await send(connection, request, bucket=BUCKET_HISTORICAL)
            except CTraderRequestError as e:
                # One list refused says nothing about the next: note it and ask the rest.
                note = f"closing list refused: {key} {e.error_code}"
                recording.add("marker", note, None, now())
                return
            recording.closing[key].append(response)
            # TODO(verify): that `hasMore` on these lists means what it says, and at how many
            # rows; a session with more deals than one response holds shows it.
            if response.hasMore:
                # Further pages are not asked for; the marker says the list is incomplete.
                recording.add("marker", f"closing list truncated: {key}", None, now())

        # TODO(verify): the widest window these two requests accept; a refusal here is marked
        # `closing list refused` with the venue's code.
        window = {
            "fromTimestamp": recording.started_wall_ms - _CLOSING_LOOKBACK_MS,
            "toTimestamp": int(time.time() * 1000),
        }
        await ask("deals", oa.ProtoOADealListReq(ctidTraderAccountId=account_id, **window))
        await ask("orders", oa.ProtoOAOrderListReq(ctidTraderAccountId=account_id, **window))
        positions: set[int] = set()
        for message in list(recording.messages()):
            positions |= _position_ids(message)
        # TODO(verify): that the by-position requests answer without a time window; a refusal
        # is marked like any other.
        for position_id in sorted(positions):
            await ask(
                "position_orders",
                oa.ProtoOAOrderListByPositionIdReq(
                    ctidTraderAccountId=account_id,
                    positionId=position_id,
                ),
            )
            await ask(
                "position_deals",
                oa.ProtoOADealListByPositionIdReq(
                    ctidTraderAccountId=account_id,
                    positionId=position_id,
                ),
            )

    def announce_deadline() -> None:
        if not stop.is_set():
            status(STATUS_DEADLINE)

    last_event: float | None = None
    # The types of the events the owner has not been told about yet.
    unannounced: list[str] = []

    def announce_events() -> None:
        if unannounced:
            status(events_status(unannounced))
            unannounced.clear()

    def on_event(message: Message) -> None:
        nonlocal last_event
        # A note typed before the event arrived goes before it, not at the next tick.
        drain_markers()
        recording.add("event", "", message, now())
        unannounced.append(type(message).__name__)
        last_event = time.monotonic()

    attempt = 0
    first = True
    while not finished():
        connection = CTraderConnection(
            host,
            port,
            logger=record_fixtures._QuietLogger(),
            tls=tls,
            rate_limiter=RateLimiter(_RATE_LIMITS) if rate_limiter is None else rate_limiter,
        )
        lost = asyncio.Event()
        last_event = None
        # TODO(verify): that account authorisation alone gets execution events pushed, with no
        # subscription request; a recording that holds an event confirms it.
        connection.set_event_handler(on_event)
        connection.set_disconnect_handler(lambda _error, lost=lost: lost.set())
        try:
            await connection.connect()
            await send(
                connection,
                oa.ProtoOAApplicationAuthReq(clientId=client_id, clientSecret=client_secret),
            )
            await send(
                connection,
                oa.ProtoOAAccountAuthReq(
                    ctidTraderAccountId=account_id,
                    accessToken=access_token,
                ),
            )
            if not first:
                recording.add("marker", "reconnected", None, now())
            await snapshot(connection, "start" if first else "after reconnect")
            status(STATUS_STARTED if first else STATUS_RECONNECTED)
            first = False
            attempt = 0

            while not finished() and not lost.is_set():
                drain_markers()
                seen = last_event
                if seen is not None and time.monotonic() - seen >= snapshot_debounce_secs:
                    last_event = None
                    announce_events()
                    try:
                        await snapshot(connection, "after events")
                    except CTraderRequestError as e:
                        # The events are recorded either way; only this view of the account
                        # is missing.
                        refused = f"snapshot refused: {e.error_code}"
                        recording.add("marker", refused, None, now())
                        status(refused)
                await asyncio.sleep(tick_secs)

            # A note typed during the last tick.
            drain_markers()
            if not lost.is_set():
                announce_deadline()
                try:
                    await closing(connection)
                except CTraderError as e:
                    recording.add(
                        "marker",
                        f"closing requests failed: {type(e).__name__}",
                        None,
                        now(),
                    )
                return recording
            recording.add("marker", "connection lost", None, now())
        except CTraderRequestError:
            # The venue answered, and refused: asking again on a new connection changes nothing.
            raise
        except (CTraderError, OSError) as e:
            recording.add("marker", f"connection lost: {type(e).__name__}", None, now())
        finally:
            with contextlib.suppress(CTraderError, OSError):
                await connection.close()
        # Events the drop cut off from their snapshot are announced now, so the count after the
        # reconnect holds only what came after it.
        announce_events()
        if finished():
            break
        status(STATUS_LOST)
        # Not a plain sleep: a stop typed during the wait takes effect at once.
        with contextlib.suppress(TimeoutError):
            wait = reconnect_waits[min(attempt, len(reconnect_waits) - 1)]
            await asyncio.wait_for(stop.wait(), wait)
        attempt += 1
    drain_markers()
    announce_deadline()
    recording.add("marker", "closing requests skipped: not connected", None, now())
    return recording


def raw_path(output: pathlib.Path, directory: pathlib.Path) -> pathlib.Path:
    """Where the unscrubbed recording behind the fixture `output` is kept."""
    return directory / f"{output.stem}.raw.json"


@dataclass
class Outcome:
    """What a run left behind. Filled in as it happens, so it can be read after the run raised."""

    # Set once the run has recorded at least one message; nothing is written before that.
    recording: Recording | None = None
    raw: pathlib.Path | None = None
    fixture: bool = False
    run_error: BaseException | None = None


async def record_to_file(
    output: pathlib.Path,
    *,
    login: int | None,
    secrets: Iterable[str],
    account_id: int,
    raw_dir: pathlib.Path = _RAW_DIR,
    outcome: Outcome | None = None,
    **kwargs,
) -> None:
    """Run `record()` and keep what it saw, whatever way the run ended.

    The owner's manual trades cannot be repeated on demand, so:

    - the recording is first written as it was seen to `raw_path(output, raw_dir)`, with no
      scrubbing and no check in the way. That file holds real identifiers: `raw_dir` must be a
      directory git ignores;
    - then the fixture is built, checked and written to `output`, even if the raw file could
      not be. If any of that fails, `output` is left as it was and the error is raised, from
      the run's own error if it had one; `rescrub()` builds the fixture from the raw file later;
    - a run that failed or was interrupted is kept the same way, and its error is re-raised;
    - a run that recorded no message at all writes nothing: there is nothing to keep, and a
      file would only stand in the way of the next run.

    Each file is replaced in one step, so a failed write never damages an earlier one.
    """
    recording = Recording(int(time.time() * 1000))
    outcome = Outcome() if outcome is None else outcome

    def keep() -> None:
        if not any(True for _ in recording.messages()):
            return
        outcome.recording = recording
        raw = raw_path(output, raw_dir)
        raw_failure: Exception | None = None
        try:
            raw_dir.mkdir(parents=True, exist_ok=True)
            _write_atomically(raw, encode_raw(recording, account_id=account_id, login=login))
            outcome.raw = raw
        except Exception as e:
            # The fixture is still worth having without its unscrubbed copy.
            raw_failure = e
        write_fixture(output, recording, account_id=account_id, login=login, secrets=secrets)
        outcome.fixture = True
        if raw_failure is not None:
            raise raw_failure

    try:
        await record(account_id=account_id, recording=recording, **kwargs)
    except BaseException as e:
        outcome.run_error = e
        last = recording.timeline[-1].t if recording.timeline else 0.0
        recording.add("marker", f"run failed: {type(e).__name__}", None, last)
        try:
            keep()
        except Exception as unkept:
            raise unkept from e
        raise
    keep()


def rescrub(
    raw_file: pathlib.Path,
    output: pathlib.Path,
    secrets: Iterable[str],
    outcome: Outcome | None = None,
) -> None:
    """Build the fixture again from a raw recording; no connection is involved."""
    outcome = Outcome() if outcome is None else outcome
    recording, account_id, login = decode_raw(raw_file.read_bytes())
    outcome.recording = recording
    write_fixture(output, recording, account_id=account_id, login=login, secrets=secrets)
    outcome.fixture = True


class NoSuchAccount(Refused):
    """The access token does not grant exactly one account with the trader login given."""


def _match_account(accounts: Iterable[Message], trader_login: int) -> Message:
    matched = [a for a in accounts if a.HasField("traderLogin") and a.traderLogin == trader_login]
    if not matched:
        raise NoSuchAccount("the access token grants no account with that trader login")
    if len(matched) > 1:
        raise NoSuchAccount("more than one granted account has that trader login")
    return matched[0]


async def _resolve_account(
    trader_login: int,
    client_id: str,
    client_secret: str,
    access_token: str,
) -> tuple[str, int]:
    """The host the account lives on and the account id every request carries."""
    connection = CTraderConnection(DEMO_HOST, PROTOBUF_PORT, logger=record_fixtures._QuietLogger())
    await connection.connect()
    try:
        await send(
            connection,
            oa.ProtoOAApplicationAuthReq(clientId=client_id, clientSecret=client_secret),
        )
        listed = await send(
            connection,
            oa.ProtoOAGetAccountListByAccessTokenReq(accessToken=access_token),
        )
    finally:
        await connection.close()
    account = _match_account(listed.ctidTraderAccount, trader_login)
    return (LIVE_HOST if account.isLive else DEMO_HOST), account.ctidTraderAccountId


def _check_target(path: pathlib.Path, *, overwrite: bool, create_dir: bool = False) -> None:
    """Refuse now what would otherwise fail, or silently replace a file, after the session."""
    if path.exists() and not overwrite:
        raise Refused(f"{path} already exists; pass --overwrite to replace it")
    try:
        if create_dir:
            path.parent.mkdir(parents=True, exist_ok=True)
        # The only test of a directory that holds on every platform: write into it.
        with tempfile.TemporaryFile(dir=path.parent):
            pass
    except OSError:
        raise Refused(f"cannot write a file in {path.parent}") from None


def _read_keyboard(
    loop: asyncio.AbstractEventLoop,
    stop: asyncio.Event,
    markers: asyncio.Queue[str],
) -> None:
    """A typed line becomes a marker; `q` stops the recording. Runs on its own thread."""
    # An unreadable stdin, or a loop already closed: there is nobody left to tell.
    with contextlib.suppress(OSError, ValueError, RuntimeError):
        while True:
            line = sys.stdin.readline()
            if not line:
                # End of input, where every further read returns at once: stop reading.
                return
            line = line.strip()
            if line.lower() == "q":
                loop.call_soon_threadsafe(stop.set)
                return
            if line:
                loop.call_soon_threadsafe(markers.put_nowait, line)


def _start_keyboard(
    loop: asyncio.AbstractEventLoop,
    stop: asyncio.Event,
    markers: asyncio.Queue[str],
) -> None:
    # A daemon thread: a read still blocked on stdin must not keep the process alive once the
    # run is over.
    threading.Thread(target=_read_keyboard, args=(loop, stop, markers), daemon=True).start()


_CHECKLIST = """\
Recording. Trade by hand in the cTrader terminal, at the smallest volume, and after each
step type what you did and press Enter:
  1. a market buy with a stop-loss and a take-profit
  2. move the stop-loss
  3. remove the take-profit, then add it back
  4. partially close the position
  5. let the stop-loss trigger, or close the position by hand
  6. a second position, closed by its take-profit
  7. place a pending limit order, then cancel it
  8. (optional, long) keep a position open across the daily rollover
A line "events: ..." follows each step the venue reported; if none appears, nothing is being
recorded. Type q and press Enter to stop."""

# Each one is needed to connect, and every one present must stay out of a fixture.
_REQUIRED_KEYS = ("CTRADER_CLIENT_ID", "CTRADER_CLIENT_SECRET", "CTRADER_ACCESS_TOKEN")
_SECRET_KEYS = (*_REQUIRED_KEYS, "CTRADER_REFRESH_TOKEN")


def _credentials(env: dict[str, str]) -> tuple[str, str, str]:
    """The client id, client secret and access token; `Refused` if the env file lacks one."""
    missing = [key for key in _REQUIRED_KEYS if not env.get(key)]
    if missing:
        raise Refused(f"the env file lacks {', '.join(missing)}")
    client_id, client_secret, access_token = (env[key] for key in _REQUIRED_KEYS)
    return client_id, client_secret, access_token


def _secrets(env: dict[str, str]) -> tuple[str, ...]:
    """What the clean check must never find in a fixture."""
    return tuple(env.get(key, "") for key in _SECRET_KEYS)


async def _run(args: argparse.Namespace, env: dict[str, str], outcome: Outcome) -> None:
    client_id, client_secret, access_token = _credentials(env)
    print("Connecting...")
    host, account_id = await _resolve_account(
        args.trader_login,
        client_id,
        client_secret,
        access_token,
    )
    stop: asyncio.Event = asyncio.Event()
    markers: asyncio.Queue[str] = asyncio.Queue()

    def on_status(text: str) -> None:
        # The checklist only once events are really being recorded.
        if text == STATUS_STARTED:
            print(_CHECKLIST)
        else:
            print(text, file=sys.stderr)

    _start_keyboard(asyncio.get_running_loop(), stop, markers)
    await record_to_file(
        args.output,
        login=args.trader_login,
        secrets=_secrets(env),
        raw_dir=args.raw_dir,
        outcome=outcome,
        host=host,
        port=PROTOBUF_PORT,
        tls=True,
        client_id=client_id,
        client_secret=client_secret,
        access_token=access_token,
        account_id=account_id,
        minutes=args.minutes,
        stop=stop,
        markers=markers,
        status=on_status,
    )


def _failure(error: BaseException) -> str:
    """The error's type and, for a venue refusal, the venue's error code.

    Never its message: a venue's description, or any other text, could quote an identifier.
    """
    code = f" {error.error_code}" if isinstance(error, CTraderRequestError) else ""
    return f"{type(error).__name__}{code}"


def _outcome_lines(outcome: Outcome, output: pathlib.Path, *, rescrub: bool = False) -> list[str]:
    """What is on disk once `main` is done, and what was recorded: the counts come last.

    `rescrub` leaves the unscrubbed copy out: that mode reads it and never writes it.
    """
    if outcome.recording is None:
        if rescrub:
            return ["The fixture was NOT written."]
        return ["Nothing was recorded, and nothing was written."]
    if outcome.fixture:
        lines = [f"Recording written to {output}"]
    else:
        lines = [f"The fixture was NOT written; {output} is untouched."]
    if not rescrub and outcome.raw is None:
        lines.append("The unscrubbed copy could NOT be written.")
    elif not rescrub:
        lines.append(f"Unscrubbed copy, never to be committed: {outcome.raw}")
        if not outcome.fixture:
            lines.append(f"Rebuild the fixture from it with: --rescrub {outcome.raw}")
    return [*lines, *summary(outcome.recording)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--describe", type=pathlib.Path, metavar="FILE")
    parser.add_argument(
        "--rescrub",
        type=pathlib.Path,
        metavar="RAW_FILE",
        help="build the fixture again from an unscrubbed recording, offline",
    )
    parser.add_argument("--trader-login", type=int)
    parser.add_argument("--minutes", type=float, default=120.0)
    parser.add_argument("--output", type=pathlib.Path, default=_OUTPUT_PATH)
    parser.add_argument(
        "--raw-dir",
        type=pathlib.Path,
        default=_RAW_DIR,
        help="where the unscrubbed recording is kept; must be ignored by git",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="replace an output or raw file that already exists",
    )
    parser.add_argument("--env-file", type=pathlib.Path, default=_REPO_ROOT / ".env")
    args = parser.parse_args(argv)

    if args.describe is not None:
        try:
            print(describe(args.describe.read_bytes()))
        except Refused as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        return 0
    if args.rescrub is None and args.trader_login is None:
        parser.error("--trader-login is required to record")

    outcome = Outcome()
    code = 0
    try:
        # All of this before anything is sent or written: a session must not end on a missing
        # key or on a file that cannot be written.
        env = get_tokens.load_env(args.env_file)
        _credentials(env)
        _check_target(args.output, overwrite=args.overwrite)
        if args.rescrub is not None:
            rescrub(args.rescrub, args.output, _secrets(env), outcome)
        else:
            raw = raw_path(args.output, args.raw_dir)
            _check_target(raw, overwrite=args.overwrite, create_dir=True)
            asyncio.run(_run(args, env, outcome))
    except KeyboardInterrupt:
        # Stopped by hand. Whether anything was written depends on when: the lines below say.
        code = 130
    except Refused as e:
        print(f"error: {e}", file=sys.stderr)
        code = 1
    except Exception as e:
        print(f"error: {_failure(e)}", file=sys.stderr)
        if outcome.run_error is not None and outcome.run_error is not e:
            print(f"the run itself had failed: {_failure(outcome.run_error)}", file=sys.stderr)
        code = 1
    for line in _outcome_lines(outcome, args.output, rescrub=args.rescrub is not None):
        print(line)
    return code


if __name__ == "__main__":
    sys.exit(main())
