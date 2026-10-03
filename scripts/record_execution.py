"""Record a cTrader account's execution events while its owner trades by hand.

Read-only. The script listens for the events the venue pushes, asks for snapshots of the
account's positions and orders, and writes a scrubbed fixture. Every message it sends goes
through `send()`, which refuses any request class outside `READ_ONLY_REQUESTS`; no request that
places, changes or closes an order is named anywhere in this module.

    uv run python scripts/record_execution.py --trader-login <login>

While it runs, type a short note and press Enter to mark what you just did in the terminal
("moved the stop"); type `q` and Enter to stop. `--describe` prints a recording back.

The fixture never holds the account id, the trader login, a token, a balance, the broker's
name, or the broker's own order, position and deal ids: those are replaced consistently, so a
position still lines up with its orders and deals. A wall time in milliseconds or seconds is
shifted by one constant, which keeps every interval; a bar's time in minutes keeps its value.

A session is kept however it ends. What was seen is first written unscrubbed to
`tests/recordings/`, which git ignores, and only then scrubbed, checked and written as the
fixture. If that second step fails the fixture is left alone, and the first file rebuilds it
later, with no connection:

    uv run python scripts/record_execution.py --rescrub tests/recordings/<name>.raw.json

The first Ctrl+C, like `q`, ends the run and writes both files. If the process ends before it
can - a second Ctrl+C, a crash - what was seen is still in the journal it appended to as it
went, `tests/recordings/<name>.raw.jsonl`; the next run with the same output finishes that
recording before anything else, and only a later run records a new session.

The unscrubbed files are never printed, by `--describe` or otherwise. Neither file is replaced
without `--overwrite`, which never discards a journal, and a run that recorded nothing writes
nothing. A refusal by the venue ends the run at once and is reported by its error code.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import datetime
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
# Stands in for the broker's name in free text.
BROKER_PLACEHOLDER = "<broker>"
_MIN_NAME_CHARS = 3
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
STATUS_CLOSING_LISTS = "stopping: asking the closing lists"
STATUS_WRITING = "stopping: writing the recording"
STATUS_JOURNAL_FAILED = "the journal could not be written: an abrupt end would now lose the session"

# The recorder's own markers about a recording finished from its journal.
JOURNAL_FINISHED = "recording finished after an interruption: closing lists asked on a later run"
JOURNAL_TORN = "recording journal ended in a partly written line, which was dropped"
JOURNAL_STOPPED = "recording journal stopped here: an abrupt end after this would lose the rest"
# What `main` prints about a journal: fixed texts, followed at most by its path.
JOURNAL_FOUND = "An interrupted recording was found and is finished first, from its journal"
JOURNAL_DISCARD_HINT = (
    "To discard it instead, move that journal away by hand; --overwrite does not discard it."
)
JOURNAL_KEPT = "The journal is kept, and the next run with this output finishes it"
JOURNAL_UNREADABLE = (
    "the journal of an interrupted recording cannot be read, so it cannot be finished automatically"
)
JOURNAL_UNREADABLE_KEPT = "The journal is kept; move it away by hand to record again"
JOURNAL_OTHER_ACCOUNT = (
    "the interrupted recording is of another account: finish it with that account's trader "
    "login, or move its journal away by hand"
)

# How the recorder's own markers begin when the owner has to read them before trusting the
# recording: a list it lacks, in whole or in part, or a recording finished from its journal.
_PROBLEM_MARKERS = ("closing ", "recording ")
# The whole of a fixture's shape. A file with any other key is not one; an unscrubbed
# recording carries more at both levels.
_FIXTURE_KEYS = frozenset({"format", "timeline", "closing"})
_FIXTURE_ENTRY_KEYS = frozenset({"t", "kind", "note", "type", "payload"})
NOT_A_FIXTURE = (
    "not a scrubbed fixture, so nothing of it is printed; "
    "an unscrubbed recording becomes a fixture with --rescrub"
)

_ID_KINDS = {"positionId": "position", "orderId": "order", "dealId": "deal"}
_ID_BASES = {"position": 5_000_000, "order": 6_000_000, "deal": 7_000_000}
# Free text the account's owner, or a robot of theirs, may have written.
_TEXT_FIELDS = frozenset({"label", "comment", "clientOrderId", "externalNote"})
# The venue's own wording: kept as evidence, with the numbers that identify taken out.
_VENUE_TEXT_FIELDS = frozenset({"description", "reason"})
# Money moved in or out of the account: nothing the order logic needs.
_CLEARED_MESSAGES = frozenset({"depositWithdraw", "bonusDepositWithdraw"})
# Lists of account ids. They name the owner's other accounts, whose ids no check knows.
_ACCOUNT_ID_LISTS = frozenset({"ctidTraderAccountIds"})
# The owner's id at the identity provider. Zeroed, not cleared: the field is `required`.
_ZEROED_IDS = frozenset({"userId"})
_INT_TYPES = (FieldDescriptor.TYPE_INT64, FieldDescriptor.TYPE_UINT64)
# Wall times the schema gives in seconds, by message and field: the same name elsewhere is in
# milliseconds.
_SECONDS_TIMESTAMPS = frozenset({("ProtoOAErrorRes", "maintenanceEndTimestamp")})
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


def _names_pattern(names: Iterable[str]) -> re.Pattern[str] | None:
    """What matches any of `names` as whole words, in any case; `None` if there is no name.

    Whole words only: a short name would otherwise match inside ordinary words. The longest
    name is tried first, so a name inside a longer one does not leave the rest behind.
    """
    ordered = sorted({name for name in names if name}, key=len, reverse=True)
    if not ordered:
        return None
    alternatives = "|".join(re.escape(name) for name in ordered)
    return re.compile(rf"(?<!\w)(?:{alternatives})(?!\w)", re.IGNORECASE)


def clean_text(
    text: str,
    *,
    account_id: int,
    login: int | None,
    ids: IdMap,
    names: Iterable[str] = (),
) -> str:
    """Free text with the names and numbers that could identify the account taken out.

    Numbers are matched as whole runs of digits: the digits of an id inside a longer number are
    not that id.

    - each of `names`, the broker's, becomes `BROKER_PLACEHOLDER`, as a whole word in any case;
    - a known order, position or deal id becomes its fake id;
    - the account id and the trader login become the fake values their own fields get;
    - any other run of `_LONG_NUMBER_DIGITS` digits or more becomes `NUMBER_PLACEHOLDER`: an id
      quoted only in text was never seen in a field, so it cannot be mapped. A run that is the
      integer or the fractional part of a decimal number is a price or a rate, and is kept.
    """
    pattern = _names_pattern(names)
    if pattern is not None:
        text = pattern.sub(BROKER_PLACEHOLDER, text)
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
    names: Iterable[str] = (),
) -> Message:
    """`record_fixtures.scrub()`, then what execution messages add to it."""
    result = record_fixtures.scrub(message, account_id, login)
    # A field newer than these bindings is scrubbed by nothing here, and may hold anything.
    result.DiscardUnknownFields()

    def clean(text: str) -> str:
        return clean_text(text, account_id=account_id, login=login, ids=ids, names=names)

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
            if name in _ACCOUNT_ID_LISTS:
                # Cut to the one fake id: how many accounts the owner has is theirs to tell.
                message.ClearField(name)
                getattr(message, name).append(record_fixtures.FAKE_ACCOUNT_ID)
        elif name in _ZEROED_IDS and descriptor.type in _INT_TYPES:
            setattr(message, name, 0)
        elif name in _ID_KINDS and descriptor.type in _INT_TYPES:
            # Zero or less stands for "no id": mapping it would replace every 0 in a text.
            if value > 0:
                setattr(message, name, ids.fake(_ID_KINDS[name], value))
        elif descriptor.type == FieldDescriptor.TYPE_STRING and name in _TEXT_FIELDS:
            setattr(message, name, SCRUBBED_TEXT)
        elif descriptor.type == FieldDescriptor.TYPE_STRING and name in _VENUE_TEXT_FIELDS:
            setattr(message, name, clean(value))
        elif descriptor.type in _INT_TYPES and (name == "timestamp" or name.endswith("Timestamp")):
            in_seconds = (message.DESCRIPTOR.name, name) in _SECONDS_TIMESTAMPS
            shift = shift_ms // 1000 if in_seconds else shift_ms
            # Below the shift it is not a wall time in its unit, and an unsigned field refuses
            # the negative result.
            if value > max(shift, 0):
                setattr(message, name, value - shift)


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
    # The broker's names, as the venue gave them: what scrubbing takes out of free text.
    names: list[str] = field(default_factory=list)
    # Where each entry and name is also written as it is added, if anywhere.
    journal: Journal | None = field(default=None, repr=False, compare=False)

    def add_name(self, name: str) -> None:
        name = name.strip()
        # A shorter one would take letters out of ordinary words ("a", "of").
        if len(name) >= _MIN_NAME_CHARS and name not in self.names:
            self.names.append(name)
            if self.journal is not None:
                self.journal.add_name(name)

    def add(
        self,
        kind: str,
        note: str,
        message: Message | None,
        t: float,
        *,
        typed: bool = False,
    ) -> None:
        entry = Entry(round(t, 3), kind, note, message, typed)
        self.timeline.append(entry)
        if self.journal is not None:
            self.journal.add(self, entry)

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
            names=recording.names,
        )

    # A first pass only to fill the id map, so a text quoting an id is fixed even where the id
    # itself first appears later in the recording.
    for message in recording.messages():
        clean(message)

    timeline = []
    for entry in recording.timeline:
        note = clean_text(
            entry.note,
            account_id=account_id,
            login=login,
            ids=ids,
            names=recording.names,
        )
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
    output = {
        "format": FORMAT,
        "started_wall_ms": recording.started_wall_ms,
        "account_id": account_id,
        "login": login,
        "names": recording.names,
        "timeline": [_raw_entry(entry) for entry in recording.timeline],
        "closing": {
            key: [_encode(message, partial=True) for message in items]
            for key, items in recording.closing.items()
        },
    }
    return json.dumps(output, indent=2).encode("utf-8")


def decode_raw(data: bytes) -> tuple[Recording, int, int | None]:
    """The recording, account id and trader login that `encode_raw()` wrote.

    A marker with no `typed` flag is read as typed: only the recorder's own markers are ever
    quoted on the terminal, so that is the reading under which a note cannot be.
    """
    raw = json.loads(data)
    # A file written before names were kept has none.
    recording = Recording(raw["started_wall_ms"], names=list(raw.get("names", [])))
    recording.timeline = [_entry_from_raw(item) for item in raw["timeline"]]
    recording.closing = {
        key: [_decode(item) for item in items] for key, items in raw["closing"].items()
    }
    return recording, raw["account_id"], raw["login"]


def _raw_entry(entry: Entry) -> dict:
    """A timeline entry as the raw file and the journal hold it: unscrubbed."""
    item: dict = {"t": entry.t, "kind": entry.kind, "note": entry.note}
    if entry.kind == "marker":
        # Written for the recorder's own markers too: see `decode_raw()`.
        item["typed"] = entry.typed
    if entry.message is not None:
        item.update(_encode(entry.message, partial=True))
    return item


def _entry_from_raw(item: dict) -> Entry:
    return Entry(
        item["t"],
        item["kind"],
        item["note"],
        _decode(item) if "payload" in item else None,
        item.get("typed", item["kind"] == "marker"),
    )


def journal_path(output: pathlib.Path, directory: pathlib.Path) -> pathlib.Path:
    """Where the journal of a recording that will become the fixture `output` is kept."""
    return directory / f"{output.stem}.raw.jsonl"


class Journal:
    """A recording appended to a file entry by entry, as it is made, for an abrupt end.

    Unscrubbed, like the raw file. One JSON object per line: a header first, with what a
    rebuild needs, then each timeline entry in the raw file's form, and a `name` line for a
    broker name learned after the header. Every line is flushed to disk as it is written.

    The file is created at the first entry, so a run that records nothing leaves none, and
    never over an existing one. A failed write stops the journal, not the run: `status` is told
    once, and a marker in the recording says from where on the journal holds nothing.
    """

    def __init__(
        self,
        path: pathlib.Path,
        *,
        account_id: int,
        login: int | None,
        status: Callable[[str], None],
    ) -> None:
        self.path = path
        self._account_id = account_id
        self._login = login
        self._status = status
        self._handle = None
        self._failed = False
        self._closed = False
        self._recording: Recording | None = None
        # Set once this object has created the file: only such a file is ever removed.
        self.created = False

    def add(self, recording: Recording, entry: Entry) -> None:
        self._recording = recording
        items = [_raw_entry(entry)]
        if not self.created:
            header = {
                "format": FORMAT,
                "started_wall_ms": recording.started_wall_ms,
                "account_id": self._account_id,
                "login": self._login,
                "names": recording.names,
            }
            items.insert(0, header)
        self._append(items)

    def add_name(self, name: str) -> None:
        # Before the header is written, the header itself carries it.
        if self.created:
            self._append([{"name": name}])

    def _append(self, items: list[dict]) -> None:
        if self._failed or self._closed:
            return
        try:
            if self._handle is None:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                # Held open across writes, so not a `with`.
                self._handle = open(self.path, "xb")  # noqa: SIM115
                self.created = True
            self._handle.write(b"".join(json.dumps(item).encode() + b"\n" for item in items))
            self._handle.flush()
            os.fsync(self._handle.fileno())
        except OSError:
            self._failed = True
            self.close()
            self._status(STATUS_JOURNAL_FAILED)
            recording = self._recording
            if recording is not None:
                # Added after `_failed` is set, so it does not come back here.
                last = recording.timeline[-1].t if recording.timeline else 0.0
                recording.add("marker", JOURNAL_STOPPED, None, last)

    def close(self) -> None:
        """Stop the journal: what is added after this is not written."""
        self._closed = True
        if self._handle is not None:
            with contextlib.suppress(OSError):
                self._handle.close()
            self._handle = None

    def remove(self) -> None:
        self.close()
        if self.created:
            self.path.unlink(missing_ok=True)
            self.created = False


def read_journal(data: bytes) -> tuple[Recording, int, int | None]:
    """The recording, account id and trader login a journal holds.

    A last line that is not whole JSON - the process ended while writing it - is dropped, and a
    marker says so. Anything else that does not read raises.
    """
    lines = data.split(b"\n")
    if lines and not lines[-1]:
        lines.pop()
    header = json.loads(lines[0])
    if header["format"] != FORMAT:
        raise ValueError("a journal of another format")
    recording = Recording(header["started_wall_ms"], names=list(header["names"]))
    torn = False
    for index, line in enumerate(lines[1:], start=1):
        try:
            item = json.loads(line)
        except ValueError:
            if index < len(lines) - 1:
                raise
            torn = True
            break
        if "name" in item:
            recording.add_name(item["name"])
        else:
            recording.timeline.append(_entry_from_raw(item))
    if torn:
        last = recording.timeline[-1].t if recording.timeline else 0.0
        recording.add("marker", JOURNAL_TORN, None, last)
    return recording, header["account_id"], header["login"]


def check_clean(
    data: bytes,
    recording: Recording,
    *,
    account_id: int,
    login: int | None,
    ids: IdMap,
    secrets: Iterable[str],
) -> None:
    """Raise `ScrubError` if a real identifier, secret or name survives in what is to be written.

    Checked twice: each scrubbed message's own serialized bytes, where an int64 is a varint and
    no decimal search can see it, and the final JSON, where a text field or a note could hold it.

    The broker's names are looked for in every note and every text field instead, the way
    `clean_text()` matches them: in the JSON or the bytes, a short name would turn up by chance
    inside the base64 of a payload.
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

    names = _names_pattern(recording.names)
    if names is None:
        return
    texts = [item["note"] for item in decoded["timeline"]]
    for message in messages:
        texts.extend(_strings(message))
    # The token itself may read as a name: "<broker>" for a broker called "Broker".
    if any(names.search(t.replace(BROKER_PLACEHOLDER, " ")) for t in texts):
        raise record_fixtures.ScrubError("a broker name was found in recorded fixture data")


def _strings(message: Message) -> Iterable[str]:
    """Every text field of `message`, at any depth."""
    for descriptor, value in message.ListFields():
        items = value if descriptor.label == FieldDescriptor.LABEL_REPEATED else (value,)
        if descriptor.type == FieldDescriptor.TYPE_MESSAGE:
            for item in items:
                yield from _strings(item)
        elif descriptor.type == FieldDescriptor.TYPE_STRING:
            yield from items


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
            # On disk before it replaces anything: the journal is removed once this returns.
            handle.flush()
            os.fsync(handle.fileno())
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


def _holds_a_real_account_id(message: Message) -> bool:
    """Whether any account id in `message` is other than the fake one scrubbing leaves."""
    fake = record_fixtures.FAKE_ACCOUNT_ID
    for descriptor, value in message.ListFields():
        items = value if descriptor.label == FieldDescriptor.LABEL_REPEATED else (value,)
        if descriptor.type == FieldDescriptor.TYPE_MESSAGE:
            real = any(_holds_a_real_account_id(item) for item in items)
        elif descriptor.name == "ctidTraderAccountId" or descriptor.name in _ACCOUNT_ID_LISTS:
            real = any(item != fake for item in items)
        else:
            real = False
        if real:
            return True
    return False


def describe(data: bytes) -> str:
    """One line per timeline entry of a fixture.

    Raises `Refused(NOT_A_FIXTURE)`, which quotes nothing of `data`, unless it is a scrubbed
    fixture: exactly the fixture's keys and format, every payload decodable, and no account
    id in it but the fake one. Anything else may be an unscrubbed recording, and is not printed.
    """
    try:
        raw = json.loads(data)
        entries = [*raw["timeline"], *(item for items in raw["closing"].values() for item in items)]
        shaped = (
            raw.keys() == _FIXTURE_KEYS
            and raw["format"] == FORMAT
            and all(entry.keys() <= _FIXTURE_ENTRY_KEYS for entry in entries)
        )
        timeline = decode_recording(data)["timeline"] if shaped else []
        messages = [_decode(entry) for entry in entries if shaped and "payload" in entry]
        if not shaped or any(_holds_a_real_account_id(message) for message in messages):
            raise Refused(NOT_A_FIXTURE)
        lines = []
        for item in timeline:
            head = f"{item['t']:9.3f}  {item['kind']:<8}"
            message = item["message"]
            if message is None:
                lines.append(f"{head}  {item['note']}")
                continue
            body = text_format.MessageToString(message, as_one_line=True)
            note = f"[{item['note']}] " if item["note"] else ""
            lines.append(f"{head}  {note}{type(message).__name__} {body}")
    except Refused:
        raise
    except Exception:
        # Truncated, not JSON, not this shape: the error's own text could quote the file.
        raise Refused(NOT_A_FIXTURE) from None
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


async def _ask_closing_lists(
    connection: CTraderConnection,
    recording: Recording,
    account_id: int,
    now: Callable[[], float],
) -> None:
    """Ask the lists that hold what the recording's session did, into `recording.closing`.

    A list refused is marked and the rest are asked. A list that fails otherwise - no answer,
    no connection - ends the asking: each list left is marked as skipped.
    """
    failed = False

    async def ask(key: str, request: Message) -> None:
        nonlocal failed
        if failed:
            recording.add("marker", f"closing list skipped: {key}", None, now())
            return
        try:
            response = await send(connection, request, bucket=BUCKET_HISTORICAL)
        except CTraderRequestError as e:
            # One list refused says nothing about the next: note it and ask the rest.
            note = f"closing list refused: {key} {e.error_code}"
            recording.add("marker", note, None, now())
            return
        except CTraderError as e:
            # No answer, or no connection: every list after it would only wait as long, so
            # each is marked as skipped instead.
            failed = True
            note = f"closing list failed: {key} {type(e).__name__}"
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
      `snapshot refused` marker. However the run ends, `STATUS_WRITING` comes once at the end
      if anything was recorded; `STATUS_CLOSING_LISTS` comes before it only when the closing
      lists are asked.

    The broker's name, from the trader asked for once on the first connection, is added to the
    recording's names; that response itself is not recorded.

    Raises `CTraderRequestError` if the venue refuses authentication, the trader, or the
    snapshot taken at the start or right after a reconnect: a refusal is not a lost connection,
    and asking again changes nothing. A snapshot refused later, after events, is only marked,
    and the run goes on.
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

    def announce_deadline() -> None:
        if not stop.is_set():
            status(STATUS_DEADLINE)

    writing_announced = False

    def announce_writing() -> None:
        nonlocal writing_announced
        # A run that recorded nothing has nothing to write.
        if not writing_announced and any(True for _ in recording.messages()):
            writing_announced = True
            status(STATUS_WRITING)

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
    try:
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
                if first:
                    trader = await send(
                        connection,
                        oa.ProtoOATraderReq(ctidTraderAccountId=account_id),
                    )
                    recording.add_name(trader.trader.brokerName)
                else:
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
                    status(STATUS_CLOSING_LISTS)
                    await _ask_closing_lists(connection, recording, account_id, now)
                    announce_writing()
                    return recording
                recording.add("marker", "connection lost", None, now())
            except CTraderRequestError:
                # The venue answered, and refused: asking again on a new connection changes nothing.
                announce_writing()
                raise
            except (CTraderError, OSError) as e:
                recording.add("marker", f"connection lost: {type(e).__name__}", None, now())
            except BaseException:
                # A cancellation (the first Ctrl+C) or a bug: told before the close, which can
                # take a while.
                announce_writing()
                raise
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
    except BaseException:
        # Whatever ends the run outside a connection: a cancellation during a reconnect
        # wait, or during the close of a lost connection.
        announce_writing()
        raise
    drain_markers()
    announce_deadline()
    announce_writing()
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
    # A journal left on disk, which the next run with the same output finishes if it can be
    # read.
    journal: pathlib.Path | None = None
    journal_readable: bool = True


def _write_recording(
    recording: Recording,
    output: pathlib.Path,
    raw_dir: pathlib.Path,
    outcome: Outcome,
    *,
    account_id: int,
    login: int | None,
    secrets: Iterable[str],
) -> None:
    """The raw file, then the fixture, as `record_to_file()` describes; nothing if no message."""
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


def _journal_done(recording: Recording, outcome: Outcome) -> bool:
    """Whether a journal of `recording` holds nothing that is not kept elsewhere by now."""
    # The raw file holds all the journal does, and the closing lists too.
    return outcome.raw is not None or not any(True for _ in recording.messages())


async def record_to_file(
    output: pathlib.Path,
    *,
    login: int | None,
    secrets: Iterable[str],
    account_id: int,
    raw_dir: pathlib.Path = _RAW_DIR,
    outcome: Outcome | None = None,
    names: Iterable[str] = (),
    **kwargs,
) -> None:
    """Run `record()` and keep what it saw, whatever way the run ended.

    `names` are the broker's names known before the run; `record()` adds the one it learns.

    The owner's manual trades cannot be repeated on demand, so:

    - while the run goes on, each entry is appended to a `Journal` at
      `journal_path(output, raw_dir)`: if the process ends before anything below can happen,
      the next run with the same output finishes the recording from it (`finish()`);
    - the recording is first written as it was seen to `raw_path(output, raw_dir)`, with no
      scrubbing and no check in the way. That file holds real identifiers: `raw_dir` must be a
      directory git ignores;
    - then the fixture is built, checked and written to `output`, even if the raw file could
      not be. If any of that fails, `output` is left as it was and the error is raised, from
      the run's own error if it had one; `rescrub()` builds the fixture from the raw file later;
    - a run that failed or was interrupted is kept the same way, and its error is re-raised;
    - a run that recorded no message at all writes nothing: there is nothing to keep, and a
      file would only stand in the way of the next run;
    - the journal is removed once the raw file is written, or if there was nothing to keep.

    Each file is replaced in one step, so a failed write never damages an earlier one.
    """
    recording = Recording(int(time.time() * 1000))
    for name in names:
        recording.add_name(name)
    outcome = Outcome() if outcome is None else outcome
    journal = Journal(
        journal_path(output, raw_dir),
        account_id=account_id,
        login=login,
        status=kwargs.get("status", _silent),
    )
    recording.journal = journal

    def keep() -> None:
        journal.close()
        try:
            _write_recording(
                recording,
                output,
                raw_dir,
                outcome,
                account_id=account_id,
                login=login,
                secrets=secrets,
            )
        finally:
            if _journal_done(recording, outcome):
                journal.remove()
            elif journal.created:
                outcome.journal = journal.path

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


async def finish(
    journal_file: pathlib.Path,
    recording: Recording,
    output: pathlib.Path,
    *,
    host: str,
    port: int,
    tls: bool,
    client_id: str,
    client_secret: str,
    access_token: str,
    account_id: int,
    login: int | None,
    secrets: Iterable[str],
    raw_dir: pathlib.Path = _RAW_DIR,
    outcome: Outcome | None = None,
    rate_limiter: RateLimiter | None = None,
    status: Callable[[str], None] = _silent,
) -> None:
    """Finish a recording an abrupt end left in `journal_file`, read as `recording`.

    Asks the closing lists, as a run's own end would have, for the window from the recording's
    start and for the positions it saw; then writes the raw file and the fixture as
    `record_to_file()` does, and removes the journal once the raw file is written. If anything
    on the way fails, the journal is left as it was and the error is raised. `status` is told
    what a run's own end tells it: `STATUS_CLOSING_LISTS`, then `STATUS_WRITING`.
    """
    outcome = Outcome() if outcome is None else outcome
    outcome.journal = journal_file
    last = recording.timeline[-1].t if recording.timeline else 0.0
    connection = CTraderConnection(
        host,
        port,
        logger=record_fixtures._QuietLogger(),
        tls=tls,
        rate_limiter=RateLimiter(_RATE_LIMITS) if rate_limiter is None else rate_limiter,
    )
    await connection.connect()
    try:
        await send(
            connection,
            oa.ProtoOAApplicationAuthReq(clientId=client_id, clientSecret=client_secret),
        )
        await send(
            connection,
            oa.ProtoOAAccountAuthReq(ctidTraderAccountId=account_id, accessToken=access_token),
        )
        recording.add("marker", JOURNAL_FINISHED, None, last)
        status(STATUS_CLOSING_LISTS)
        await _ask_closing_lists(connection, recording, account_id, lambda: last)
    finally:
        with contextlib.suppress(CTraderError, OSError):
            await connection.close()
    if any(True for _ in recording.messages()):
        status(STATUS_WRITING)
    _write_finished(
        journal_file,
        recording,
        output,
        raw_dir,
        outcome,
        account_id=account_id,
        login=login,
        secrets=secrets,
    )


def _write_finished(
    journal_file: pathlib.Path,
    recording: Recording,
    output: pathlib.Path,
    raw_dir: pathlib.Path,
    outcome: Outcome,
    *,
    account_id: int,
    login: int | None,
    secrets: Iterable[str],
) -> None:
    try:
        _write_recording(
            recording,
            output,
            raw_dir,
            outcome,
            account_id=account_id,
            login=login,
            secrets=secrets,
        )
    finally:
        if _journal_done(recording, outcome):
            journal_file.unlink(missing_ok=True)
            outcome.journal = None


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
) -> tuple[str, int, str]:
    """The account's host and id, and its broker's short title (empty if the list gave none)."""
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
    host = LIVE_HOST if account.isLive else DEMO_HOST
    return host, account.ctidTraderAccountId, account.brokerTitleShort


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
Recording until {ends} local time (--minutes {minutes}), when it stops by itself.
Trade by hand in the cTrader terminal, at the smallest volume, and after each step type what
you did and press Enter.
Notes are published with the fixture: write no names and no account numbers.
  1. a market buy with a stop-loss and a take-profit
  2. move the stop-loss
  3. remove the take-profit, then add it back
  4. partially close the position
  5. let the stop-loss trigger, or close the position by hand
  6. a second position, closed by its take-profit
  7. place a pending limit order, then cancel it
  8. (optional, long) keep a position open across the daily rollover: needs a --minutes
     that reaches past it
A line "events: ..." follows each step the venue reported; if none appears, nothing is being
recorded. Type q and press Enter to stop."""


def checklist(minutes: float, ends: datetime.datetime) -> str:
    """What the owner reads once the recording has started; `ends` is the local deadline."""
    return _CHECKLIST.format(ends=ends.strftime("%Y-%m-%d %H:%M"), minutes=f"{minutes:g}")


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
    host, account_id, broker_title = await _resolve_account(
        args.trader_login,
        client_id,
        client_secret,
        access_token,
    )
    stop: asyncio.Event = asyncio.Event()
    markers: asyncio.Queue[str] = asyncio.Queue()
    # `record()` counts its deadline from its own start, a moment from now.
    ends = datetime.datetime.now() + datetime.timedelta(minutes=args.minutes)

    def on_status(text: str) -> None:
        # The checklist only once events are really being recorded.
        if text == STATUS_STARTED:
            print(checklist(args.minutes, ends))
        else:
            print(text, file=sys.stderr)

    _start_keyboard(asyncio.get_running_loop(), stop, markers)
    await record_to_file(
        args.output,
        login=args.trader_login,
        secrets=_secrets(env),
        raw_dir=args.raw_dir,
        outcome=outcome,
        names=(broker_title,),
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


class OtherAccount(Refused):
    """A journal found for the output belongs to an account other than the one asked for."""


async def _finish_run(
    args: argparse.Namespace,
    env: dict[str, str],
    outcome: Outcome,
    journal_file: pathlib.Path,
    journaled: tuple[Recording, int, int | None],
) -> None:
    """Finish the recording `journal_file` holds, read as `journaled` by `read_journal()`."""
    client_id, client_secret, access_token = _credentials(env)
    recording, account_id, login = journaled
    if login != args.trader_login:
        raise OtherAccount(JOURNAL_OTHER_ACCOUNT)
    print("Connecting...")
    host, granted_id, broker_title = await _resolve_account(
        login,
        client_id,
        client_secret,
        access_token,
    )
    if granted_id != account_id:
        raise OtherAccount(JOURNAL_OTHER_ACCOUNT)
    recording.add_name(broker_title)
    await finish(
        journal_file,
        recording,
        args.output,
        host=host,
        port=PROTOBUF_PORT,
        tls=True,
        client_id=client_id,
        client_secret=client_secret,
        access_token=access_token,
        account_id=account_id,
        login=login,
        secrets=_secrets(env),
        raw_dir=args.raw_dir,
        outcome=outcome,
        status=lambda text: print(text, file=sys.stderr),
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
    kept_text = JOURNAL_KEPT if outcome.journal_readable else JOURNAL_UNREADABLE_KEPT
    kept = [f"{kept_text}: {outcome.journal}"] if outcome.journal is not None else []
    if outcome.recording is None:
        if rescrub:
            return ["The fixture was NOT written."]
        return kept or ["Nothing was recorded, and nothing was written."]
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
    return [*lines, *kept, *summary(outcome.recording)]


def _read_journal_file(
    journal: pathlib.Path,
    outcome: Outcome,
) -> tuple[Recording, int, int | None]:
    """`read_journal()` of `journal`; `Refused`, which quotes nothing of it, if it cannot be."""
    try:
        return read_journal(journal.read_bytes())
    except Exception:
        # Empty, a partly written header, a damaged line: whatever it says could quote the file.
        outcome.journal_readable = False
        raise Refused(JOURNAL_UNREADABLE) from None


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
    parser.add_argument(
        "--minutes",
        type=float,
        default=480.0,
        help="how long to record before stopping by itself",
    )
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
            text = describe(args.describe.read_bytes())
            # A note may hold a character the terminal's code page lacks: escaped, not fatal.
            encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
            print(text.encode(encoding, "backslashreplace").decode(encoding))
        except Refused as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        except OSError as e:
            print(f"error: {type(e).__name__}", file=sys.stderr)
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
        journal = journal_path(args.output, args.raw_dir)
        # An interrupted session comes first, whatever was asked: a new one is a separate run.
        finishing = args.rescrub is None and journal.exists()
        if finishing:
            outcome.journal = journal
            journaled = _read_journal_file(journal, outcome)
            print(f"{JOURNAL_FOUND}: {journal}")
            print(JOURNAL_DISCARD_HINT)
        _check_target(args.output, overwrite=args.overwrite)
        if args.rescrub is not None:
            rescrub(args.rescrub, args.output, _secrets(env), outcome)
        else:
            raw = raw_path(args.output, args.raw_dir)
            _check_target(raw, overwrite=args.overwrite, create_dir=True)
            if finishing:
                asyncio.run(_finish_run(args, env, outcome, journal, journaled))
            else:
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
    if outcome.recording is None:
        # A run that ended cleanly with nothing in hand did not do what it was started for.
        code = code or 1
    for line in _outcome_lines(outcome, args.output, rescrub=args.rescrub is not None):
        print(line)
    return code


if __name__ == "__main__":
    sys.exit(main())
