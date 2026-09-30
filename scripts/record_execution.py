"""Record a cTrader account's execution events while its owner trades by hand.

Read-only. The script listens for the events the venue pushes, asks for snapshots of the
account's positions and orders, and writes a scrubbed fixture. Every message it sends goes
through `send()`, which refuses any request class outside `READ_ONLY_REQUESTS`; no request that
places, changes or closes an order is named anywhere in this module.

    uv run python scripts/record_execution.py --trader-login <login>

While it runs, type a short note and press Enter to mark what you just did in the terminal
("moved the stop"); type `q` and Enter to stop. `--describe` prints a recording back.

What is written never holds the account id, the trader login, a token, a balance, or the
broker's own order, position and deal ids: those are replaced consistently, so a position still
lines up with its orders and deals. Timestamps are shifted by one constant, which keeps every
interval and hides when the session took place.
"""

from __future__ import annotations

import base64
import importlib.util
import json
import pathlib
import sys
from collections.abc import Iterable
from dataclasses import dataclass, field

from google.protobuf import text_format
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import Message

from nautilus_ctrader.common import codec
from nautilus_ctrader.common.connection import CTraderConnection
from nautilus_ctrader.constants import (
    BUCKET_DEFAULT,
    BUCKET_HISTORICAL,
)
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
_OUTPUT_PATH = _REPO_ROOT / "tests" / "fixtures" / "m3_execution_recorded.json"


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

_ID_KINDS = {"positionId": "position", "orderId": "order", "dealId": "deal"}
_ID_BASES = {"position": 5_000_000, "order": 6_000_000, "deal": 7_000_000}
# Free text the account's owner, or a robot of theirs, may have written.
_TEXT_FIELDS = frozenset({"label", "comment", "clientOrderId", "externalNote"})
# Money moved in or out of the account: nothing the order logic needs.
_CLEARED_MESSAGES = frozenset({"depositWithdraw", "bonusDepositWithdraw"})
_INT_TYPES = (FieldDescriptor.TYPE_INT64, FieldDescriptor.TYPE_UINT64)


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

    def replace_in_text(self, text: str) -> str:
        for known in self._fake.values():
            for real, fake in known.items():
                text = text.replace(str(real), str(fake))
        return text


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
    _scrub_in_place(result, ids, shift_ms)
    return result


def _scrub_in_place(message: Message, ids: IdMap, shift_ms: int) -> None:
    for descriptor, value in list(message.ListFields()):
        name = descriptor.name
        if name in _CLEARED_MESSAGES:
            message.ClearField(name)
        elif descriptor.type == FieldDescriptor.TYPE_MESSAGE:
            items = value if descriptor.label == FieldDescriptor.LABEL_REPEATED else (value,)
            for item in items:
                _scrub_in_place(item, ids, shift_ms)
        elif descriptor.label == FieldDescriptor.LABEL_REPEATED:
            continue
        elif name in _ID_KINDS and descriptor.type in _INT_TYPES:
            setattr(message, name, ids.fake(_ID_KINDS[name], value))
        elif descriptor.type == FieldDescriptor.TYPE_STRING and name in _TEXT_FIELDS:
            setattr(message, name, SCRUBBED_TEXT)
        elif descriptor.type == FieldDescriptor.TYPE_STRING and name == "description":
            # The broker's own wording is evidence; an id quoted inside it is not.
            setattr(message, name, ids.replace_in_text(value))
        elif name.endswith("Timestamp") and descriptor.type in _INT_TYPES and value > 0:
            setattr(message, name, value - shift_ms)


@dataclass
class Entry:
    t: float
    kind: str  # "event", "snapshot" or "marker"
    note: str
    message: Message | None


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

    def add(self, kind: str, note: str, message: Message | None, t: float) -> None:
        self.timeline.append(Entry(round(t, 3), kind, note, message))

    def messages(self) -> Iterable[Message]:
        for entry in self.timeline:
            if entry.message is not None:
                yield entry.message
        for items in self.closing.values():
            yield from items


def _encode(message: Message) -> dict:
    return {
        "type": message.payloadType,
        "payload": base64.b64encode(message.SerializeToString()).decode("ascii"),
    }


def encode_recording(
    recording: Recording,
    *,
    account_id: int,
    login: int | None,
) -> tuple[bytes, IdMap]:
    """The recording as the fixture's bytes, scrubbed, with the id map that scrubbing built."""
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

    timeline = []
    for entry in recording.timeline:
        item: dict = {"t": entry.t, "kind": entry.kind, "note": entry.note}
        if entry.message is not None:
            item.update(_encode(clean(entry.message)))
        timeline.append(item)
    output = {
        "format": FORMAT,
        "timeline": timeline,
        "closing": {
            key: [_encode(clean(message)) for message in items]
            for key, items in recording.closing.items()
        },
    }
    return json.dumps(output, indent=2).encode("utf-8"), ids


def decode_recording(data: bytes) -> dict:
    """The fixture with every payload parsed back into its protobuf message."""

    def decode(item: dict) -> Message:
        message = codec.payload_class(item["type"])()
        message.ParseFromString(base64.b64decode(item["payload"]))
        return message

    raw = json.loads(data)
    return {
        "format": raw["format"],
        "timeline": [
            {
                "t": item["t"],
                "kind": item["kind"],
                "note": item["note"],
                "message": decode(item) if "payload" in item else None,
            }
            for item in raw["timeline"]
        ],
        "closing": {key: [decode(item) for item in items] for key, items in raw["closing"].items()},
    }


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


def describe(data: bytes) -> str:
    """One line per timeline entry, from the already scrubbed fixture."""
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
