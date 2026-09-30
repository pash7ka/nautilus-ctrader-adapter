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

import argparse
import asyncio
import base64
import contextlib
import importlib.util
import json
import pathlib
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass, field

from google.protobuf import text_format
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import Message

from nautilus_ctrader.common import codec
from nautilus_ctrader.common.connection import CTraderConnection
from nautilus_ctrader.common.errors import CTraderError
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


def _position_ids(message: Message) -> set[int]:
    found: set[int] = set()
    for descriptor, value in message.ListFields():
        if descriptor.type == FieldDescriptor.TYPE_MESSAGE:
            items = value if descriptor.label == FieldDescriptor.LABEL_REPEATED else (value,)
            for item in items:
                found |= _position_ids(item)
        elif descriptor.name == "positionId" and descriptor.label != FieldDescriptor.LABEL_REPEATED:
            found.add(value)
    return found


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
) -> Recording:
    """Listen until `stop` is set or `minutes` pass; reconnect if the connection drops.

    `recording` lets the caller keep what was seen if this raises: it is filled in place.
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
            recording.add("marker", markers.get_nowait(), None, now())

    async def snapshot(connection: CTraderConnection, note: str) -> None:
        for separate, shape in ((False, "levels on positions"), (True, "protection orders")):
            response = await send(
                connection,
                oa.ProtoOAReconcileReq(
                    ctidTraderAccountId=account_id,
                    returnProtectionOrders=separate,
                ),
            )
            recording.add("snapshot", f"{note}; {shape}", response, now())

    async def closing(connection: CTraderConnection) -> None:
        window = {
            "fromTimestamp": recording.started_wall_ms - _CLOSING_LOOKBACK_MS,
            "toTimestamp": int(time.time() * 1000),
        }
        recording.closing["deals"].append(
            await send(
                connection,
                oa.ProtoOADealListReq(ctidTraderAccountId=account_id, **window),
                bucket=BUCKET_HISTORICAL,
            ),
        )
        recording.closing["orders"].append(
            await send(
                connection,
                oa.ProtoOAOrderListReq(ctidTraderAccountId=account_id, **window),
                bucket=BUCKET_HISTORICAL,
            ),
        )
        positions: set[int] = set()
        for message in list(recording.messages()):
            positions |= _position_ids(message)
        for position_id in sorted(positions):
            recording.closing["position_orders"].append(
                await send(
                    connection,
                    oa.ProtoOAOrderListByPositionIdReq(
                        ctidTraderAccountId=account_id,
                        positionId=position_id,
                    ),
                    bucket=BUCKET_HISTORICAL,
                ),
            )
            recording.closing["position_deals"].append(
                await send(
                    connection,
                    oa.ProtoOADealListByPositionIdReq(
                        ctidTraderAccountId=account_id,
                        positionId=position_id,
                    ),
                    bucket=BUCKET_HISTORICAL,
                ),
            )

    last_event: float | None = None

    def on_event(message: Message) -> None:
        nonlocal last_event
        # A note typed before the event arrived goes before it, not at the next tick.
        drain_markers()
        recording.add("event", "", message, now())
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
            first = False
            attempt = 0

            while not finished() and not lost.is_set():
                drain_markers()
                seen = last_event
                if seen is not None and time.monotonic() - seen >= snapshot_debounce_secs:
                    last_event = None
                    await snapshot(connection, "after events")
                await asyncio.sleep(tick_secs)

            # A note typed during the last tick.
            drain_markers()
            if not lost.is_set():
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
        except (CTraderError, OSError) as e:
            recording.add("marker", f"connection lost: {type(e).__name__}", None, now())
        finally:
            with contextlib.suppress(CTraderError, OSError):
                await connection.close()
        await asyncio.sleep(reconnect_waits[min(attempt, len(reconnect_waits) - 1)])
        attempt += 1
    drain_markers()
    recording.add("marker", "closing requests skipped: not connected", None, now())
    return recording


async def record_to_file(
    output: pathlib.Path,
    *,
    login: int | None,
    secrets: Iterable[str],
    account_id: int,
    **kwargs,
) -> None:
    """Run `record()` and write the fixture, whatever way the run ended.

    The owner's manual trades cannot be repeated on demand, so a run that fails, or is
    interrupted, still writes what it saw before the error is re-raised.
    """
    recording = Recording(int(time.time() * 1000))

    def write() -> None:
        data, ids = encode_recording(recording, account_id=account_id, login=login)
        check_clean(data, recording, account_id=account_id, login=login, ids=ids, secrets=secrets)
        output.write_bytes(data)

    try:
        await record(account_id=account_id, recording=recording, **kwargs)
    except BaseException as e:
        last = recording.timeline[-1].t if recording.timeline else 0.0
        recording.add("marker", f"run failed: {type(e).__name__}", None, last)
        write()
        raise
    write()


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
    matched = [
        a
        for a in listed.ctidTraderAccount
        if a.HasField("traderLogin") and a.traderLogin == trader_login
    ]
    if len(matched) != 1:
        raise RuntimeError(f"expected one granted account with that login, found {len(matched)}")
    return (LIVE_HOST if matched[0].isLive else DEMO_HOST), matched[0].ctidTraderAccountId


async def _read_keyboard(stop: asyncio.Event, markers: asyncio.Queue[str]) -> None:
    """A typed line becomes a marker; `q` stops the recording."""
    while not stop.is_set():
        line = await asyncio.to_thread(sys.stdin.readline)
        if not line:
            # End of input, where every further read returns at once: stop reading.
            return
        line = line.strip()
        if line.lower() == "q":
            stop.set()
        elif line:
            await markers.put(line)


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
Type q and press Enter to stop."""


async def _run(args: argparse.Namespace, env: dict[str, str]) -> None:
    client_id = env["CTRADER_CLIENT_ID"]
    client_secret = env["CTRADER_CLIENT_SECRET"]
    access_token = env["CTRADER_ACCESS_TOKEN"]
    host, account_id = await _resolve_account(
        args.trader_login,
        client_id,
        client_secret,
        access_token,
    )
    stop: asyncio.Event = asyncio.Event()
    markers: asyncio.Queue[str] = asyncio.Queue()
    print(_CHECKLIST)
    keyboard = asyncio.create_task(_read_keyboard(stop, markers))
    try:
        await record_to_file(
            args.output,
            login=args.trader_login,
            secrets=(client_id, client_secret, access_token, env.get("CTRADER_REFRESH_TOKEN", "")),
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
        )
    finally:
        stop.set()
        keyboard.cancel()
    print(f"Recording written to {args.output}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--describe", type=pathlib.Path, metavar="FILE")
    parser.add_argument("--trader-login", type=int)
    parser.add_argument("--minutes", type=float, default=120.0)
    parser.add_argument("--output", type=pathlib.Path, default=_OUTPUT_PATH)
    parser.add_argument("--env-file", type=pathlib.Path, default=_REPO_ROOT / ".env")
    args = parser.parse_args(argv)

    if args.describe is not None:
        print(describe(args.describe.read_bytes()))
        return 0
    if args.trader_login is None:
        parser.error("--trader-login is required to record")
    try:
        asyncio.run(_run(args, get_tokens.load_env(args.env_file)))
    except (CTraderError, OSError, RuntimeError, KeyError) as e:
        # The type only: a message could quote an identifier.
        print(f"error: {type(e).__name__}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
