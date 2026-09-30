"""Offline tests for `scripts/record_execution.py`.

The script is loaded by file path and registered in `sys.modules`, the way
`tests/test_verify_live_data.py` loads its script. What only a live session can show - the real
sequences - is not covered here; this file covers the recorder's own guarantees.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import importlib.util
import io
import json
import os
import pathlib
import subprocess
import sys

import pytest
from google.protobuf.descriptor import FieldDescriptor

from nautilus_ctrader.common import codec
from nautilus_ctrader.common.connection import CTraderConnection
from nautilus_ctrader.common.errors import CTraderRequestError, CTraderTimeoutError
from nautilus_ctrader.common.rate_limit import RateLimiter
from nautilus_ctrader.constants import BUCKET_DEFAULT, BUCKET_HISTORICAL
from nautilus_ctrader.messages import OpenApiCommonMessages_pb2 as common
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.fake_server import FakeCTraderServer
from tests.polling import wait_until

_SCRIPT_PATH = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "record_execution.py"
_SPEC = importlib.util.spec_from_file_location("record_execution", _SCRIPT_PATH)
r = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = r
_SPEC.loader.exec_module(r)

ACCOUNT_ID = 7654321
LOGIN = 8901234
POSITION_ID = 440_011_223
ORDER_ID = 550_022_334
DEAL_ID = 660_033_445
FAST = RateLimiter({BUCKET_DEFAULT: 1000.0, BUCKET_HISTORICAL: 1000.0})


def trade_data(*, label: str = "", comment: str = "") -> om.ProtoOATradeData:
    data = om.ProtoOATradeData(symbolId=1, volume=100_000, tradeSide=om.BUY)
    data.openTimestamp = 1_790_000_000_000
    if label:
        data.label = label
    if comment:
        data.comment = comment
    return data


def execution_event(
    *,
    position_id: int = POSITION_ID,
    order_id: int = ORDER_ID,
    deal_id: int = DEAL_ID,
    label: str = "",
    comment: str = "",
) -> oa.ProtoOAExecutionEvent:
    event = oa.ProtoOAExecutionEvent(
        ctidTraderAccountId=ACCOUNT_ID,
        executionType=om.ORDER_FILLED,
    )
    event.position.CopyFrom(
        om.ProtoOAPosition(
            positionId=position_id,
            tradeData=trade_data(label=label, comment=comment),
            positionStatus=om.POSITION_STATUS_OPEN,
            swap=0,
        ),
    )
    event.position.utcLastUpdateTimestamp = 1_790_000_000_500
    event.order.CopyFrom(
        om.ProtoOAOrder(
            orderId=order_id,
            tradeData=trade_data(label=label, comment=comment),
            orderType=om.MARKET,
            orderStatus=om.ORDER_STATUS_FILLED,
        ),
    )
    event.order.positionId = position_id
    event.deal.CopyFrom(
        om.ProtoOADeal(
            dealId=deal_id,
            orderId=order_id,
            positionId=position_id,
            volume=100_000,
            filledVolume=100_000,
            symbolId=1,
            createTimestamp=1_790_000_000_400,
            executionTimestamp=1_790_000_000_450,
            tradeSide=om.BUY,
            dealStatus=om.FILLED,
        ),
    )
    return event


def scrubbed(message, ids: r.IdMap | None = None, shift_ms: int = 0):
    return r.scrub_execution(
        message,
        account_id=ACCOUNT_ID,
        login=LOGIN,
        ids=ids if ids is not None else r.IdMap(),
        shift_ms=shift_ms,
    )


def test_the_send_point_refuses_a_request_that_changes_the_account() -> None:
    """The guarantee is the allow-list, checked before the socket is reached."""

    class NeverSends:
        async def request(self, *_args, **_kwargs):
            raise AssertionError("a refused request must not reach the connection")

    with pytest.raises(r.ReadOnlyViolation):
        asyncio.run(
            r.send(NeverSends(), oa.ProtoOANewOrderReq(ctidTraderAccountId=1, symbolId=1)),
        )


def test_the_allow_list_holds_nothing_that_changes_account_state() -> None:
    changing = ("Order", "Position", "Amend", "Cancel", "Close", "Deposit", "Withdraw")
    for request in r.READ_ONLY_REQUESTS:
        name = request.__name__
        # The list requests name what they list; only a request *acting* on one is a change.
        if name.startswith(("ProtoOAOrderList", "ProtoOADealList")):
            continue
        assert not any(word in name for word in changing), name


def test_the_script_never_names_a_request_that_changes_the_account() -> None:
    source = _SCRIPT_PATH.read_text(encoding="utf-8")
    for name in (
        "ProtoOANewOrderReq",
        "ProtoOACancelOrderReq",
        "ProtoOAAmendOrderReq",
        "ProtoOAAmendPositionSLTPReq",
        "ProtoOAClosePositionReq",
    ):
        assert name not in source, name


def test_the_allow_list_is_exactly_these_requests() -> None:
    """A request added to the list has to be added here too, where a reviewer sees it."""
    assert {
        oa.ProtoOAApplicationAuthReq,
        oa.ProtoOAAccountAuthReq,
        oa.ProtoOAGetAccountListByAccessTokenReq,
        oa.ProtoOATraderReq,
        oa.ProtoOAReconcileReq,
        oa.ProtoOADealListReq,
        oa.ProtoOAOrderListReq,
        oa.ProtoOADealListByPositionIdReq,
        oa.ProtoOAOrderListByPositionIdReq,
    } == r.READ_ONLY_REQUESTS


def test_send_is_the_only_way_to_the_connection() -> None:
    """One call of the connection's request method, inside `send()`, and no other way out."""
    source = _SCRIPT_PATH.read_text(encoding="utf-8")
    send_body = source.split("async def send(")[1].split("\nclass ")[0]

    assert source.count(".request(") == 1
    assert ".request(" in send_body
    # The connection's own lower layers: its unanswered send, its frame writers, its socket.
    for lower in ("send", "_write", "_write_now", "_writer"):
        assert hasattr(CTraderConnection("127.0.0.1", 1, logger=None), lower), lower
        assert f".{lower}" not in source, lower


def test_ids_are_remapped_consistently_across_messages() -> None:
    ids = r.IdMap()
    first = scrubbed(execution_event(), ids)
    second = scrubbed(execution_event(deal_id=DEAL_ID + 1), ids)

    assert first.position.positionId == second.position.positionId != POSITION_ID
    assert first.order.orderId == first.deal.orderId != ORDER_ID
    assert first.deal.positionId == first.position.positionId
    assert first.deal.dealId != second.deal.dealId
    assert POSITION_ID not in (first.position.positionId, second.position.positionId)
    assert set(ids.real_ids()) == {POSITION_ID, ORDER_ID, DEAL_ID, DEAL_ID + 1}


def test_text_fields_keep_their_presence_but_not_their_content() -> None:
    event = scrubbed(execution_event(label="my robot", comment="a private note"))

    assert event.position.tradeData.label == r.SCRUBBED_TEXT
    assert event.position.tradeData.comment == r.SCRUBBED_TEXT
    # An absent field stays absent: presence is part of what the recording is for.
    assert not scrubbed(execution_event()).position.tradeData.HasField("label")


def test_timestamps_shift_by_one_constant() -> None:
    shift = 190_000_000_000
    event = scrubbed(execution_event(), shift_ms=shift)

    assert event.deal.createTimestamp == 1_790_000_000_400 - shift
    assert event.deal.executionTimestamp - event.deal.createTimestamp == 50
    assert event.position.tradeData.openTimestamp == 1_790_000_000_000 - shift


def test_a_deposit_is_cleared_and_a_balance_zeroed() -> None:
    event = execution_event()
    event.depositWithdraw.CopyFrom(
        om.ProtoOADepositWithdraw(
            operationType=om.BALANCE_DEPOSIT,
            balanceHistoryId=1,
            balance=999_999,
            delta=500_000,
            changeBalanceTimestamp=1_790_000_000_000,
        ),
    )
    event.deal.closePositionDetail.CopyFrom(
        om.ProtoOAClosePositionDetail(
            entryPrice=1.1,
            grossProfit=1234,
            swap=-5,
            commission=-7,
            balance=999_999,
        ),
    )

    clean = scrubbed(event)

    assert not clean.HasField("depositWithdraw")
    assert clean.deal.closePositionDetail.balance == 0
    # Commission, swap and profit stay: they are what the scaling tests need.
    assert clean.deal.closePositionDetail.commission == -7
    assert clean.deal.closePositionDetail.grossProfit == 1234


def test_a_real_id_in_an_error_description_is_replaced() -> None:
    ids = r.IdMap()
    scrubbed(execution_event(), ids)
    error = oa.ProtoOAOrderErrorEvent(
        ctidTraderAccountId=ACCOUNT_ID,
        errorCode="POSITION_NOT_FOUND",
        positionId=POSITION_ID,
        description=f"Position {POSITION_ID} was not found",
    )

    clean = scrubbed(error, ids)

    assert str(POSITION_ID) not in clean.description
    assert str(clean.positionId) in clean.description


def recording_of(*messages) -> r.Recording:
    recording = r.Recording(started_wall_ms=1_790_000_000_000)
    for index, message in enumerate(messages):
        recording.add("event", "", message, float(index))
    return recording


def test_an_encoded_recording_round_trips_and_is_clean() -> None:
    recording = recording_of(execution_event())
    recording.add("marker", "moved the stop", None, 5.0)

    data, ids = r.encode_recording(recording, account_id=ACCOUNT_ID, login=LOGIN)
    r.check_clean(data, recording, account_id=ACCOUNT_ID, login=LOGIN, ids=ids, secrets=())
    decoded = r.decode_recording(data)

    assert decoded["format"] == 1
    assert [entry["kind"] for entry in decoded["timeline"]] == ["event", "marker"]
    assert decoded["timeline"][1]["note"] == "moved the stop"
    event = decoded["timeline"][0]["message"]
    assert event.ctidTraderAccountId == r.record_fixtures.FAKE_ACCOUNT_ID
    assert event.position.positionId == 5_000_001
    assert (event.order.orderId, event.deal.dealId) == (6_000_001, 7_000_001)


def test_check_clean_refuses_a_recording_that_still_holds_a_real_id() -> None:
    recording = recording_of(execution_event())
    data, ids = r.encode_recording(recording, account_id=ACCOUNT_ID, login=LOGIN)
    leaked = json.loads(data)
    leaked["timeline"][0]["note"] = f"position {POSITION_ID}"

    with pytest.raises(r.record_fixtures.ScrubError):
        r.check_clean(
            json.dumps(leaked).encode(),
            recording,
            account_id=ACCOUNT_ID,
            login=LOGIN,
            ids=ids,
            secrets=(),
        )


def fixture_of(*messages) -> bytes:
    """Fixture bytes holding `messages` as they are, with no scrubbing in the way."""
    timeline = [
        {
            "t": 0.0,
            "kind": "event",
            "note": "",
            "type": message.payloadType,
            "payload": base64.b64encode(message.SerializeToString()).decode("ascii"),
        }
        for message in messages
    ]
    closing = {"deals": [], "orders": [], "position_orders": [], "position_deals": []}
    return json.dumps({"format": 1, "timeline": timeline, "closing": closing}).encode()


def test_check_clean_sees_a_real_id_in_a_nested_numeric_field() -> None:
    """There it is a varint inside base64: the search of the JSON text cannot find it."""
    ids = r.IdMap()
    event = scrubbed(execution_event(), ids)
    event.deal.positionId = POSITION_ID

    with pytest.raises(r.record_fixtures.ScrubError):
        r.check_clean(
            fixture_of(event),
            recording_of(execution_event()),
            account_id=ACCOUNT_ID,
            login=LOGIN,
            ids=ids,
            secrets=(),
        )


def test_check_clean_refuses_a_recording_that_lost_a_message() -> None:
    ids = r.IdMap()
    data = fixture_of(scrubbed(execution_event(), ids))
    recording = recording_of(execution_event(), execution_event())

    with pytest.raises(r.record_fixtures.ScrubError, match="lost a message"):
        r.check_clean(data, recording, account_id=ACCOUNT_ID, login=LOGIN, ids=ids, secrets=())


def clean_timeline(recording: r.Recording) -> list[dict]:
    """The decoded timeline of `recording`, once `check_clean` has accepted it."""
    data, ids = r.encode_recording(recording, account_id=ACCOUNT_ID, login=LOGIN)
    r.check_clean(data, recording, account_id=ACCOUNT_ID, login=LOGIN, ids=ids, secrets=())
    return r.decode_recording(data)["timeline"]


def test_an_id_quoted_before_it_first_appears_is_still_replaced() -> None:
    error = oa.ProtoOAOrderErrorEvent(
        ctidTraderAccountId=ACCOUNT_ID,
        errorCode="POSITION_NOT_FOUND",
        description=f"Position {POSITION_ID} was not found",
    )

    first, second = clean_timeline(recording_of(error, execution_event()))

    fake = second["message"].position.positionId
    assert first["message"].description == f"Position {fake} was not found"
    # Numbered by first appearance in the recording, as a single pass would number them.
    assert fake == 5_000_001


def test_an_id_in_a_typed_note_is_replaced() -> None:
    recording = recording_of(execution_event())
    recording.add("marker", f"closed position {POSITION_ID} by hand", None, 5.0)

    event, marker = clean_timeline(recording)

    assert marker["note"] == f"closed position {event['message'].position.positionId} by hand"


def cleaned(text: str, ids: r.IdMap | None = None) -> str:
    return r.clean_text(text, account_id=ACCOUNT_ID, login=LOGIN, ids=ids or r.IdMap())


def test_a_longer_id_is_replaced_before_one_it_contains() -> None:
    ids = r.IdMap()
    short, long = ids.fake("position", 4400), ids.fake("order", 440_011_223)

    text = cleaned("order 440011223 on position 4400", ids)

    assert text == f"order {long} on position {short}"


def test_a_timestamp_below_the_shift_is_left_alone() -> None:
    # In seconds, where the shift is in milliseconds, and in a field that cannot go negative.
    notice = common.ProtoErrorRes(
        errorCode="SERVER_IS_UNDER_MAINTENANCE",
        maintenanceEndTimestamp=1_790_003_600,
    )

    clean = scrubbed(notice, shift_ms=190_000_000_000)

    assert clean.maintenanceEndTimestamp == 1_790_003_600


def test_a_zero_id_is_not_an_id() -> None:
    """Mapped like a real one, it would replace every 0 a note holds."""
    recording = recording_of(execution_event(position_id=0))
    recording.add("marker", "moved the stop 10 pips, 0 left", None, 5.0)

    data, ids = r.encode_recording(recording, account_id=ACCOUNT_ID, login=LOGIN)
    event, marker = r.decode_recording(data)["timeline"]

    assert marker["note"] == "moved the stop 10 pips, 0 left"
    assert event["message"].position.positionId == 0
    assert 0 not in ids.real_ids()


def test_an_id_inside_a_longer_number_is_not_that_id() -> None:
    ids = r.IdMap()
    fake = ids.fake("position", POSITION_ID)

    text = cleaned(f"ref 99{POSITION_ID}99, and {POSITION_ID}", ids)

    # The longer number is taken out whole; the fake id is not written into the middle of it.
    assert text == f"ref {r.NUMBER_PLACEHOLDER}, and {fake}"


@pytest.mark.parametrize(
    "text",
    ["TP 1.1050000000000001", "price 1234567.5", "rate 1234567.12345678 exactly"],
    ids=["a long fraction", "a long integer part", "both"],
)
def test_a_decimal_number_is_not_cut_into(text) -> None:
    assert cleaned(text) == text


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Order 313131313 not found.", "Order <number> not found."),
        ("order 313131313.", "order <number>."),
        ("see 313131313. 5 left", "see <number>. 5 left"),
    ],
    ids=["a full stop mid-sentence", "a full stop at the end", "a full stop then a number"],
)
def test_a_long_number_before_a_full_stop_is_still_taken_out(text, expected) -> None:
    assert cleaned(text) == expected.replace("<number>", r.NUMBER_PLACEHOLDER)


def test_a_known_id_is_replaced_even_inside_a_decimal() -> None:
    ids = r.IdMap()
    fake = ids.fake("position", POSITION_ID)

    text = cleaned(f"position {POSITION_ID}.5, account {ACCOUNT_ID}.0, login 0.{LOGIN}", ids)

    assert text == (
        f"position {fake}.5, account {r.record_fixtures.FAKE_ACCOUNT_ID}.0, "
        f"login 0.{r.record_fixtures.FAKE_TRADER_LOGIN}"
    )


def test_the_account_and_login_in_a_text_become_their_fake_values() -> None:
    error = oa.ProtoOAOrderErrorEvent(
        ctidTraderAccountId=ACCOUNT_ID,
        errorCode="TRADING_DISABLED",
        description=f"Trading is disabled for account {ACCOUNT_ID} (login {LOGIN})",
    )
    recording = recording_of(error)
    recording.add("marker", f"checked account {ACCOUNT_ID}", None, 5.0)

    event, marker = clean_timeline(recording)

    fake_account, fake_login = (
        r.record_fixtures.FAKE_ACCOUNT_ID,
        r.record_fixtures.FAKE_TRADER_LOGIN,
    )
    assert event["message"].description == (
        f"Trading is disabled for account {fake_account} (login {fake_login})"
    )
    assert marker["note"] == f"checked account {fake_account}"


def test_a_reason_is_cleaned_like_a_description() -> None:
    ids = r.IdMap()
    scrubbed(execution_event(), ids)
    gone = oa.ProtoOAClientDisconnectEvent(
        reason=f"Account {ACCOUNT_ID} was closed with position {POSITION_ID} open",
    )

    clean = scrubbed(gone, ids)

    assert clean.reason == (
        f"Account {r.record_fixtures.FAKE_ACCOUNT_ID} was closed with position 5000001 open"
    )


def test_a_long_number_no_field_identified_is_taken_out_of_a_text() -> None:
    error = oa.ProtoOAOrderErrorEvent(
        ctidTraderAccountId=ACCOUNT_ID,
        errorCode="ORDER_NOT_FOUND",
        description="Order 313131313 not found",
    )
    recording = recording_of(error)
    recording.add("marker", "moved stop to 1.1050, volume 100000, ticket 313131313", None, 5.0)

    event, marker = clean_timeline(recording)

    assert event["message"].description == f"Order {r.NUMBER_PLACEHOLDER} not found"
    # A price and a volume are shorter than any id, and are left as typed.
    assert marker["note"] == f"moved stop to 1.1050, volume 100000, ticket {r.NUMBER_PLACEHOLDER}"
    assert not any(char.isdigit() for char in r.NUMBER_PLACEHOLDER)


def test_owner_text_and_money_movements_are_gone_from_the_fixture() -> None:
    event = execution_event()
    event.order.clientOrderId = "my-robot-17"
    event.depositWithdraw.CopyFrom(
        om.ProtoOADepositWithdraw(
            operationType=om.BALANCE_DEPOSIT,
            balanceHistoryId=1,
            balance=999_999,
            delta=500_000,
            changeBalanceTimestamp=1_790_000_000_000,
            externalNote="from my savings",
        ),
    )
    event.bonusDepositWithdraw.CopyFrom(
        om.ProtoOABonusDepositWithdraw(
            operationType=om.BONUS_DEPOSIT,
            bonusHistoryId=1,
            managerBonus=1,
            managerDelta=1,
            ibBonus=1,
            ibDelta=1,
            changeBonusTimestamp=1_790_000_000_000,
        ),
    )

    (decoded,) = clean_timeline(recording_of(event))

    clean = decoded["message"]
    assert clean.order.clientOrderId == r.SCRUBBED_TEXT
    assert not clean.HasField("depositWithdraw")
    assert not clean.HasField("bonusDepositWithdraw")
    assert b"savings" not in clean.SerializeToString()


def with_unknown_field(message, text: bytes):
    """`message` as a newer venue would send it: one more field, number 99, holding `text`."""
    # 0x9a 0x06 is the tag of field 99 with a length-delimited value.
    extended = type(message)()
    extended.ParseFromString(message.SerializeToString() + b"\x9a\x06" + bytes([len(text)]) + text)
    return extended


def test_a_field_the_bindings_do_not_know_is_dropped_and_named() -> None:
    recording = recording_of(with_unknown_field(execution_event(), b"private words"))
    recording.add("marker", "moved the stop", None, 5.0)

    event, _, dropped = clean_timeline(recording)

    assert b"private words" not in event["message"].SerializeToString()
    assert dropped["kind"] == "marker"
    assert dropped["note"] == "unknown fields dropped from: ProtoOAExecutionEvent"
    # The unscrubbed copy keeps it: that is the evidence that the schema has moved.
    raw, _, _ = r.decode_raw(r.encode_raw(recording, account_id=ACCOUNT_ID, login=LOGIN))
    assert b"private words" in raw.timeline[0].message.SerializeToString()


def test_a_raw_recording_rebuilds_the_same_fixture() -> None:
    recording = recording_of(execution_event(label="my robot"))
    recording.add("marker", f"closed position {POSITION_ID}", None, 5.0, typed=True)
    recording.add("marker", "reconnected", None, 6.0)
    recording.closing["deals"].append(
        oa.ProtoOADealListRes(ctidTraderAccountId=ACCOUNT_ID, hasMore=False),
    )

    raw = r.encode_raw(recording, account_id=ACCOUNT_ID, login=LOGIN)
    rebuilt, account_id, login = r.decode_raw(raw)

    assert (account_id, login) == (ACCOUNT_ID, LOGIN)
    # Which notes were typed survives, though the fixture itself has no place for it.
    assert [entry.typed for entry in rebuilt.timeline] == [False, True, False]
    # Unscrubbed: this is what makes the file unfit for a committed path.
    assert rebuilt.timeline[0].message.ctidTraderAccountId == ACCOUNT_ID
    assert (
        r.encode_recording(rebuilt, account_id=account_id, login=login)[0]
        == (r.encode_recording(recording, account_id=ACCOUNT_ID, login=LOGIN)[0])
    )


def test_a_raw_recording_is_ignored_by_git_wherever_it_is_put() -> None:
    repo = pathlib.Path(__file__).resolve().parents[1]
    ignored = (repo / ".gitignore").read_text(encoding="utf-8").splitlines()

    # The default directory as a whole, and the file by its name anywhere else.
    assert repo / "tests" / "recordings" == r._RAW_DIR
    assert "tests/recordings/" in ignored
    assert r.raw_path(pathlib.Path("a/b/session.json"), r._RAW_DIR).name == "session.raw.json"
    assert "*.raw.json" in ignored


def fields_of(descriptor, seen: set[str]):
    """Every field of a message type and of the message types nested in it, each type once."""
    if descriptor.full_name in seen:
        return
    seen.add(descriptor.full_name)
    for field in descriptor.fields:
        yield descriptor.name, field
        if field.type == FieldDescriptor.TYPE_MESSAGE:
            yield from fields_of(field.message_type, seen)


def test_no_repeated_scalar_field_holds_what_the_scrubbing_would_have_to_touch() -> None:
    """The scrubbing skips repeated scalars; this pins that no id, text or time lives in one.

    Walked over every payload type of the schema, which covers whatever the venue can push
    and every response to a request on the allow-list.
    """
    handled = r._ID_KINDS.keys() | r._TEXT_FIELDS | r._VENUE_TEXT_FIELDS
    payload_types = codec._build_registry().values()
    assert {oa.ProtoOAExecutionEvent, oa.ProtoOAOrderErrorEvent, oa.ProtoOAReconcileRes} <= set(
        payload_types,
    )

    for payload_type in payload_types:
        for owner, field in fields_of(payload_type.DESCRIPTOR, set()):
            if field.label != FieldDescriptor.LABEL_REPEATED:
                continue
            if field.type == FieldDescriptor.TYPE_MESSAGE:
                continue
            where = f"{payload_type.__name__}: {owner}.{field.name}"
            assert field.name not in handled, where
            assert not field.name.endswith("Timestamp"), where


def test_describe_prints_no_real_identifier() -> None:
    data, _ = r.encode_recording(
        recording_of(execution_event()), account_id=ACCOUNT_ID, login=LOGIN
    )

    text = r.describe(data)

    assert "ORDER_FILLED" in text
    for real in (ACCOUNT_ID, LOGIN, POSITION_ID, ORDER_ID, DEAL_ID):
        assert str(real) not in text


def venue() -> FakeCTraderServer:
    server = FakeCTraderServer()
    server.on(om.PROTO_OA_APPLICATION_AUTH_REQ, lambda _r: oa.ProtoOAApplicationAuthRes())
    server.on(
        om.PROTO_OA_ACCOUNT_AUTH_REQ,
        lambda q: oa.ProtoOAAccountAuthRes(ctidTraderAccountId=q.ctidTraderAccountId),
    )
    server.on(
        om.PROTO_OA_RECONCILE_REQ,
        lambda q: oa.ProtoOAReconcileRes(ctidTraderAccountId=q.ctidTraderAccountId),
    )
    server.on(
        om.PROTO_OA_DEAL_LIST_REQ,
        lambda q: oa.ProtoOADealListRes(ctidTraderAccountId=q.ctidTraderAccountId, hasMore=False),
    )
    server.on(
        om.PROTO_OA_ORDER_LIST_REQ,
        lambda q: oa.ProtoOAOrderListRes(ctidTraderAccountId=q.ctidTraderAccountId, hasMore=False),
    )
    server.on(
        om.PROTO_OA_DEAL_LIST_BY_POSITION_ID_REQ,
        lambda q: oa.ProtoOADealListByPositionIdRes(
            ctidTraderAccountId=q.ctidTraderAccountId,
            hasMore=False,
        ),
    )
    server.on(
        om.PROTO_OA_ORDER_LIST_BY_POSITION_ID_REQ,
        lambda q: oa.ProtoOAOrderListByPositionIdRes(
            ctidTraderAccountId=q.ctidTraderAccountId,
            hasMore=False,
        ),
    )
    return server


def settings(
    server: FakeCTraderServer,
    stop: asyncio.Event,
    markers: asyncio.Queue,
    **overrides,
) -> dict:
    """The arguments of a `record()` run against `server`, fast enough for a test."""
    return {
        "host": server.host,
        "port": server.port,
        "tls": False,
        "client_id": "client-id",
        "client_secret": "client-secret",
        "access_token": "access-token",
        "account_id": ACCOUNT_ID,
        "minutes": 1.0,
        "stop": stop,
        "markers": markers,
        "rate_limiter": FAST,
        "snapshot_debounce_secs": 0.05,
        "tick_secs": 0.01,
        "reconnect_waits": (0.05,),
    } | overrides


async def run(server: FakeCTraderServer, stop: asyncio.Event, markers: asyncio.Queue, **kwargs):
    return await r.record(**settings(server, stop, markers, **kwargs))


async def to_file(output: pathlib.Path, **kwargs) -> None:
    """`record_to_file` with the unscrubbed copy kept beside `output`, never in the repository."""
    arguments = {"login": LOGIN, "secrets": (), "raw_dir": output.parent / "raw"} | kwargs
    await r.record_to_file(output, **arguments)


def raw_of(output: pathlib.Path) -> pathlib.Path:
    return r.raw_path(output, output.parent / "raw")


def refusal(code: str):
    return lambda _request: oa.ProtoOAErrorRes(errorCode=code)


def refusing_after(server: FakeCTraderServer, payload_type: int, allowed: int, code: str) -> None:
    """Answer `payload_type` as before `allowed` times, then refuse it with `code`."""
    answer = server._handlers[payload_type]
    seen = 0

    def handler(request):
        nonlocal seen
        seen += 1
        return answer(request) if seen <= allowed else oa.ProtoOAErrorRes(errorCode=code)

    server.on(payload_type, handler)


def marker_notes(recording: r.Recording) -> list[str]:
    return [entry.note for entry in recording.timeline if entry.kind == "marker"]


async def test_a_run_records_events_snapshots_and_markers_in_order() -> None:
    server = venue()
    await server.start()
    stop, markers = asyncio.Event(), asyncio.Queue()
    recording = r.Recording(started_wall_ms=1_790_000_000_000)
    task = asyncio.create_task(run(server, stop, markers, recording=recording))
    try:
        await server.wait_for_connections(1)
        # Both start snapshots first: an event pushed between them is recorded between them.
        await wait_until(lambda: len(recording.timeline) >= 2)
        await markers.put("market buy with stop and target")
        await server.push(execution_event())
        # The debounced snapshot follows the event: two more reconcile requests.
        await wait_until(
            lambda: sum(isinstance(m, oa.ProtoOAReconcileReq) for m in server.received) >= 4,
        )
        stop.set()
        assert await asyncio.wait_for(task, 10) is recording
    finally:
        task.cancel()
        await server.stop()

    kinds = [entry.kind for entry in recording.timeline]
    assert kinds[:2] == ["snapshot", "snapshot"]
    assert "marker" in kinds and "event" in kinds
    assert kinds.index("marker") < kinds.index("event")
    times = [entry.t for entry in recording.timeline]
    assert times == sorted(times)
    # The position the event named is asked about at the end.
    assert any(isinstance(m, oa.ProtoOAOrderListByPositionIdReq) for m in server.received)
    assert len(recording.closing["deals"]) == 1
    # Nothing sent to the venue changes the account.
    assert {type(m) for m in server.received} <= r.READ_ONLY_REQUESTS


async def test_a_snapshot_is_named_by_the_flag_it_was_asked_with() -> None:
    server = venue()
    await server.start()
    stop, markers = asyncio.Event(), asyncio.Queue()
    recording = r.Recording(started_wall_ms=1_790_000_000_000)
    task = asyncio.create_task(run(server, stop, markers, recording=recording))
    try:
        await wait_until(lambda: len(recording.timeline) >= 2)
        stop.set()
        await asyncio.wait_for(task, 10)
    finally:
        task.cancel()
        await server.stop()

    assert [entry.note for entry in recording.timeline[:2]] == [
        "start; returnProtectionOrders=false",
        "start; returnProtectionOrders=true",
    ]
    asked = [m for m in server.received if isinstance(m, oa.ProtoOAReconcileReq)]
    assert [m.returnProtectionOrders for m in asked[:2]] == [False, True]


async def test_a_note_typed_before_an_event_is_recorded_before_it() -> None:
    server = venue()
    await server.start()
    stop, markers = asyncio.Event(), asyncio.Queue()
    recording = r.Recording(started_wall_ms=1_790_000_000_000)
    # The tick is too slow to place the note: only the event's own arrival can.
    task = asyncio.create_task(run(server, stop, markers, recording=recording, tick_secs=0.3))
    try:
        await server.wait_for_connections(1)
        await wait_until(lambda: len(recording.timeline) >= 2)
        markers.put_nowait("market buy with stop and target")
        await server.push(execution_event())
        await wait_until(lambda: len(recording.timeline) >= 4)
        kinds = [entry.kind for entry in recording.timeline[:4]]
        stop.set()
        await asyncio.wait_for(task, 10)
    finally:
        task.cancel()
        await server.stop()

    assert kinds == ["snapshot", "snapshot", "marker", "event"]


async def test_a_note_typed_just_before_stopping_is_kept() -> None:
    server = venue()
    await server.start()
    stop, markers = asyncio.Event(), asyncio.Queue()
    recording = r.Recording(started_wall_ms=1_790_000_000_000)
    task = asyncio.create_task(run(server, stop, markers, recording=recording))
    try:
        await server.wait_for_connections(1)
        await wait_until(lambda: len(recording.timeline) >= 2)
        # No tick runs between the note and the stop.
        markers.put_nowait("closed by hand")
        stop.set()
        await asyncio.wait_for(task, 10)
    finally:
        task.cancel()
        await server.stop()

    assert "closed by hand" in [entry.note for entry in recording.timeline]


async def test_a_dropped_connection_is_reconnected_and_marked() -> None:
    server = venue()
    await server.start()
    stop, markers = asyncio.Event(), asyncio.Queue()
    statuses: list[str] = []
    recording = r.Recording(started_wall_ms=1_790_000_000_000)
    task = asyncio.create_task(
        run(server, stop, markers, recording=recording, status=statuses.append),
    )
    try:
        await wait_until(lambda: statuses == [r.STATUS_STARTED])
        await server.drop_connections()
        await wait_until(lambda: r.STATUS_RECONNECTED in statuses)
        await server.push(execution_event())
        await wait_until(lambda: any(entry.kind == "event" for entry in recording.timeline))
        stop.set()
        await asyncio.wait_for(task, 10)
    finally:
        task.cancel()
        await server.stop()

    notes = marker_notes(recording)
    assert any("connection lost" in note for note in notes)
    assert any("reconnected" in note for note in notes)
    assert statuses[:3] == [r.STATUS_STARTED, r.STATUS_LOST, r.STATUS_RECONNECTED]
    assert server.connection_count == 2


async def test_events_are_announced_once_their_burst_is_over() -> None:
    """The owner sees that what they did reached the recording, by count and type only."""
    server = venue()
    await server.start()
    stop, markers = asyncio.Event(), asyncio.Queue()
    statuses: list[str] = []
    # A debounce long enough that the three pushes below are one burst on any machine.
    task = asyncio.create_task(
        run(server, stop, markers, status=statuses.append, snapshot_debounce_secs=0.3),
    )
    try:
        await wait_until(lambda: statuses == [r.STATUS_STARTED])
        await server.push(execution_event())
        await server.push(execution_event(deal_id=DEAL_ID + 1))
        await server.push(
            oa.ProtoOAOrderErrorEvent(ctidTraderAccountId=ACCOUNT_ID, errorCode="MARKET_CLOSED"),
        )
        await wait_until(lambda: len(statuses) >= 2)
        stop.set()
        recording = await asyncio.wait_for(task, 10)
    finally:
        task.cancel()
        await server.stop()

    assert statuses[1] == "events: 3 (ProtoOAExecutionEvent x2, ProtoOAOrderErrorEvent x1)"
    assert sum(entry.kind == "event" for entry in recording.timeline) == 3


async def test_the_deadline_ends_the_run_and_is_announced() -> None:
    server = venue()
    await server.start()
    statuses: list[str] = []
    try:
        recording = await asyncio.wait_for(
            run(
                server,
                asyncio.Event(),
                asyncio.Queue(),
                minutes=0.001,
                status=statuses.append,
            ),
            10,
        )
    finally:
        await server.stop()

    assert statuses == [r.STATUS_STARTED, r.STATUS_DEADLINE]
    assert len(recording.closing["deals"]) == 1


async def test_events_before_a_drop_are_announced_at_the_drop() -> None:
    """Not carried into the next connection's count, and not lost to the owner either."""
    server = venue()
    await server.start()
    stop, markers = asyncio.Event(), asyncio.Queue()
    statuses: list[str] = []
    recording = r.Recording(started_wall_ms=1_790_000_000_000)
    # The debounce never fires on the first connection: the drop comes first.
    task = asyncio.create_task(
        run(
            server,
            stop,
            markers,
            recording=recording,
            status=statuses.append,
            snapshot_debounce_secs=0.3,
        ),
    )
    one = "events: 1 (ProtoOAExecutionEvent x1)"
    try:
        await wait_until(lambda: statuses == [r.STATUS_STARTED])
        await server.push(execution_event())
        await wait_until(lambda: any(entry.kind == "event" for entry in recording.timeline))
        await server.drop_connections()
        await wait_until(lambda: r.STATUS_RECONNECTED in statuses)
        await server.push(execution_event(deal_id=DEAL_ID + 1))
        await wait_until(lambda: statuses.count(one) == 2)
        stop.set()
        await asyncio.wait_for(task, 10)
    finally:
        task.cancel()
        await server.stop()

    assert statuses == [r.STATUS_STARTED, one, r.STATUS_LOST, r.STATUS_RECONNECTED, one]


@pytest.mark.parametrize(
    "refused",
    [om.PROTO_OA_APPLICATION_AUTH_REQ, om.PROTO_OA_ACCOUNT_AUTH_REQ, om.PROTO_OA_RECONCILE_REQ],
    ids=["application auth", "account auth", "the start snapshot"],
)
async def test_a_refusal_before_anything_is_recorded_ends_the_run_and_writes_nothing(
    tmp_path,
    refused,
) -> None:
    server = venue()
    server.on(refused, refusal("INVALID_REQUEST"))
    await server.start()
    output = tmp_path / "recording.json"
    outcome = r.Outcome()
    try:
        with pytest.raises(CTraderRequestError) as raised:
            await asyncio.wait_for(
                to_file(
                    output,
                    outcome=outcome,
                    **settings(server, asyncio.Event(), asyncio.Queue()),
                ),
                2,
            )
    finally:
        await server.stop()

    assert raised.value.error_code == "INVALID_REQUEST"
    # Asked once: a refusal is not retried the way a lost connection is.
    assert server.connection_count == 1
    # A file holding nothing but the failure would only stand in the way of the next run.
    assert not output.exists() and not raw_of(output).exists()
    assert outcome.recording is None


async def test_a_run_that_fails_at_once_leaves_an_earlier_pair_of_files_alone(tmp_path) -> None:
    server = venue()
    server.on(om.PROTO_OA_ACCOUNT_AUTH_REQ, refusal("CH_ACCESS_TOKEN_INVALID"))
    await server.start()
    output = tmp_path / "recording.json"
    raw_of(output).parent.mkdir()
    output.write_bytes(b"an earlier fixture")
    raw_of(output).write_bytes(b"an earlier raw recording")
    try:
        with pytest.raises(CTraderRequestError):
            await asyncio.wait_for(
                to_file(output, **settings(server, asyncio.Event(), asyncio.Queue())),
                2,
            )
    finally:
        await server.stop()

    assert output.read_bytes() == b"an earlier fixture"
    assert raw_of(output).read_bytes() == b"an earlier raw recording"


async def test_an_interrupt_before_recording_started_writes_nothing(tmp_path) -> None:
    # A venue that accepts the connection and never answers: the run is still authenticating.
    server = FakeCTraderServer()
    await server.start()
    output = tmp_path / "recording.json"
    task = asyncio.create_task(
        to_file(output, **settings(server, asyncio.Event(), asyncio.Queue())),
    )
    try:
        await wait_until(lambda: len(server.received) == 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        task.cancel()
        await server.stop()

    assert not output.exists() and not raw_of(output).exists()


async def test_a_refusal_after_the_start_snapshot_still_writes_the_file(tmp_path) -> None:
    server = venue()
    await server.start()
    stop, markers = asyncio.Event(), asyncio.Queue()
    statuses: list[str] = []
    output = tmp_path / "recording.json"
    task = asyncio.create_task(
        to_file(output, **settings(server, stop, markers, status=statuses.append)),
    )
    try:
        await wait_until(lambda: statuses == [r.STATUS_STARTED])
        server.on(om.PROTO_OA_ACCOUNT_AUTH_REQ, refusal("CH_ACCESS_TOKEN_INVALID"))
        await server.drop_connections()
        with pytest.raises(CTraderRequestError):
            await asyncio.wait_for(task, 5)
    finally:
        task.cancel()
        await server.stop()

    timeline = r.decode_recording(output.read_bytes())["timeline"]
    assert [item["kind"] for item in timeline[:2]] == ["snapshot", "snapshot"]
    assert timeline[-1]["note"] == "run failed: CTraderRequestError"
    assert raw_of(output).exists()


@pytest.mark.parametrize(
    "refused",
    [om.PROTO_OA_ACCOUNT_AUTH_REQ, om.PROTO_OA_RECONCILE_REQ],
    ids=["account auth", "the snapshot after the reconnect"],
)
async def test_a_refusal_after_a_reconnect_ends_the_run_too(refused) -> None:
    server = venue()
    await server.start()
    stop, markers = asyncio.Event(), asyncio.Queue()
    statuses: list[str] = []
    task = asyncio.create_task(run(server, stop, markers, status=statuses.append))
    try:
        await wait_until(lambda: statuses == [r.STATUS_STARTED])
        server.on(refused, refusal("CH_ACCESS_TOKEN_INVALID"))
        await server.drop_connections()
        with pytest.raises(CTraderRequestError):
            await asyncio.wait_for(task, 5)
    finally:
        task.cancel()
        await server.stop()

    assert statuses == [r.STATUS_STARTED, r.STATUS_LOST]
    assert server.connection_count == 2


async def test_a_snapshot_refused_after_events_is_marked_and_the_run_goes_on() -> None:
    server = venue()
    # The two start requests are answered; every later one is refused.
    refusing_after(server, om.PROTO_OA_RECONCILE_REQ, 2, "REQUEST_FREQUENCY_EXCEEDED")
    await server.start()
    stop, markers = asyncio.Event(), asyncio.Queue()
    statuses: list[str] = []
    recording = r.Recording(started_wall_ms=1_790_000_000_000)
    task = asyncio.create_task(
        run(server, stop, markers, recording=recording, status=statuses.append),
    )
    refused = "snapshot refused: REQUEST_FREQUENCY_EXCEEDED"
    try:
        await wait_until(lambda: statuses == [r.STATUS_STARTED])
        await server.push(execution_event())
        await wait_until(lambda: refused in statuses)
        # Still listening: a later event is recorded, on the same connection.
        await server.push(execution_event(deal_id=DEAL_ID + 1))
        await wait_until(lambda: statuses.count(refused) == 2)
        stop.set()
        await asyncio.wait_for(task, 10)
    finally:
        task.cancel()
        await server.stop()

    assert marker_notes(recording).count(refused) == 2
    assert sum(entry.kind == "event" for entry in recording.timeline) == 2
    assert server.connection_count == 1


async def test_a_stop_takes_effect_during_a_reconnect_wait() -> None:
    server = venue()
    await server.start()
    stop, markers = asyncio.Event(), asyncio.Queue()
    statuses: list[str] = []
    task = asyncio.create_task(
        run(server, stop, markers, status=statuses.append, reconnect_waits=(30.0,)),
    )
    try:
        await wait_until(lambda: statuses == [r.STATUS_STARTED])
        await server.drop_connections()
        await wait_until(lambda: r.STATUS_LOST in statuses)
        stop.set()
        recording = await asyncio.wait_for(task, 5)
    finally:
        task.cancel()
        await server.stop()

    assert marker_notes(recording)[-1] == "closing requests skipped: not connected"


async def test_a_truncated_closing_list_is_marked() -> None:
    server = venue()
    server.on(
        om.PROTO_OA_DEAL_LIST_REQ,
        lambda q: oa.ProtoOADealListRes(ctidTraderAccountId=q.ctidTraderAccountId, hasMore=True),
    )
    await server.start()
    stop, markers = asyncio.Event(), asyncio.Queue()
    recording = r.Recording(started_wall_ms=1_790_000_000_000)
    task = asyncio.create_task(run(server, stop, markers, recording=recording))
    try:
        await wait_until(lambda: len(recording.timeline) >= 2)
        stop.set()
        await asyncio.wait_for(task, 10)
    finally:
        task.cancel()
        await server.stop()

    assert marker_notes(recording) == ["closing list truncated: deals"]


async def test_a_recording_is_written_even_when_the_run_fails(tmp_path) -> None:
    """A session that ends badly still leaves what it saw: the owner's trades are not repeatable."""
    server = venue()
    # One closing request is refused. The run does not fail: the list is marked as missing,
    # and the requests after it are still asked.
    server.on(
        om.PROTO_OA_DEAL_LIST_REQ,
        lambda q: oa.ProtoOAErrorRes(
            ctidTraderAccountId=q.ctidTraderAccountId,
            errorCode="INVALID_REQUEST",
        ),
    )
    await server.start()
    stop, markers = asyncio.Event(), asyncio.Queue()
    output = tmp_path / "recording.json"
    task = asyncio.create_task(
        to_file(
            output,
            secrets=("client-secret", "access-token"),
            host=server.host,
            port=server.port,
            tls=False,
            client_id="client-id",
            client_secret="client-secret",
            access_token="access-token",
            account_id=ACCOUNT_ID,
            minutes=1.0,
            stop=stop,
            markers=markers,
            rate_limiter=FAST,
            snapshot_debounce_secs=0.05,
            tick_secs=0.01,
            reconnect_waits=(0.05,),
        ),
    )
    try:
        await server.wait_for_connections(1)
        await wait_until(
            lambda: any(isinstance(m, oa.ProtoOAReconcileReq) for m in server.received)
        )
        await server.push(execution_event())
        await wait_until(
            lambda: sum(isinstance(m, oa.ProtoOAReconcileReq) for m in server.received) >= 4,
        )
        stop.set()
        await asyncio.wait_for(task, 10)
    finally:
        task.cancel()
        await server.stop()

    decoded = r.decode_recording(output.read_bytes())
    events = [item["message"] for item in decoded["timeline"] if item["kind"] == "event"]
    assert [event.ctidTraderAccountId for event in events] == [r.record_fixtures.FAKE_ACCOUNT_ID]
    assert events[0].position.positionId == 5_000_001
    notes = [item["note"] for item in decoded["timeline"] if item["kind"] == "marker"]
    assert notes == ["closing list refused: deals INVALID_REQUEST"]
    assert decoded["closing"]["deals"] == []
    assert len(decoded["closing"]["orders"]) == 1
    assert len(decoded["closing"]["position_orders"]) == 1


@pytest.mark.parametrize(
    "error",
    [CTraderTimeoutError("no response"), asyncio.CancelledError()],
    ids=["an error", "a cancellation"],
)
async def test_a_recording_is_written_when_the_run_raises(tmp_path, monkeypatch, error) -> None:
    """What `record()` does not handle itself still leaves the file, and still propagates."""

    async def failing(*, recording: r.Recording, **_kwargs) -> r.Recording:
        recording.add("event", "", execution_event(), 1.5)
        recording.add("marker", "moved the stop", None, 2.5)
        raise error

    monkeypatch.setattr(r, "record", failing)
    output = tmp_path / "recording.json"

    with pytest.raises(type(error)):
        await to_file(output, account_id=ACCOUNT_ID)

    timeline = r.decode_recording(output.read_bytes())["timeline"]
    assert [item["kind"] for item in timeline] == ["event", "marker", "marker"]
    assert timeline[-1]["note"] == f"run failed: {type(error).__name__}"
    assert timeline[-1]["t"] >= timeline[-2]["t"]


def recorded(*entries) -> object:
    """A stand-in for `record()` that fills the recording with `entries` and returns.

    An entry is `(kind, note, message, t)`; a marker entry is a typed note.
    """

    async def record(*, recording: r.Recording, **_kwargs) -> r.Recording:
        for kind, note, message, t in entries:
            recording.add(kind, note, message, t, typed=kind == "marker")
        return recording

    return record


async def test_the_unscrubbed_recording_is_written_beside_the_fixture(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(r, "record", recorded(("event", "", execution_event(), 1.0)))
    output = tmp_path / "recording.json"
    outcome = r.Outcome()

    await to_file(output, account_id=ACCOUNT_ID, outcome=outcome)

    assert outcome.raw == raw_of(output) == tmp_path / "raw" / "recording.raw.json"
    assert outcome.fixture and output.exists()
    raw, account_id, login = r.decode_raw(outcome.raw.read_bytes())
    assert (account_id, login) == (ACCOUNT_ID, LOGIN)
    assert raw.timeline[0].message.position.positionId == POSITION_ID
    # Nothing is left of the temporary files the two were written through.
    assert sorted(path.name for path in tmp_path.rglob("*") if path.is_file()) == [
        "recording.json",
        "recording.raw.json",
    ]


async def test_a_run_that_recorded_no_message_writes_nothing(tmp_path, monkeypatch) -> None:
    """Notes alone are not a recording, however the run ended."""
    monkeypatch.setattr(r, "record", recorded(("marker", "waiting for the market", None, 1.0)))
    output = tmp_path / "recording.json"
    outcome = r.Outcome()

    await to_file(output, account_id=ACCOUNT_ID, outcome=outcome)

    assert not output.exists() and not raw_of(output).exists()
    assert (outcome.recording, outcome.raw, outcome.fixture) == (None, None, False)


async def test_a_refused_fixture_is_not_written_and_the_raw_recording_is(
    tmp_path,
    monkeypatch,
) -> None:
    note = "pasted the access-token by mistake"
    monkeypatch.setattr(
        r,
        "record",
        recorded(("event", "", execution_event(), 1.0), ("marker", note, None, 2.0)),
    )
    output = tmp_path / "recording.json"
    outcome = r.Outcome()

    with pytest.raises(r.record_fixtures.ScrubError):
        await to_file(output, account_id=ACCOUNT_ID, secrets=("access-token",), outcome=outcome)

    assert not output.exists()
    assert (outcome.raw, outcome.fixture) == (raw_of(output), False)
    raw, _, _ = r.decode_raw(raw_of(output).read_bytes())
    assert [entry.note for entry in raw.timeline] == ["", note]


async def test_a_fixture_that_cannot_be_built_leaves_the_raw_recording(
    tmp_path,
    monkeypatch,
) -> None:
    """Any failure, not only a refusal by the check: here the scrubbing itself breaks."""

    def broken(*_args, **_kwargs):
        raise ValueError("no such field")

    monkeypatch.setattr(r, "record", recorded(("event", "", execution_event(), 1.0)))
    monkeypatch.setattr(r, "encode_recording", broken)
    output = tmp_path / "recording.json"

    with pytest.raises(ValueError, match="no such field"):
        await to_file(output, account_id=ACCOUNT_ID)

    assert not output.exists()
    assert raw_of(output).exists()


async def test_an_unwritten_fixture_keeps_the_error_the_run_ended_with(
    tmp_path,
    monkeypatch,
) -> None:
    error = CTraderTimeoutError("no response")

    async def failing(*, recording: r.Recording, **_kwargs) -> r.Recording:
        recording.add("event", "", execution_event(), 0.5)
        recording.add("marker", "pasted the access-token by mistake", None, 1.0, typed=True)
        raise error

    monkeypatch.setattr(r, "record", failing)
    output = tmp_path / "recording.json"
    outcome = r.Outcome()

    with pytest.raises(r.record_fixtures.ScrubError) as raised:
        await to_file(output, account_id=ACCOUNT_ID, secrets=("access-token",), outcome=outcome)

    assert raised.value.__cause__ is error
    assert outcome.run_error is error
    assert not output.exists()
    raw, _, _ = r.decode_raw(raw_of(output).read_bytes())
    assert raw.timeline[-1].note == "run failed: CTraderTimeoutError"


async def test_a_raw_recording_that_cannot_be_written_does_not_cost_the_fixture(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(r, "record", recorded(("event", "", execution_event(), 1.0)))
    output = tmp_path / "recording.json"
    # A file where the raw directory should be: nothing can be written into it.
    blocked = tmp_path / "blocked"
    blocked.write_bytes(b"")
    outcome = r.Outcome()

    with pytest.raises(OSError):
        await to_file(output, account_id=ACCOUNT_ID, raw_dir=blocked, outcome=outcome)

    assert (outcome.raw, outcome.fixture) == (None, True)
    (event,) = r.decode_recording(output.read_bytes())["timeline"]
    assert event["message"].position.positionId == 5_000_001


async def test_a_write_that_fails_midway_leaves_the_earlier_files_whole(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(r, "record", recorded(("event", "", execution_event(), 1.0)))
    output = tmp_path / "recording.json"
    raw_of(output).parent.mkdir()
    output.write_bytes(b"an earlier fixture")
    raw_of(output).write_bytes(b"an earlier raw recording")
    real_fdopen = os.fdopen

    class HalfWritten:
        def __init__(self, handle) -> None:
            self._handle = handle

        def __enter__(self):
            return self

        def __exit__(self, *_exc) -> None:
            self._handle.close()

        def write(self, data: bytes) -> None:
            self._handle.write(data[: len(data) // 2])
            raise OSError("no space left on device")

    def fdopen(fd, mode="r", *args, **kwargs):
        handle = real_fdopen(fd, mode, *args, **kwargs)
        return HalfWritten(handle) if mode == "wb" else handle

    monkeypatch.setattr(r.os, "fdopen", fdopen)
    outcome = r.Outcome()

    with pytest.raises(OSError, match="no space left"):
        await to_file(output, account_id=ACCOUNT_ID, outcome=outcome)

    assert output.read_bytes() == b"an earlier fixture"
    assert raw_of(output).read_bytes() == b"an earlier raw recording"
    assert sorted(path.name for path in tmp_path.rglob("*") if path.is_file()) == [
        "recording.json",
        "recording.raw.json",
    ]
    # Something was recorded, and the report must not say otherwise.
    assert outcome.recording is not None
    assert (outcome.raw, outcome.fixture) == (None, False)


def env_file(tmp_path, *, without: str = "") -> pathlib.Path:
    values = {
        "CTRADER_CLIENT_ID": "client-id",
        "CTRADER_CLIENT_SECRET": "client-secret",
        "CTRADER_ACCESS_TOKEN": "access-token",
    }
    values.pop(without, None)
    path = tmp_path / "env"
    path.write_text("".join(f"{key}={value}\n" for key, value in values.items()), encoding="utf-8")
    return path


def rescrub_args(tmp_path, raw: pathlib.Path, *extra: str, without: str = "") -> list[str]:
    output = tmp_path / "rebuilt.json"
    env = env_file(tmp_path, without=without)
    return ["--rescrub", str(raw), "--output", str(output), "--env-file", str(env), *extra]


async def test_a_raw_recording_is_rescrubbed_into_the_same_fixture(tmp_path, capsys) -> None:
    server = venue()
    await server.start()
    stop, markers = asyncio.Event(), asyncio.Queue()
    statuses: list[str] = []
    output = tmp_path / "recording.json"
    outcome = r.Outcome()
    task = asyncio.create_task(
        to_file(
            output,
            outcome=outcome,
            **settings(server, stop, markers, status=statuses.append),
        ),
    )
    try:
        await wait_until(lambda: statuses == [r.STATUS_STARTED])
        # Typed notes that read like the recorder's own markers.
        markers.put_nowait(f"market buy, position {POSITION_ID}")
        markers.put_nowait("closing half the position")
        markers.put_nowait("connection lost on my side")
        await server.push(execution_event(label="my robot"))
        await wait_until(lambda: len(statuses) >= 2)
        stop.set()
        await asyncio.wait_for(task, 10)
    finally:
        task.cancel()
        await server.stop()

    # As typed notes they are counted, and not reported as problems of the run.
    live = r.summary(outcome.recording)
    assert live == ["1 events, 4 snapshots, 3 notes"]

    assert r.main(rescrub_args(tmp_path, raw_of(output))) == 0

    again = tmp_path / "rebuilt.json"
    assert again.read_bytes() == output.read_bytes()
    kinds = [item["kind"] for item in r.decode_recording(again.read_bytes())["timeline"]]
    assert kinds.count("event") == 1 and kinds.count("marker") == 3
    # Rebuilt from the raw file, the summary is the same: which notes were typed is kept there.
    assert capsys.readouterr().out.splitlines() == [f"Recording written to {again}", *live]


def test_describe_mode_needs_no_credentials_and_prints_the_timeline(tmp_path, capsys) -> None:
    data, _ = r.encode_recording(
        recording_of(execution_event()), account_id=ACCOUNT_ID, login=LOGIN
    )
    path = tmp_path / "recording.json"
    path.write_bytes(data)

    assert r.main(["--describe", str(path)]) == 0

    assert "ORDER_FILLED" in capsys.readouterr().out


def raw_file(tmp_path, *notes: str) -> pathlib.Path:
    recording = recording_of(execution_event(label="my robot"))
    for note in notes:
        recording.add("marker", note, None, 5.0, typed=True)
    path = tmp_path / "session.raw.json"
    path.write_bytes(r.encode_raw(recording, account_id=ACCOUNT_ID, login=LOGIN))
    return path


def test_describe_never_prints_an_unscrubbed_recording(tmp_path, capsys) -> None:
    raw = raw_file(tmp_path, "moved the stop")

    assert r.main(["--describe", str(raw)]) == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "--rescrub" in captured.err
    for private in (str(ACCOUNT_ID), str(LOGIN), str(ORDER_ID), "my robot", "moved the stop"):
        assert private not in captured.err


def test_recording_needs_a_trader_login(capsys) -> None:
    with pytest.raises(SystemExit):
        r.main([])
    assert "--trader-login" in capsys.readouterr().err


async def test_typed_lines_become_markers_and_q_stops(monkeypatch) -> None:
    typed = io.StringIO("moved the stop\n\n Q \nnever read\n")
    monkeypatch.setattr(sys, "stdin", typed)
    stop, markers = asyncio.Event(), asyncio.Queue()

    await asyncio.to_thread(r._read_keyboard, asyncio.get_running_loop(), stop, markers)
    await asyncio.wait_for(stop.wait(), 5)

    assert [markers.get_nowait() for _ in range(markers.qsize())] == ["moved the stop"]
    assert typed.readline() == "never read\n"


async def test_the_keyboard_reader_ends_at_end_of_input(monkeypatch) -> None:
    """A closed stdin returns an empty line for ever; the run goes on to its deadline."""
    monkeypatch.setattr(sys, "stdin", io.StringIO("moved the stop\n"))
    stop, markers = asyncio.Event(), asyncio.Queue()

    await asyncio.to_thread(r._read_keyboard, asyncio.get_running_loop(), stop, markers)
    await wait_until(lambda: not markers.empty())

    assert not stop.is_set()
    assert markers.get_nowait() == "moved the stop"


def test_a_blocked_keyboard_read_does_not_keep_the_process_alive() -> None:
    program = f"""
import asyncio, importlib.util, sys

spec = importlib.util.spec_from_file_location("record_execution", {str(_SCRIPT_PATH)!r})
r = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = r
spec.loader.exec_module(r)


async def main():
    r._start_keyboard(asyncio.get_running_loop(), asyncio.Event(), asyncio.Queue())
    await asyncio.sleep(0.2)


asyncio.run(main())
print("ended")
"""
    # stdin stays open and silent, so the reader is still blocked when the run is over.
    process = subprocess.Popen(
        [sys.executable, "-c", program],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert process.wait(timeout=60) == 0
        assert process.stdout.read().split() == [b"ended"]
        assert process.stderr.read() == b""
    finally:
        process.kill()
        process.wait(timeout=60)
        for pipe in (process.stdin, process.stdout, process.stderr):
            pipe.close()


def granted(*logins: int) -> list[om.ProtoOACtidTraderAccount]:
    return [
        om.ProtoOACtidTraderAccount(ctidTraderAccountId=ACCOUNT_ID + n, traderLogin=login)
        for n, login in enumerate(logins)
    ]


def test_the_account_is_the_one_with_the_trader_login() -> None:
    account = r._match_account(granted(LOGIN + 1, LOGIN), LOGIN)

    assert account.ctidTraderAccountId == ACCOUNT_ID + 1


@pytest.mark.parametrize("logins", [(LOGIN + 1,), (LOGIN, LOGIN)], ids=["none", "two"])
def test_an_unmatched_trader_login_is_reported_without_naming_it(logins) -> None:
    with pytest.raises(r.NoSuchAccount) as raised:
        r._match_account(granted(*logins), LOGIN)

    assert "trader login" in str(raised.value)
    assert not any(char.isdigit() for char in str(raised.value))


async def test_the_checklist_waits_for_the_recording_to_start(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    printed: dict[str, str] = {}

    async def resolved(*_args) -> tuple[str, int]:
        return "127.0.0.1", ACCOUNT_ID

    async def record_to_file(_output, *, status, **_kwargs) -> None:
        printed["before"] = capsys.readouterr().out
        status(r.STATUS_STARTED)
        printed["started"] = capsys.readouterr().out
        status(r.STATUS_LOST)

    monkeypatch.setattr(r, "_resolve_account", resolved)
    monkeypatch.setattr(r, "record_to_file", record_to_file)
    monkeypatch.setattr(r, "_start_keyboard", lambda *_args: None)
    args = argparse.Namespace(
        trader_login=LOGIN,
        minutes=1.0,
        output=tmp_path / "recording.json",
        raw_dir=tmp_path / "raw",
    )
    env = {
        "CTRADER_CLIENT_ID": "client-id",
        "CTRADER_CLIENT_SECRET": "client-secret",
        "CTRADER_ACCESS_TOKEN": "access-token",
    }

    await r._run(args, env, r.Outcome())

    assert "Connecting" in printed["before"]
    assert "Trade by hand" not in printed["before"]
    assert "Trade by hand" in printed["started"]
    assert capsys.readouterr().err == f"{r.STATUS_LOST}\n"


def session(*own: str, events: int = 0, typed: tuple[str, ...] = ()) -> r.Recording:
    """A recording of `events` events, the recorder's `own` markers and the `typed` notes."""
    recording = recording_of(*(execution_event(deal_id=DEAL_ID + n) for n in range(events)))
    for note in own:
        recording.add("marker", note, None, 9.0)
    for note in typed:
        recording.add("marker", note, None, 9.5, typed=True)
    return recording


def test_the_summary_ends_with_the_counts() -> None:
    lines = r.summary(session("reconnected", events=2, typed=("moved the stop",)))

    assert lines == ["2 events, 0 snapshots, 1 notes"]


def test_the_summary_says_in_words_that_no_event_was_recorded() -> None:
    (line,) = r.summary(session(typed=("moved the stop",)))

    assert line.startswith("0 events, 0 snapshots, 1 notes")
    assert "NO EVENTS" in line


def test_the_summary_lists_every_closing_problem_and_dropped_field() -> None:
    problems = [
        "closing list refused: deals INVALID_REQUEST",
        "closing list truncated: orders",
        "closing requests failed: CTraderTimeoutError",
        "closing requests skipped: not connected",
    ]
    recording = session(*problems, events=1, typed=("moved the stop",))
    recording.add("event", "", with_unknown_field(execution_event(), b"private words"), 9.9)

    lines = r.summary(recording)

    assert lines == [
        *problems,
        "unknown fields dropped from: ProtoOAExecutionEvent",
        "2 events, 0 snapshots, 1 notes",
    ]


def test_a_typed_note_is_never_taken_for_a_problem_of_the_run() -> None:
    typed = ("closing half the position", "connection lost on my side")

    assert r.summary(session(events=1, typed=typed)) == ["1 events, 0 snapshots, 2 notes"]


@pytest.mark.parametrize(
    ("raw_kept", "fixture_written", "expected"),
    [
        (
            True,
            True,
            ["Recording written to {output}", "Unscrubbed copy, never to be committed: {raw}"],
        ),
        (
            False,
            True,
            ["Recording written to {output}", "The unscrubbed copy could NOT be written."],
        ),
        (
            True,
            False,
            [
                "The fixture was NOT written; {output} is untouched.",
                "Unscrubbed copy, never to be committed: {raw}",
                "Rebuild the fixture from it with: --rescrub {raw}",
            ],
        ),
        (
            False,
            False,
            [
                "The fixture was NOT written; {output} is untouched.",
                "The unscrubbed copy could NOT be written.",
            ],
        ),
    ],
    ids=["both", "only the fixture", "only the raw recording", "neither"],
)
def test_what_is_on_disk_is_reported_as_it_is_and_always_with_the_counts(
    tmp_path,
    raw_kept,
    fixture_written,
    expected,
) -> None:
    output, raw = tmp_path / "recording.json", tmp_path / "raw" / "recording.raw.json"
    outcome = r.Outcome(
        recording=session(events=1),
        raw=raw if raw_kept else None,
        fixture=fixture_written,
    )

    lines = r._outcome_lines(outcome, output)

    assert lines == [
        *(line.format(output=output, raw=raw) for line in expected),
        "1 events, 0 snapshots, 0 notes",
    ]


def main_run(tmp_path, monkeypatch, run, *extra: str) -> int:
    """`main` in recording mode with the run itself replaced by `run`; nothing connects."""
    monkeypatch.setattr(r, "_run", run)
    return r.main(
        [
            "--trader-login",
            str(LOGIN),
            "--env-file",
            str(env_file(tmp_path)),
            "--output",
            str(tmp_path / "recording.json"),
            "--raw-dir",
            str(tmp_path / "raw"),
            *extra,
        ],
    )


def ending_with(outcome: BaseException):
    async def run(_args, _env, _outcome) -> None:
        raise outcome

    return run


NOTHING = "Nothing was recorded, and nothing was written.\n"


def test_a_venue_refusal_is_reported_by_its_code_only(tmp_path, monkeypatch, capsys) -> None:
    refused = CTraderRequestError("RET_ACCOUNT_DISABLED", f"account {ACCOUNT_ID} is disabled")

    assert main_run(tmp_path, monkeypatch, ending_with(refused)) == 1

    captured = capsys.readouterr()
    assert captured.err == "error: CTraderRequestError RET_ACCOUNT_DISABLED\n"
    assert captured.out == NOTHING


def test_an_unexpected_error_is_reported_by_its_type_only(tmp_path, monkeypatch, capsys) -> None:
    error = ValueError(f"account {ACCOUNT_ID}")

    assert main_run(tmp_path, monkeypatch, ending_with(error)) == 1

    assert capsys.readouterr().err == "error: ValueError\n"


def test_an_unmatched_trader_login_is_explained(tmp_path, monkeypatch, capsys) -> None:
    unmatched = r.NoSuchAccount("the access token grants no account with that trader login")

    assert main_run(tmp_path, monkeypatch, ending_with(unmatched)) == 1

    assert capsys.readouterr().err == f"error: {unmatched}\n"


def test_an_interrupt_before_the_recording_says_nothing_was_written(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    assert main_run(tmp_path, monkeypatch, ending_with(KeyboardInterrupt())) == 130

    captured = capsys.readouterr()
    assert (captured.out, captured.err) == (NOTHING, "")


def test_an_interrupt_after_the_recording_prints_its_summary(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    async def interrupted(args, _env, outcome) -> None:
        outcome.recording = session(events=1, typed=("moved the stop",))
        outcome.raw = r.raw_path(args.output, args.raw_dir)
        outcome.fixture = True
        raise KeyboardInterrupt

    assert main_run(tmp_path, monkeypatch, interrupted) == 130

    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out.splitlines() == [
        f"Recording written to {tmp_path / 'recording.json'}",
        f"Unscrubbed copy, never to be committed: {tmp_path / 'raw' / 'recording.raw.json'}",
        "1 events, 0 snapshots, 1 notes",
    ]


def test_a_recording_with_no_event_says_so_last(tmp_path, monkeypatch, capsys) -> None:
    async def silent(_args, _env, outcome) -> None:
        outcome.recording = session("closing requests skipped: not connected")
        outcome.fixture = True

    assert main_run(tmp_path, monkeypatch, silent) == 0

    lines = capsys.readouterr().out.splitlines()
    assert "closing requests skipped: not connected" in lines
    assert lines[-1].startswith("0 events, 0 snapshots, 0 notes") and "NO EVENTS" in lines[-1]


def test_an_unwritten_fixture_is_reported_with_the_raw_file_and_how_to_rebuild(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    raw = tmp_path / "raw" / "recording.raw.json"

    async def unwritten(_args, _env, outcome) -> None:
        outcome.recording = session(events=2)
        outcome.raw = raw
        outcome.run_error = CTraderRequestError("INVALID_REQUEST", f"account {ACCOUNT_ID}")
        raise r.record_fixtures.ScrubError(f"found {ACCOUNT_ID}") from outcome.run_error

    assert main_run(tmp_path, monkeypatch, unwritten) == 1

    captured = capsys.readouterr()
    assert captured.err.splitlines() == [
        "error: ScrubError",
        "the run itself had failed: CTraderRequestError INVALID_REQUEST",
    ]
    assert f"--rescrub {raw}" in captured.out
    assert captured.out.splitlines()[-1] == "2 events, 0 snapshots, 0 notes"
    assert str(ACCOUNT_ID) not in captured.out + captured.err


@pytest.mark.parametrize("existing", ["recording.json", "raw/recording.raw.json"])
def test_an_existing_file_is_not_replaced_without_being_asked(
    tmp_path,
    monkeypatch,
    capsys,
    existing,
) -> None:
    kept = tmp_path / existing
    kept.parent.mkdir(exist_ok=True)
    kept.write_bytes(b"an earlier session")
    runs: list[object] = []

    async def run(args, _env, _outcome) -> None:
        runs.append(args)

    assert main_run(tmp_path, monkeypatch, run) == 1

    # Refused before the run, so before any connection.
    assert runs == []
    assert kept.read_bytes() == b"an earlier session"
    assert capsys.readouterr().err == (
        f"error: {kept} already exists; pass --overwrite to replace it\n"
    )

    assert main_run(tmp_path, monkeypatch, run, "--overwrite") == 0
    assert len(runs) == 1


def test_a_missing_output_directory_is_refused_before_the_run(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    runs: list[object] = []

    async def run(args, _env, _outcome) -> None:
        runs.append(args)

    output = tmp_path / "missing" / "recording.json"

    code = main_run(tmp_path, monkeypatch, run, "--output", str(output))

    assert (code, runs) == (1, [])
    assert capsys.readouterr().err == f"error: cannot write a file in {output.parent}\n"


def test_rescrub_writes_the_fixture_and_its_summary(tmp_path, capsys) -> None:
    raw = raw_file(tmp_path, "moved the stop")

    assert r.main(rescrub_args(tmp_path, raw)) == 0

    output = tmp_path / "rebuilt.json"
    (event, _) = r.decode_recording(output.read_bytes())["timeline"]
    assert event["message"].ctidTraderAccountId == r.record_fixtures.FAKE_ACCOUNT_ID
    assert event["message"].position.tradeData.label == r.SCRUBBED_TEXT
    assert capsys.readouterr().out.splitlines() == [
        f"Recording written to {output}",
        "1 events, 0 snapshots, 1 notes",
    ]


def test_rescrub_does_not_replace_a_fixture_without_being_asked(tmp_path, capsys) -> None:
    raw = raw_file(tmp_path)
    output = tmp_path / "rebuilt.json"
    output.write_bytes(b"an earlier fixture")

    assert r.main(rescrub_args(tmp_path, raw)) == 1

    assert output.read_bytes() == b"an earlier fixture"
    captured = capsys.readouterr()
    assert "already exists" in captured.err
    assert captured.out == "The fixture was NOT written.\n"

    assert r.main(rescrub_args(tmp_path, raw, "--overwrite")) == 0
    assert output.read_bytes() != b"an earlier fixture"


def test_rescrub_checks_the_fixture_against_the_secrets_in_the_env_file(tmp_path, capsys) -> None:
    raw = raw_file(tmp_path, "pasted the access-token by mistake")

    assert r.main(rescrub_args(tmp_path, raw)) == 1

    output = tmp_path / "rebuilt.json"
    assert not output.exists()
    captured = capsys.readouterr()
    assert captured.err == "error: ScrubError\n"
    # What the raw file holds is still reported, and nothing claims it was not recorded.
    assert captured.out.splitlines() == [
        f"The fixture was NOT written; {output} is untouched.",
        "1 events, 0 snapshots, 1 notes",
    ]
    assert "access-token" not in captured.out + captured.err


def test_rescrub_needs_the_same_env_keys_as_a_recording(tmp_path, capsys) -> None:
    raw = raw_file(tmp_path)

    assert r.main(rescrub_args(tmp_path, raw, without="CTRADER_ACCESS_TOKEN")) == 1

    assert not (tmp_path / "rebuilt.json").exists()
    assert capsys.readouterr().err == "error: the env file lacks CTRADER_ACCESS_TOKEN\n"


def test_a_recording_needs_its_env_keys_before_anything_connects(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    runs: list[object] = []

    async def run(args, _env, _outcome) -> None:
        runs.append(args)

    (tmp_path / "other").mkdir()
    env = env_file(tmp_path / "other", without="CTRADER_CLIENT_SECRET")

    assert main_run(tmp_path, monkeypatch, run, "--env-file", str(env)) == 1

    assert runs == []
    assert capsys.readouterr().err == "error: the env file lacks CTRADER_CLIENT_SECRET\n"
