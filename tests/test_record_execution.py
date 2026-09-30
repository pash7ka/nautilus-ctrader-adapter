"""Offline tests for `scripts/record_execution.py`.

The script is loaded by file path and registered in `sys.modules`, the way
`tests/test_verify_live_data.py` loads its script. What only a live session can show - the real
sequences - is not covered here; this file covers the recorder's own guarantees.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import pathlib
import sys

import pytest

from nautilus_ctrader.common.rate_limit import RateLimiter
from nautilus_ctrader.constants import BUCKET_DEFAULT, BUCKET_HISTORICAL
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om

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
    assert (
        decoded["timeline"][0]["message"].ctidTraderAccountId == r.record_fixtures.FAKE_ACCOUNT_ID
    )
    assert str(ACCOUNT_ID).encode() not in data and str(POSITION_ID).encode() not in data


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


def test_describe_prints_no_real_identifier() -> None:
    data, _ = r.encode_recording(
        recording_of(execution_event()), account_id=ACCOUNT_ID, login=LOGIN
    )

    text = r.describe(data)

    assert "ORDER_FILLED" in text
    for real in (ACCOUNT_ID, LOGIN, POSITION_ID, ORDER_ID, DEAL_ID):
        assert str(real) not in text
