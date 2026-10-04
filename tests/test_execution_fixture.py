"""The committed execution recording is scrubbed and well-formed.

It was made on a real account, so these checks run on every test run, not once at recording.
"""

from __future__ import annotations

from itertools import pairwise

from google.protobuf.descriptor import FieldDescriptor

from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from tests.fixtures import FAKE_ACCOUNT_ID, load_execution_recording

RECORDING = load_execution_recording()
_FAKE_RANGES = {
    "positionId": range(5_000_001, 6_000_000),
    "orderId": range(6_000_001, 7_000_000),
    "dealId": range(7_000_001, 8_000_000),
    "balanceHistoryId": range(8_000_001, 9_000_000),
}
_TEXT_FIELDS = {"label", "comment", "clientOrderId", "externalNote"}


def messages():
    for item in RECORDING["timeline"]:
        if item["message"] is not None:
            yield item["message"]
    for items in RECORDING["closing"].values():
        yield from items


def fields(message):
    for descriptor, value in message.ListFields():
        if descriptor.type == FieldDescriptor.TYPE_MESSAGE:
            items = value if descriptor.label == FieldDescriptor.LABEL_REPEATED else (value,)
            for item in items:
                yield from fields(item)
        elif descriptor.label == FieldDescriptor.LABEL_REPEATED:
            for item in value:
                yield descriptor.name, item
        else:
            yield descriptor.name, value


def balance_changes() -> list[tuple[int, int, int]]:
    """`(balanceVersion, balance after, change)` for every deposit and closing deal listed."""
    found = {}
    for response in RECORDING["closing"]["cash_flow"]:
        for operation in response.depositWithdraw:
            found[operation.balanceVersion] = (operation.balance, operation.delta)
    for response in RECORDING["closing"]["account_deals"]:
        for deal in response.deal:
            if deal.HasField("closePositionDetail"):
                d = deal.closePositionDetail
                change = d.grossProfit + d.swap + d.commission + d.pnlConversionFee
                found[d.balanceVersion] = (d.balance, change)
    return sorted((version, *values) for version, values in found.items())


def test_the_recording_has_a_timeline_with_events_and_markers() -> None:
    kinds = {item["kind"] for item in RECORDING["timeline"]}
    assert {"event", "snapshot", "marker"} <= kinds
    times = [item["t"] for item in RECORDING["timeline"]]
    assert times == sorted(times)
    assert any(isinstance(m, oa.ProtoOAExecutionEvent) for m in messages())


def test_every_identifier_in_the_recording_is_a_fake_one() -> None:
    for message in messages():
        for name, value in fields(message):
            if name in ("ctidTraderAccountId", "ctidTraderAccountIds"):
                assert value == FAKE_ACCOUNT_ID
            elif name in _FAKE_RANGES:
                assert value in _FAKE_RANGES[name], (name, value)
            elif name == "traderLogin":
                raise AssertionError("a trader login survived the scrub")


def test_no_free_text_survives() -> None:
    for message in messages():
        for name, value in fields(message):
            if name in _TEXT_FIELDS:
                assert value == "scrubbed", name


def test_hidden_balances_keep_their_arithmetic() -> None:
    # Amounts are shifted, not zeroed: each balance is the one before it plus the change.
    changes = balance_changes()
    assert len(changes) >= 2
    _, first_balance, first_change = changes[0]
    assert first_balance == first_change
    for (_, before, _), (_, after, change) in pairwise(changes):
        assert after == before + change
    assert all(balance != 0 for _, balance, _ in changes)
