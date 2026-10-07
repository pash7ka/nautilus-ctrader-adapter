"""The committed execution recordings are scrubbed and well-formed.

It was made on a real account, so these checks run on every test run, not once at recording.
"""

from __future__ import annotations

from itertools import pairwise

import pytest
from google.protobuf.descriptor import FieldDescriptor

from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from tests.fixtures import FAKE_ACCOUNT_ID, load_execution_recording, load_stage2_recording

RECORDINGS = {"m3_execution": load_execution_recording(), "m3_stage2": load_stage2_recording()}
# The scrubber numbers each kind of id from its own base, in order of first appearance.
_FAKE_BASES = {
    "positionId": 5_000_000,
    "orderId": 6_000_000,
    "dealId": 7_000_000,
    "balanceHistoryId": 8_000_000,
}
# The scrubbed free text, the venue's own name for a symbol's unit, and an empty unit name.
_ALLOWED_TEXT = {"scrubbed", "Contracts", ""}


@pytest.fixture(params=sorted(RECORDINGS))
def recording(request) -> dict:
    return RECORDINGS[request.param]


def messages(recording: dict):
    for item in recording["timeline"]:
        if item["message"] is not None:
            yield item["message"]
    for items in recording["closing"].values():
        yield from items


def fields(message):
    for descriptor, value in message.ListFields():
        if descriptor.type == FieldDescriptor.TYPE_MESSAGE:
            items = value if descriptor.is_repeated else (value,)
            for item in items:
                yield from fields(item)
        elif descriptor.is_repeated:
            for item in value:
                yield descriptor.name, item
        else:
            yield descriptor.name, value


def balance_changes(recording: dict) -> list[tuple[int, int, int]]:
    """`(balanceVersion, balance after, change)` for every deposit and closing deal listed."""
    found = {}
    for response in recording["closing"]["cash_flow"]:
        for operation in response.depositWithdraw:
            found[operation.balanceVersion] = (operation.balance, operation.delta)
    for response in recording["closing"]["account_deals"]:
        for deal in response.deal:
            if deal.HasField("closePositionDetail"):
                d = deal.closePositionDetail
                change = d.grossProfit + d.swap + d.commission + d.pnlConversionFee
                found[d.balanceVersion] = (d.balance, change)
    return sorted((version, *values) for version, values in found.items())


def test_the_recording_has_a_timeline_with_events_and_markers(recording: dict) -> None:
    kinds = {item["kind"] for item in recording["timeline"]}
    assert {"event", "snapshot", "marker"} <= kinds
    # Events only: a snapshot is stamped when its first answer arrived, so it may sit after a
    # later event.
    times = [item["t"] for item in recording["timeline"] if item["kind"] == "event"]
    assert times == sorted(times)
    assert any(isinstance(m, oa.ProtoOAExecutionEvent) for m in messages(recording))


def test_every_identifier_in_the_recording_is_a_fake_one(recording: dict) -> None:
    for message in messages(recording):
        for name, value in fields(message):
            if name in ("ctidTraderAccountId", "ctidTraderAccountIds"):
                assert value == FAKE_ACCOUNT_ID
            elif name == "traderLogin":
                raise AssertionError("a trader login survived the scrub")


def test_each_kind_of_id_runs_from_its_fake_base_without_gaps(recording: dict) -> None:
    seen: dict[str, set[int]] = {name: set() for name in _FAKE_BASES}
    for message in messages(recording):
        for name, value in fields(message):
            if name in seen:
                seen[name].add(value)
    for name, values in seen.items():
        base = _FAKE_BASES[name]
        assert values == set(range(base + 1, base + 1 + len(values))), name


def test_no_free_text_survives(recording: dict) -> None:
    for message in messages(recording):
        for name, value in fields(message):
            if isinstance(value, str | bytes):
                assert value in _ALLOWED_TEXT, name


def test_no_field_unknown_to_the_bindings_survives(recording: dict) -> None:
    for message in messages(recording):
        known = type(message)()
        known.CopyFrom(message)
        known.DiscardUnknownFields()
        assert known.SerializeToString() == message.SerializeToString(), type(message).__name__


def test_hidden_balances_keep_their_arithmetic(recording: dict) -> None:
    # Amounts are shifted, not zeroed: each balance is the one before it plus the change.
    changes = balance_changes(recording)
    assert len(changes) >= 2
    _, first_balance, first_change = changes[0]
    assert first_balance == first_change
    for (_, before, _), (_, after, change) in pairwise(changes):
        assert after == before + change
    assert all(balance != 0 for _, balance, _ in changes)


def test_every_balance_in_the_recording_is_one_of_the_chain(recording: dict) -> None:
    chain = {balance for _, balance, _ in balance_changes(recording)}
    for message in messages(recording):
        for name, value in fields(message):
            if name == "balance":
                assert value in chain, value
