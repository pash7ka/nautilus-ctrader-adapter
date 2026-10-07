"""The client delivers the model's records in the broker's order, behind its own queued events.

Nautilus queues an event but applies a report at once. A report or an activity that follows an
event the client sent waits until Nautilus has applied that event, and so does every record
after it.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal

import pytest
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.identifiers import ClientOrderId

from nautilus_ctrader import execution
from nautilus_ctrader.activity import ACCOUNT_ACTIVITY_TOPIC
from nautilus_ctrader.common.session import SessionState
from nautilus_ctrader.common.venue_records import (
    Action,
    Activity,
    ActivityKind,
    OrderEvent,
    OrderEventKind,
)
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.account_venue import hold_account_auth
from tests.execution_replay import make_event, make_order
from tests.execution_venue import (
    CLOSE,
    ENTRY,
    FIRST_EVENTS,
    OURS,
    STOP,
    STRATEGY_ID,
    US100_ID,
    US100_SYMBOL_ID,
    ExecutionVenue,
    Harness,
    close_sent,
    harness,
    node_close_events,
    on_us100,
    our_market_position,
    push,
    started,
    status,
    submitted,
)
from tests.polling import wait_until

UNLOADED = 1


def unloaded_order(order_id: int) -> object:
    """A trader's order on a symbol the node has not loaded: an activity alone."""
    order = make_order(order_id, 5_900_001, order_type=om.LIMIT, limit=1.1, symbol=UNLOADED)
    return make_event(om.ORDER_ACCEPTED, order)


async def never_applied(h: Harness) -> None:
    """An event of the node's that Nautilus will never apply, as when it refuses one."""
    await submitted(h)
    h.client._unapplied[UUID4()] = (ClientOrderId(ENTRY), h.client._loop.time())


async def test_an_activity_behind_an_event_never_applied_is_published_after_a_warning() -> None:
    async with harness() as h:
        await never_applied(h)

        await push(h, unloaded_order(6_900_001), unloaded_order(6_900_002))
        assert h.activity == []
        await wait_until(lambda: len(h.activity) == 2)

        assert [a.ts_event for a in h.activity] == [1_000_000, 1_000_000]
        assert any("did not apply 1 order event" in line for line in h.logger.warnings())
        assert h.client._unapplied == {}
        assert h.logger.errors() == []


async def test_records_held_when_the_client_detaches_are_dropped() -> None:
    async with harness() as h:
        await never_applied(h)
        await push(h, unloaded_order(6_900_001))
        assert len(h.client._outbox) == 1

        await h.client._disconnect()

        assert len(h.client._outbox) == 0
        assert h.client._outbox_task is None
        assert h.activity == []


async def test_records_held_before_a_reconnect_reach_nautilus_before_its_mass_status() -> None:
    async with harness() as h:
        mass_statuses_seen: list[int] = []
        h.client._msgbus.subscribe(
            topic=ACCOUNT_ACTIVITY_TOPIC,
            handler=lambda _a: mass_statuses_seen.append(len(h.mass_statuses)),
        )
        held = hold_account_auth(h.server)
        await h.server.drop_connections()
        await asyncio.wait_for(held.arrived.wait(), timeout=10)

        # Held just before the rebuild starts, so it is still held when it does.
        await never_applied(h)
        h.client._handle_records(
            [
                Activity(
                    ActivityKind.UNLOADED_SYMBOL,
                    UNLOADED,
                    "order",
                    "BUY",
                    Decimal(1),
                    Action.OPENED,
                    1,
                )
            ],
        )
        assert h.activity == []
        await held.stop_holding()
        await wait_until(lambda: len(h.mass_statuses) == 1, timeout_secs=10)

        assert mass_statuses_seen == [0]


def unloaded_activity() -> Activity:
    return Activity(
        ActivityKind.UNLOADED_SYMBOL, UNLOADED, "order", "BUY", Decimal(1), Action.OPENED, 1
    )


async def test_a_detach_while_a_reconnect_waits_for_held_records_leaves_the_session_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Long enough for the detach to come while the rebuild waits.
    monkeypatch.setattr(execution, "_EVENT_WAIT_SECS", 5.0)
    async with harness() as h:
        # Another user of the account, which must get its connection back.
        await h.account.connect()
        session = h.account.session
        held = hold_account_auth(h.server)
        await h.server.drop_connections()
        await asyncio.wait_for(held.arrived.wait(), timeout=10)
        await never_applied(h)
        h.client._handle_records([unloaded_activity()])
        await held.stop_holding()
        await wait_until(lambda: session.state is SessionState.RESTORING, timeout_secs=10)

        h.client._detach()
        await wait_until(lambda: session.is_ready, timeout_secs=10)

        assert not session._supervisor.done()
        assert h.activity == []
        assert h.logger.errors() == []
        await h.account.disconnect()


async def test_an_event_refused_long_before_holds_back_no_later_activity() -> None:
    async with harness() as h:
        await submitted(h)
        h.client.generate_order_rejected(STRATEGY_ID, US100_ID, ClientOrderId(ENTRY), "refused", 0)
        await wait_until(lambda: status(h, ENTRY) == OrderStatus.REJECTED)
        # Nautilus refuses these: the entry was rejected.
        await push(h, *FIRST_EVENTS[:2])
        await asyncio.sleep(1.0)

        await push(h, unloaded_order(6_900_001))

        assert len(h.activity) == 1
        assert not any("did not apply" in line for line in h.logger.warnings())


async def test_the_events_tracked_stay_few_while_only_the_nodes_orders_trade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(execution, "_EVENT_WAIT_SECS", 0.01)
    async with harness() as h:
        await submitted(h)
        await push(h, *FIRST_EVENTS[:3])
        await wait_until(lambda: status(h, STOP) == OrderStatus.ACCEPTED)

        for step in range(20):
            trigger = Decimal("85197.20") + step
            h.client._handle_records(
                [OrderEvent(OrderEventKind.UPDATED, "6000001-SL", STOP, 1, trigger_price=trigger)],
            )
            await asyncio.sleep(0.02)

        assert len(h.client._unapplied) <= 1


async def test_a_close_held_behind_an_event_is_matched_at_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(execution, "_EVENT_WAIT_SECS", 5.0)
    now = int(time.time() * 1000)
    venue = ExecutionVenue()
    our_market_position(venue, opened=now - 120_000)
    async with harness(execution_venue=venue) as h:
        await started(h)
        closing = await close_sent(h)
        h.client._unapplied[UUID4()] = (ClientOrderId(CLOSE), h.client._loop.time())
        await push(h, unloaded_order(6_900_001))
        accepted, _filled = node_close_events(closed=now)

        await push(h, accepted)

        # Held for Nautilus, yet the close is no longer in flight for the model.
        assert len(h.client._outbox) == 2
        assert h.client._operations.close_position(CLOSE) is None
        # A trader's close of the same volume, made right after, is not taken for the node's.
        trader = make_order(
            6_300_003, OURS, side=om.SELL, closing=True, utc=now + 1, symbol=US100_SYMBOL_ID
        )
        trader.tradeData.openTimestamp = now + 1
        await push(h, *on_us100([make_event(om.ORDER_ACCEPTED, trader)]))
        assert h.client._book.known_closes() == {6_300_002: CLOSE}

        await h.client._disconnect()
        await asyncio.wait_for(closing, timeout=10)
