"""The client delivers the model's records in the broker's order, behind its own queued events.

Nautilus queues an event but applies a report at once. A report or an activity that follows an
event the client sent waits until Nautilus has applied that event, and so does every record
after it.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.identifiers import ClientOrderId

from nautilus_ctrader.activity import ACCOUNT_ACTIVITY_TOPIC
from nautilus_ctrader.common.venue_records import Action, Activity, ActivityKind
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.account_venue import hold_account_auth
from tests.execution_replay import make_event, make_order
from tests.execution_venue import ENTRY, Harness, harness, push, submitted
from tests.polling import wait_until

UNLOADED = 1


def unloaded_order(order_id: int) -> object:
    """A trader's order on a symbol the node has not loaded: an activity alone."""
    order = make_order(order_id, 5_900_001, order_type=om.LIMIT, limit=1.1, symbol=UNLOADED)
    return make_event(om.ORDER_ACCEPTED, order)


async def never_applied(h: Harness) -> None:
    """An event of the node's that Nautilus will never apply, as when it refuses one."""
    await submitted(h)
    h.client._unapplied[UUID4()] = ClientOrderId(ENTRY)


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
