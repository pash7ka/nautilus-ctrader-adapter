"""A fake venue for the execution client, and a Nautilus execution engine around the client.

The first recorded execution session was traded on a symbol the market-data fixture holds no
reference data for. `on_us100` moves its messages to `US100.cash`, which has the same price and
volume precision, and to the fake account; nothing else changes. The stage-2 session was traded
on EURUSD, which the fixture holds: `on_fake_account` only re-addresses its messages.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass

from google.protobuf.message import Message
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.common.factories import OrderFactory
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import SubmitOrder, SubmitOrderList
from nautilus_trader.execution.reports import ExecutionMassStatus
from nautilus_trader.live.config import LiveExecEngineConfig
from nautilus_trader.live.execution_engine import LiveExecutionEngine
from nautilus_trader.model.enums import OrderSide, OrderStatus
from nautilus_trader.model.identifiers import (
    ClientOrderId,
    InstrumentId,
    PositionId,
    StrategyId,
    Symbol,
    TraderId,
)
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.orders import OrderList
from nautilus_trader.portfolio.portfolio import Portfolio

from nautilus_ctrader.activity import ACCOUNT_ACTIVITY_TOPIC
from nautilus_ctrader.common import order_record
from nautilus_ctrader.common.account import CTraderAccountClient
from nautilus_ctrader.common.order_record import LegIds
from nautilus_ctrader.common.reconciliation import PositionHistory
from nautilus_ctrader.config import CTraderExecClientConfig
from nautilus_ctrader.constants import CTRADER_VENUE
from nautilus_ctrader.execution import CTraderExecutionClient
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.account_venue import (
    ACCOUNT_ID,
    RECORDED,
    TRADER_LOGIN,
    account_client,
    for_account,
    venue,
)
from tests.execution_replay import (
    FIRST,
    as_ours,
    events,
    first_n,
    history,
    make_deal,
    make_event,
    make_order,
    make_position,
    snapshot_at,
    window_deals,
)
from tests.fake_server import FakeCTraderServer
from tests.polling import wait_until
from tests.recording_logger import RecordingLogger

US100_ID = InstrumentId(Symbol("US100.cash"), CTRADER_VENUE)
US100_SYMBOL_ID = 275
EURUSD_ID = InstrumentId(Symbol("EURUSD"), CTRADER_VENUE)
EURUSD_SYMBOL_ID = 1
WEEK_MS = 604_800_000
TRADER_ID = TraderId("TESTER-001")
STRATEGY_ID = StrategyId("S-001")


def _orders(message: Message) -> list[om.ProtoOAOrder]:
    if isinstance(message, om.ProtoOAOrder):
        return [message]
    if isinstance(message, oa.ProtoOAExecutionEvent):
        return [message.order] if message.HasField("order") else []
    return list(message.order) if hasattr(message, "order") else []


def _positions(message: Message) -> list[om.ProtoOAPosition]:
    if isinstance(message, oa.ProtoOAExecutionEvent):
        return [message.position] if message.HasField("position") else []
    return list(message.position) if hasattr(message, "position") else []


def on_fake_account(messages: Iterable[Message]) -> list[Message]:
    """Copies of recorded messages, addressed to the fake account."""
    moved = []
    for message in messages:
        copy = type(message)()
        copy.CopyFrom(message)
        if hasattr(copy, "ctidTraderAccountId"):
            copy.ctidTraderAccountId = ACCOUNT_ID
        moved.append(copy)
    return moved


def shifted(messages: Iterable[Message], by_ms: int) -> list[Message]:
    """Copies of `messages` with every timestamp they carry `by_ms` later."""

    def shift(message: Message) -> None:
        for field, value in message.ListFields():
            if field.message_type is not None:
                for item in value if field.is_repeated else [value]:
                    shift(item)
            elif field.name.endswith("Timestamp"):
                setattr(message, field.name, value + by_ms)

    moved = []
    for message in messages:
        copy = type(message)()
        copy.CopyFrom(message)
        shift(copy)
        moved.append(copy)
    return moved


def on_us100(messages: Iterable[Message]) -> list[Message]:
    """Copies of recorded messages, orders and deals, on `US100.cash` and the fake account."""
    moved = []
    for copy in on_fake_account(messages):
        for order in _orders(copy):
            order.tradeData.symbolId = US100_SYMBOL_ID
        for position in _positions(copy):
            position.tradeData.symbolId = US100_SYMBOL_ID
        if isinstance(copy, oa.ProtoOAExecutionEvent) and copy.HasField("deal"):
            copy.deal.symbolId = US100_SYMBOL_ID
        if isinstance(copy, om.ProtoOADeal):
            copy.symbolId = US100_SYMBOL_ID
        moved.append(copy)
    return moved


def trader(**overrides) -> oa.ProtoOATraderRes:
    """The recorded trader on the fake account, with `overrides` set on it."""
    response = for_account(RECORDED["trader"][0], ACCOUNT_ID)
    for name, value in overrides.items():
        setattr(response.trader, name, value)
    return response


def _between(items: Iterable, request: Message, time_of) -> list:
    """`items` within the request's window, oldest first; a bound it does not carry is open."""
    low = request.fromTimestamp if request.HasField("fromTimestamp") else None
    high = request.toTimestamp if request.HasField("toTimestamp") else None
    return sorted(
        (
            item
            for item in items
            if (low is None or time_of(item) >= low) and (high is None or time_of(item) <= high)
        ),
        key=time_of,
    )


class ExecutionVenue:
    """The fake venue's account, read at each request so a test can change it between requests.

    The history lists answer oldest first, `page_size` items a page, and set `hasMore` past it.
    The cash-flow list has no pages and refuses a window over a week, as the schema states.
    A request whose payload type is in `fail` is answered with an error. `replies` holds each
    served payload type's answer, for a test that holds it back.

    A cancel or an amend of a pending order acts on the snapshot's orders and is answered as the
    broker answers it: the order cancelled or replaced, or an order error for an order it does not
    hold pending. Other order requests have no handler until a test registers one with
    `server.on`.
    """

    def __init__(self) -> None:
        self.server = venue()
        self.trader = trader()
        self.snapshot = oa.ProtoOAReconcileRes(ctidTraderAccountId=ACCOUNT_ID)
        self.position_orders: dict[int, list[om.ProtoOAOrder]] = {}
        self.position_deals: dict[int, list[om.ProtoOADeal]] = {}
        self.deals: list[om.ProtoOADeal] = []
        self.orders: list[om.ProtoOAOrder] = []
        self.cash_flow: list[om.ProtoOADepositWithdraw] = []
        self.page_size = 100
        self.fail: set[int] = set()
        self.replies: dict[int, Callable[[Message], Message]] = {}
        self._serve(om.PROTO_OA_TRADER_REQ, lambda _r: self.trader)
        self._serve(om.PROTO_OA_RECONCILE_REQ, lambda _r: self.snapshot)
        self._serve(
            om.PROTO_OA_ORDER_LIST_BY_POSITION_ID_REQ,
            lambda r: oa.ProtoOAOrderListByPositionIdRes(
                ctidTraderAccountId=ACCOUNT_ID,
                order=self.position_orders.get(r.positionId, []),
                hasMore=False,
            ),
        )
        self._serve(
            om.PROTO_OA_DEAL_LIST_BY_POSITION_ID_REQ,
            lambda r: self._page(
                oa.ProtoOADealListByPositionIdRes,
                "deal",
                _between(self.position_deals.get(r.positionId, []), r, _executed),
            ),
        )
        self._serve(
            om.PROTO_OA_DEAL_LIST_REQ,
            lambda r: self._page(oa.ProtoOADealListRes, "deal", _between(self.deals, r, _executed)),
        )
        self._serve(
            om.PROTO_OA_ORDER_LIST_REQ,
            lambda r: self._page(
                oa.ProtoOAOrderListRes, "order", _between(self.orders, r, _last_update)
            ),
        )
        self._serve(om.PROTO_OA_ORDER_DETAILS_REQ, self._details)
        self._serve(om.PROTO_OA_CASH_FLOW_HISTORY_LIST_REQ, self._cash_flow)
        self.server.on(om.PROTO_OA_CANCEL_ORDER_REQ, self._cancel_order)
        self.server.on(om.PROTO_OA_AMEND_ORDER_REQ, self._amend_order)
        self.server.on(
            om.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
            lambda r: oa.ProtoOASubscribeSpotsRes(ctidTraderAccountId=r.ctidTraderAccountId),
        )
        self.server.on(
            om.PROTO_OA_UNSUBSCRIBE_SPOTS_REQ,
            lambda r: oa.ProtoOAUnsubscribeSpotsRes(ctidTraderAccountId=r.ctidTraderAccountId),
        )

    def _serve(self, payload_type: int, reply: Callable[[Message], Message]) -> None:
        def handle(request: Message) -> Message:
            if payload_type in self.fail:
                return oa.ProtoOAErrorRes(
                    ctidTraderAccountId=ACCOUNT_ID,
                    errorCode="INTERNAL_SERVER_ERROR",
                    description="the list failed",
                )
            return reply(request)

        self.replies[payload_type] = handle
        self.server.on(payload_type, handle)

    def _cash_flow(self, request: oa.ProtoOACashFlowHistoryListReq) -> Message:
        if request.toTimestamp - request.fromTimestamp > WEEK_MS:
            return oa.ProtoOAErrorRes(
                ctidTraderAccountId=ACCOUNT_ID,
                errorCode="INVALID_REQUEST",
                description="the window is over a week",
            )
        return oa.ProtoOACashFlowHistoryListRes(
            ctidTraderAccountId=ACCOUNT_ID,
            depositWithdraw=_between(self.cash_flow, request, _changed),
        )

    def _details(self, request: oa.ProtoOAOrderDetailsReq) -> Message:
        """The order as last changed in any list the venue holds, with its deals."""
        listed = [
            order
            for order in (
                *self.snapshot.order,
                *self.orders,
                *(o for found in self.position_orders.values() for o in found),
            )
            if order.orderId == request.orderId
        ]
        if not listed:
            return oa.ProtoOAErrorRes(
                ctidTraderAccountId=ACCOUNT_ID,
                errorCode="ORDER_NOT_FOUND",
                description="no such order",
            )
        deals = {
            deal.dealId: deal
            for deal in (*self.deals, *(d for found in self.position_deals.values() for d in found))
            if deal.orderId == request.orderId
        }
        return oa.ProtoOAOrderDetailsRes(
            ctidTraderAccountId=ACCOUNT_ID,
            order=max(listed, key=_last_update),
            deal=sorted(deals.values(), key=_executed),
        )

    def _pending_order(self, order_id: int) -> om.ProtoOAOrder | Message:
        """The snapshot's pending order `order_id`, or the broker's error for any other id."""
        for order in self.snapshot.order:
            if order.orderId == order_id and order.orderType in PENDING_TYPES:
                return order
        return oa.ProtoOAOrderErrorEvent(
            ctidTraderAccountId=ACCOUNT_ID,
            errorCode="ORDER_NOT_FOUND",
            orderId=order_id,
            description="Order not found",
        )

    def _cancel_order(self, request: oa.ProtoOACancelOrderReq) -> Message:
        order = self._pending_order(request.orderId)
        if not isinstance(order, om.ProtoOAOrder):
            return order
        cancelled = om.ProtoOAOrder()
        cancelled.CopyFrom(order)
        self.snapshot.order.remove(order)
        cancelled.orderStatus = om.ORDER_STATUS_CANCELLED
        cancelled.utcLastUpdateTimestamp += 1
        self.orders.append(cancelled)
        return pending_event(om.ORDER_CANCELLED, cancelled)

    def _amend_order(self, request: oa.ProtoOAAmendOrderReq) -> Message:
        order = self._pending_order(request.orderId)
        if not isinstance(order, om.ProtoOAOrder):
            return order
        for field, value in request.ListFields():
            if field.name == "guaranteedStopLoss":
                order.tradeData.guaranteedStopLoss = value
            elif field.name == "volume":
                order.tradeData.volume = value
            elif field.name not in ("payloadType", "ctidTraderAccountId", "orderId"):
                setattr(order, field.name, value)
        order.utcLastUpdateTimestamp += 1
        return pending_event(om.ORDER_REPLACED, order)

    def _page(self, response: type[Message], field: str, items: list) -> Message:
        return response(
            ctidTraderAccountId=ACCOUNT_ID,
            hasMore=len(items) > self.page_size,
            **{field: items[: self.page_size]},
        )


PENDING_TYPES = (om.LIMIT, om.STOP, om.STOP_LIMIT)


def pending_event(kind: int, order: om.ProtoOAOrder) -> oa.ProtoOAExecutionEvent:
    """An execution event of a pending order, as the broker sends one about it (hand-built)."""
    event = oa.ProtoOAExecutionEvent(ctidTraderAccountId=ACCOUNT_ID, executionType=kind)
    event.order.CopyFrom(order)
    return event


def _executed(deal: om.ProtoOADeal) -> int:
    return deal.executionTimestamp


def _last_update(order: om.ProtoOAOrder) -> int:
    return order.utcLastUpdateTimestamp


def _changed(operation: om.ProtoOADepositWithdraw) -> int:
    return operation.changeBalanceTimestamp


def exec_config(**overrides) -> CTraderExecClientConfig:
    values = {
        "client_id": "client-id",
        "client_secret": "client-secret",
        "access_token": "access-token",
        "refresh_token": "refresh-token",
        "token_expires_at": 4_102_444_800.0,
        "trader_login": TRADER_LOGIN,
    }
    values.update(overrides)
    return CTraderExecClientConfig(**values)


class LoggedClient(CTraderExecutionClient):
    """The client with its log lines kept: the Nautilus logger writes from Rust, out of reach."""

    def __init__(self, *args, logger: RecordingLogger, **kwargs) -> None:
        self.__dict__["_recording"] = logger
        super().__init__(*args, **kwargs)

    @property
    def _log(self):
        return self.__dict__["_recording"]


@dataclass
class Harness:
    venue: ExecutionVenue
    account: CTraderAccountClient
    client: LoggedClient
    cache: Cache
    engine: LiveExecutionEngine
    factory: OrderFactory
    events: list
    reports: list
    states: list
    activity: list
    logger: RecordingLogger
    mass_statuses: list

    @property
    def server(self) -> FakeCTraderServer:
        return self.venue.server

    def received(self, cls: type[Message]) -> list[Message]:
        return [m for m in self.server.received if isinstance(m, cls)]

    def events_of(self, client_order_id: str) -> list:
        return [e for e in self.events if e.client_order_id.value == client_order_id]

    def kinds_of(self, client_order_id: str) -> list[str]:
        return [type(e).__name__ for e in self.events_of(client_order_id)]


@asynccontextmanager
async def harness(
    *,
    execution_venue: ExecutionVenue | None = None,
    config: CTraderExecClientConfig | None = None,
    connect: bool = True,
    cache: Cache | None = None,
    instruments: Iterable[InstrumentId] = (US100_ID,),
) -> AsyncIterator[Harness]:
    """The client against the fake venue, inside a live execution engine and a portfolio.

    A given `cache` stands for one a restarted node reloads from a persistent backend.
    `instruments` are the ones the node loads.

    What the client sends Nautilus is recorded on the way in: order events, execution reports,
    mass statuses, account states, and the account activity published on the message bus.
    """
    execution_venue = execution_venue or ExecutionVenue()
    server = execution_venue.server
    await server.start()
    config = config or exec_config()
    logger = RecordingLogger()
    account = account_client(server, logger=logger, credentials=config.credentials())
    provider = account.get_instrument_provider(
        config=InstrumentProviderConfig(load_ids=frozenset(instruments)),
        asset_class_overrides={},
        fail_on_instrument_error=False,
        logger=logger,
    )
    clock = LiveClock()
    msgbus = MessageBus(trader_id=TRADER_ID, clock=clock)
    cache = Cache() if cache is None else cache
    portfolio = Portfolio(msgbus, cache, clock)
    engine = LiveExecutionEngine(
        loop=asyncio.get_running_loop(),
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        config=LiveExecEngineConfig(reconciliation=False, inflight_check_interval_ms=0),
    )
    events: list = []
    reports: list = []
    states: list = []
    activity: list = []
    mass_statuses: list = []

    def recorded(store: list, handler):
        def handle(message) -> object:
            store.append(message)
            return handler(message)

        return handle

    for endpoint, store, handler in (
        ("ExecEngine.process", events, engine.process),
        ("ExecEngine.reconcile_execution_report", reports, engine.reconcile_execution_report),
        (
            "ExecEngine.reconcile_execution_mass_status",
            mass_statuses,
            engine.reconcile_execution_mass_status,
        ),
        ("Portfolio.update_account", states, portfolio.update_account),
    ):
        msgbus.deregister(endpoint=endpoint, handler=handler)
        msgbus.register(endpoint=endpoint, handler=recorded(store, handler))
    msgbus.subscribe(topic=ACCOUNT_ACTIVITY_TOPIC, handler=activity.append)

    client = LoggedClient(
        loop=asyncio.get_running_loop(),
        account=account,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        instrument_provider=provider,
        config=config,
        logger=logger,
    )
    engine.register_client(client)
    engine.start()
    h = Harness(
        execution_venue,
        account,
        client,
        cache,
        engine,
        OrderFactory(trader_id=TRADER_ID, strategy_id=STRATEGY_ID, clock=clock),
        events,
        reports,
        states,
        activity,
        logger,
        mass_statuses,
    )
    try:
        if connect:
            await client._connect()
            for instrument in provider.list_all():
                cache.add_instrument(instrument)
        yield h
    finally:
        # Also releases what a test left connected; a client never connected releases nothing.
        await client._disconnect()
        if engine.is_running:
            engine.stop()
        await server.stop()


async def sync(h: Harness) -> None:
    """Return once the client has dispatched every frame pushed before this call.

    The round trip shares the connection, so it cannot overtake an event, and the client's
    reader dispatches frames in order.
    """
    await h.account.request(oa.ProtoOATraderReq(ctidTraderAccountId=ACCOUNT_ID))


async def push(h: Harness, *messages: Message) -> None:
    for message in messages:
        await h.server.push(message)
    await sync(h)


async def push_spot(
    h: Harness,
    bid: int,
    ask: int,
    *,
    timestamp: int | None = None,
    symbol_id: int = US100_SYMBOL_ID,
) -> None:
    """A spot of `US100.cash`, or of `symbol_id`, in the venue's integer price units (1/100000).

    `timestamp` is the broker's time of the spot, in ms.
    """
    spot = oa.ProtoOASpotEvent(ctidTraderAccountId=ACCOUNT_ID, symbolId=symbol_id, bid=bid, ask=ask)
    if timestamp is not None:
        spot.timestamp = timestamp
    await push(h, spot)


def bracket(
    h: Harness,
    *,
    entry: str = "O-E-5000001",
    stop: str = "O-SL-5000001",
    target: str = "O-TP-5000001",
    side: OrderSide = OrderSide.BUY,
    quantity: str = "1.00",
    stop_price: str = "85197.20",
    target_price: str = "85387.22",
    instrument_id: InstrumentId = US100_ID,
) -> OrderList:
    """A market bracket whose ids are the ones `tests.execution_replay.as_ours` writes."""
    return h.factory.bracket(
        instrument_id=instrument_id,
        order_side=side,
        quantity=Quantity.from_str(quantity),
        sl_trigger_price=Price.from_str(stop_price),
        tp_price=Price.from_str(target_price),
        entry_client_order_id=ClientOrderId(entry),
        sl_client_order_id=ClientOrderId(stop),
        tp_client_order_id=ClientOrderId(target),
    )


async def submit_bracket(h: Harness, orders: OrderList) -> None:
    for order in orders.orders:
        h.cache.add_order(order)
    await h.client._submit_order_list(
        SubmitOrderList(
            trader_id=TRADER_ID,
            strategy_id=STRATEGY_ID,
            order_list=orders,
            command_id=UUID4(),
            ts_init=0,
        ),
    )


ENTRY, STOP, TARGET = "O-E-5000001", "O-SL-5000001", "O-TP-5000001"
# The recorded first position, made the node's, on US100.cash: accepted, filled, protected,
# both levels changed by hand, partly closed by hand, then closed by its stop-loss.
FIRST_EVENTS = on_us100(first_n(as_ours(events(), [FIRST]), FIRST))
# The same position as traded by hand: its levels are external legs.
FOREIGN_EVENTS = on_us100(first_n(events(), FIRST))


async def submitted(h: Harness, orders: OrderList | None = None) -> OrderList:
    """The bracket in Nautilus as sent, with no venue behind it: the test brings the events."""
    orders = orders or bracket(h)
    for order in orders.orders:
        h.cache.add_order(order)
        h.client.generate_order_submitted(
            order.strategy_id, order.instrument_id, order.client_order_id, 0
        )
    await wait_until(
        lambda: all(
            h.cache.order(o.client_order_id).status == OrderStatus.SUBMITTED for o in orders.orders
        ),
        description="orders submitted",
    )
    return orders


def status(h: Harness, client_order_id: str) -> OrderStatus:
    return h.cache.order(ClientOrderId(client_order_id)).status


# -- Set-ups shared by the execution tests ------------------------------------------------------

# The node's market position, hand-built: the recording holds no close of the node's.
OURS = 5_300_001
CLOSE = "O-C-5300001"


def broker_lists(
    at: float, *, mine: bool = True
) -> tuple[oa.ProtoOAReconcileRes, dict[int, PositionHistory], tuple[om.ProtoOADeal, ...]]:
    """What the broker lists for the first position at timeline time `at`, on US100.cash."""
    snapshot = snapshot_at(at)
    until = snapshot.position[0].utcLastUpdateTimestamp if snapshot.position else None
    found = history(FIRST, until_ms=until)
    orders = list(found.orders)
    if mine:
        snapshot = as_ours([snapshot], [FIRST])[0]
        orders = as_ours(orders, [FIRST])
    moved = PositionHistory(tuple(on_us100(orders)), tuple(on_us100(found.deals)))
    deals = tuple(on_us100(window_deals(FIRST, until_ms=until)))
    return on_us100([snapshot])[0], {FIRST: moved}, deals


def serve(venue: ExecutionVenue, at: float, *, mine: bool = True, later_ms: int = 0) -> None:
    """Make the venue's snapshot and lists the first position at timeline time `at`.

    The position is the node's when `mine`, else as traded by hand; every time is `later_ms`
    later than recorded.
    """
    snapshot, histories, deals = broker_lists(at, mine=mine)
    (venue.snapshot,) = shifted([snapshot], later_ms)
    venue.position_orders = {
        pid: shifted(found.orders, later_ms) for pid, found in histories.items()
    }
    venue.position_deals = {pid: shifted(found.deals, later_ms) for pid, found in histories.items()}
    venue.deals = shifted(deals, later_ms)


async def started(h: Harness) -> ExecutionMassStatus:
    """The start's reconciliation, reconciled by the engine as a node would."""
    built = await h.client.generate_mass_status()
    h.engine.reconcile_execution_mass_status(built)
    return built


def our_market_position(venue: ExecutionVenue, *, opened: int, closed: int | None = None) -> None:
    """The node's market order on `US100.cash`, filled at `opened`; closed at `closed` if given."""
    entry_id = "O-M-5300001"
    entry = make_order(
        6_300_001,
        OURS,
        utc=opened,
        label=order_record.encode_label(entry_id),
        comment=order_record.encode_comment(LegIds(None, None)),
        client_order_id=entry_id,
        symbol=US100_SYMBOL_ID,
    )
    entry.orderStatus = om.ORDER_STATUS_FILLED
    orders = [entry]
    deals = [
        make_deal(7_300_001, 6_300_001, OURS, side=om.BUY, volume=100, price=85000.0, ts=opened)
    ]
    venue.snapshot = oa.ProtoOAReconcileRes(ctidTraderAccountId=ACCOUNT_ID)
    if closed is None:
        position = make_position(OURS, symbol=US100_SYMBOL_ID)
        position.price = 85000.0
        position.utcLastUpdateTimestamp = opened
        venue.snapshot.position.append(position)
    else:
        close = make_order(
            6_300_002, OURS, side=om.SELL, closing=True, utc=closed, symbol=US100_SYMBOL_ID
        )
        close.orderStatus = om.ORDER_STATUS_FILLED
        orders.append(close)
        deals.append(
            make_deal(
                7_300_002, 6_300_002, OURS, side=om.SELL, volume=100, price=85100.0, ts=closed
            ),
        )
    venue.position_orders = {OURS: orders}
    venue.position_deals = {OURS: on_us100(deals)}
    venue.deals = on_us100(deals)


def answer_closes(venue: ExecutionVenue, *, behind_ms: int = 0) -> list[oa.ProtoOAExecutionEvent]:
    """Answer the node's close with its acceptance, made when the close arrives.

    The broker stamps it `behind_ms` behind the node's clock. Returns the list that then holds
    the close's events, accepted and filled.
    """
    made: list[oa.ProtoOAExecutionEvent] = []

    def answer(_request: Message) -> Message:
        made[:] = node_close_events(closed=int(time.time() * 1000) - behind_ms)
        return made[0]

    venue.server.on(om.PROTO_OA_CLOSE_POSITION_REQ, answer)
    return made


async def close_sent(h: Harness) -> asyncio.Task:
    """The node's close of `OURS`, sent; returns the task awaiting its answer."""
    position_id = PositionId(str(OURS))
    order = h.factory.market(
        US100_ID,
        OrderSide.SELL,
        Quantity.from_str("1.00"),
        reduce_only=True,
        client_order_id=ClientOrderId(CLOSE),
    )
    h.cache.add_order(order, position_id=position_id)
    closing = asyncio.create_task(
        h.client._submit_order(
            SubmitOrder(
                trader_id=TRADER_ID,
                strategy_id=STRATEGY_ID,
                order=order,
                command_id=UUID4(),
                ts_init=0,
                position_id=position_id,
            ),
        ),
    )
    await wait_until(lambda: len(h.received(oa.ProtoOAClosePositionReq)) == 1)
    return closing


def node_close_events(*, closed: int) -> list[oa.ProtoOAExecutionEvent]:
    """The node's close of `OURS` created and accepted, then filled, at `closed` (hand-built).

    A closing order is taken for the node's close only if it was created after the newest
    broker time the node had seen when it sent it, so `closed` must be later than that.
    """

    def order(utc: int) -> om.ProtoOAOrder:
        found = make_order(
            6_300_002, OURS, side=om.SELL, closing=True, utc=utc, symbol=US100_SYMBOL_ID
        )
        found.tradeData.openTimestamp = closed
        return found

    def position(utc: int, status: int = om.POSITION_STATUS_OPEN) -> om.ProtoOAPosition:
        found = make_position(
            OURS, volume=0 if status == om.POSITION_STATUS_CLOSED else 100, status=status
        )
        found.utcLastUpdateTimestamp = utc
        return found

    deal = make_deal(7_300_002, 6_300_002, OURS, side=om.SELL, volume=100, price=85100.0, ts=closed)
    deal.closePositionDetail.CopyFrom(
        om.ProtoOAClosePositionDetail(
            entryPrice=85000.0,
            grossProfit=10_000,
            swap=0,
            commission=0,
            balance=1_000_000,
            balanceVersion=100,
            moneyDigits=2,
        ),
    )
    return on_us100(
        [
            make_event(om.ORDER_ACCEPTED, order(closed), position=position(closed)),
            make_event(
                om.ORDER_FILLED,
                order(closed),
                position=position(closed, om.POSITION_STATUS_CLOSED),
                deal=deal,
            ),
        ],
    )
