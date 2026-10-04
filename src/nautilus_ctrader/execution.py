"""The execution client: Nautilus orders to the broker, and the broker's execution events back.

What the broker holds is the venue model's (`VenueBook`), changed only by the broker's own data.
This client translates between that model and Nautilus:

- each command becomes a venue request, registered as in flight before it leaves, so the model
  can tell the node's own changes from a trader's;
- a request's response and the pushed events go through one entry point into the model;
- the model's records become Nautilus events, reports and account activity.

An order or a close whose outcome is unknown is never resent: the lost answer may hide a fill.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal

from google.protobuf.message import Message
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.execution.messages import (
    GenerateFillReports,
    GenerateOrderStatusReport,
    GenerateOrderStatusReports,
    GeneratePositionStatusReports,
)
from nautilus_trader.execution.reports import FillReport, OrderStatusReport, PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import AccountType, LiquiditySide, OmsType
from nautilus_trader.model.identifiers import (
    AccountId,
    ClientId,
    ClientOrderId,
    InstrumentId,
    PositionId,
    TradeId,
    VenueOrderId,
)
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.orders import Order

from nautilus_ctrader.activity import ACCOUNT_ACTIVITY_TOPIC
from nautilus_ctrader.common import execution_reports as reports
from nautilus_ctrader.common.account import CTraderAccountClient
from nautilus_ctrader.common.errors import (
    CTraderAccountError,
    CTraderConnectionError,
    CTraderRequestError,
)
from nautilus_ctrader.common.operations import OperationsInFlight
from nautilus_ctrader.common.parsing import PRICE_SCALE
from nautilus_ctrader.common.session import CTraderSession
from nautilus_ctrader.common.venue_book import VenueBook
from nautilus_ctrader.common.venue_records import (
    Activity,
    ActivityKind,
    AwaitProtection,
    ExternalOrder,
    Notice,
    OrderEvent,
    OrderEventKind,
    ProtectionMissing,
    Record,
    money_of,
)
from nautilus_ctrader.config import CTraderExecClientConfig
from nautilus_ctrader.constants import BUCKET_HISTORICAL, CTRADER, CTRADER_VENUE
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from nautilus_ctrader.providers import CTraderInstrumentProvider

_REFERENCE_CONSUMER = "reference"
# The most pages of one position's order list read; the venue lists newest first.
_MAX_ORDER_PAGES = 20


@dataclass
class _Quote:
    """The latest spot of one symbol; `at` is the event loop's time it arrived."""

    bid: Decimal | None = None
    ask: Decimal | None = None
    at: float = 0.0


def check_account(trader: om.ProtoOATrader) -> None:
    """Refuse an account this adapter cannot trade, naming why."""
    if trader.accountType != om.HEDGED:
        kind = om.ProtoOAAccountType.Name(trader.accountType)
        raise CTraderAccountError(f"the account is {kind}; only hedging accounts are supported")
    if trader.accessRights != om.FULL_ACCESS:
        rights = om.ProtoOAAccessRights.Name(trader.accessRights)
        raise CTraderAccountError(
            f"the account's access rights are {rights}; trading needs FULL_ACCESS",
        )
    if trader.isLimitedRisk:
        raise CTraderAccountError(
            "the account is a limited-risk account, which needs a guaranteed stop on every "
            "order; not supported",
        )


def _reason(code: str, description: str | None) -> str:
    # TODO(verify): that the venue's description never carries an account id or login.
    return f"{code}: {description}" if description else code


class CTraderExecutionClient(LiveExecutionClient):
    """
    Order execution on one cTrader hedging account.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    account : CTraderAccountClient
        The account's connection, shared with every other client of the same account.
    msgbus : MessageBus
        The message bus for the client.
    cache : Cache
        The cache for the client.
    clock : LiveClock
        The clock for the client.
    instrument_provider : CTraderInstrumentProvider
        The account's own provider, from `account.get_instrument_provider()`.
    config : CTraderExecClientConfig
        The configuration for the client.
    name : str, optional
        The custom client ID.

    Raises
    ------
    ValueError
        If `instrument_provider` is not the account's own.

    Notes
    -----
    Connecting fails with `CTraderAccountError` when the account is not a hedging account,
    lacks full access rights, or is a limited-risk account.

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        account: CTraderAccountClient,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: CTraderInstrumentProvider,
        config: CTraderExecClientConfig,
        name: str | None = None,
    ) -> None:
        if instrument_provider is not account.instrument_provider:
            raise ValueError(
                "instrument_provider must be the account's own, from "
                "account.get_instrument_provider()",
            )
        super().__init__(
            loop=loop,
            client_id=ClientId(name or CTRADER_VENUE.value),
            venue=CTRADER_VENUE,
            oms_type=OmsType.HEDGING,
            account_type=AccountType.MARGIN,
            # Known only once connected; every account state carries the deposit currency.
            base_currency=None,
            instrument_provider=instrument_provider,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
        )
        # Nautilus logs account ids, so this is never the broker's account number.
        self._set_account_id(AccountId(f"{CTRADER}-001"))
        self._instrument_provider: CTraderInstrumentProvider = instrument_provider
        self._account = account
        self._config = config
        # Scopes this client's spot holds in the registry every client of the account shares.
        self._owner = f"exec:{self.id}"
        self._session: CTraderSession | None = None
        self._book = VenueBook(self._price_precision)
        self._currency: Currency | None = None
        self._balance = Decimal(0)
        self._balance_version = -1
        # Position id -> (symbol id, margin used), for each open position.
        self._margins: dict[int, tuple[int, Decimal]] = {}
        # Position id -> the `utcLastUpdateTimestamp` of the state its margin was taken from.
        self._margin_times: dict[int, int] = {}
        self._quotes: dict[int, _Quote] = {}
        self._spot_symbols: set[int] = set()
        self._operations = OperationsInFlight()
        # Execution events held while the model is rebuilt, or `None` when it stands.
        self._buffer: list[oa.ProtoOAExecutionEvent] | None = None
        self._restore_key = ("execution", self._owner)
        # The account counts its users without knowing who releases, and Nautilus calls
        # `_disconnect` even after a failed `_connect`: only a user this client holds is released.
        self._holds_account = False

    @property
    def instrument_provider(self) -> CTraderInstrumentProvider:
        return self._instrument_provider

    async def _connect(self) -> None:
        await self._account.connect()
        self._holds_account = True
        try:
            await self._instrument_provider.initialize()
            session = self._account.session
            assert session is not None  # `connect()` returned, so the session is up
            self._session = session
            session.add_event_handler(oa.ProtoOAExecutionEvent, self._on_execution_event)
            session.add_event_handler(oa.ProtoOAOrderErrorEvent, self._on_order_error_event)
            trader = await self._trader()
            check_account(trader)
            self._currency = Currency.from_str(self._account.deposit_asset.name)
            self._balance = money_of(trader.balance, trader.moneyDigits)
            self._balance_version = trader.balanceVersion
            await self._load()
            self._emit_account_state(self._clock.timestamp_ns())
            await self._hold_reference_spots()
            # The first bring-up has run its restores already; this one serves every later one.
            session.add_restore(self._restore_key, self._reload)
        except BaseException:
            await self._disconnect()
            raise

    async def _disconnect(self) -> None:
        self._detach()
        try:
            await self._release_reference_spots()
        finally:
            if self._holds_account:
                self._holds_account = False
                await self._account.disconnect()

    def _detach(self) -> None:
        session, self._session = self._session, None
        if session is None:
            return
        session.remove_event_handler(oa.ProtoOAExecutionEvent, self._on_execution_event)
        session.remove_event_handler(oa.ProtoOAOrderErrorEvent, self._on_order_error_event)
        session.remove_restore(self._restore_key)

    async def _reload(self) -> None:
        """Rebuild the model on a reconnect: the broker may have changed meanwhile."""
        self._log.warning("Rebuilding the venue model after a reconnect")
        trader = await self._trader()
        self._balance = money_of(trader.balance, trader.moneyDigits)
        self._balance_version = trader.balanceVersion
        await self._load()
        self._emit_account_state(self._clock.timestamp_ns())

    # Reconciliation reports are not built yet. Until they are, these report nothing, and
    # Nautilus resolves orders in flight through its own in-flight check.

    async def generate_order_status_report(
        self,
        command: GenerateOrderStatusReport,
    ) -> OrderStatusReport | None:
        return None

    async def generate_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        return []

    async def generate_fill_reports(self, command: GenerateFillReports) -> list[FillReport]:
        return []

    async def generate_position_status_reports(
        self,
        command: GeneratePositionStatusReports,
    ) -> list[PositionStatusReport]:
        return []

    # -- Venue data ---------------------------------------------------------------------------

    async def _request(self, payload: Message, *, bucket: str | None = None) -> Message:
        session = self._account.session
        if session is None:
            raise CTraderConnectionError("account client not connected")
        return await session.request(payload, bucket=bucket)

    async def _trader(self) -> om.ProtoOATrader:
        response = await self._request(
            oa.ProtoOATraderReq(ctidTraderAccountId=self._account.account_id),
        )
        return response.trader

    async def _load(self) -> None:
        """Rebuild the venue model from the broker's open positions and orders.

        Execution events that arrive meanwhile are applied once it stands, in order.
        """
        self._buffer = []
        try:
            snapshot = await self._request(
                oa.ProtoOAReconcileReq(
                    ctidTraderAccountId=self._account.account_id,
                    returnProtectionOrders=True,
                ),
            )
            position_orders = {
                position.positionId: await self._position_orders(position.positionId)
                for position in snapshot.position
            }
            self._handle_records(self._book.load(snapshot, position_orders))
            self._margins = {}
            self._margin_times = {}
            for position in snapshot.position:
                self._set_margin(position)
        finally:
            held, self._buffer = self._buffer, None
            for event in held:
                self._on_execution_event(event)

    async def _position_orders(self, position_id: int) -> list[om.ProtoOAOrder]:
        """Every order of one position: its entry tells whose position it is."""
        found: dict[int, om.ProtoOAOrder] = {}
        request = oa.ProtoOAOrderListByPositionIdReq(
            ctidTraderAccountId=self._account.account_id,
            positionId=position_id,
        )
        for _ in range(_MAX_ORDER_PAGES):
            response = await self._request(request, bucket=BUCKET_HISTORICAL)
            new = [order for order in response.order if order.orderId not in found]
            found.update((order.orderId, order) for order in new)
            if not response.hasMore or not new:
                break
            # TODO(verify): paging backwards by `toTimestamp`; no position with more orders than
            # one page was recorded.
            request.toTimestamp = min(order.utcLastUpdateTimestamp for order in new)
        else:
            self._log.warning(
                f"Position {position_id}: its order list did not end within "
                f"{_MAX_ORDER_PAGES} pages; its entry may be missing",
            )
        return list(found.values())

    def _price_precision(self, symbol_id: int) -> int | None:
        instrument = self._instrument_provider.instrument_for_symbol_id(symbol_id)
        return None if instrument is None else instrument.price_precision

    # -- Account state --------------------------------------------------------------------------

    def _money(self, amount: int, message: Message) -> Decimal:
        """An amount `message` carries, at its own `moneyDigits` when it has them."""
        digits = (
            message.moneyDigits if message.HasField("moneyDigits") else self._account.money_digits
        )
        return money_of(amount, digits)

    def _set_margin(self, position: om.ProtoOAPosition) -> bool:
        """Keep one position's margin; returns whether it changed.

        A state older than the one held is ignored: a response can be applied after a later
        event. A created position has no update time and counts as 0, the oldest.
        """
        position_id = position.positionId
        updated = position.utcLastUpdateTimestamp
        if updated < self._margin_times.get(position_id, -1):
            return False
        self._margin_times[position_id] = updated
        if position.positionStatus != om.POSITION_STATUS_OPEN:
            return self._margins.pop(position_id, None) is not None
        entry = (position.tradeData.symbolId, self._money(position.usedMargin, position))
        if self._margins.get(position_id) == entry:
            return False
        self._margins[position_id] = entry
        return True

    def _emit_account_state(self, ts_event: int) -> None:
        margins: dict[InstrumentId | None, Decimal] = {}
        for symbol_id, margin in self._margins.values():
            instrument = self._instrument_provider.instrument_for_symbol_id(symbol_id)
            key = None if instrument is None else instrument.id
            margins[key] = margins.get(key, Decimal(0)) + margin
        self._send_account_state(
            reports.account_state(
                self.account_id,
                self._currency,
                self._balance,
                margins,
                ts_event=ts_event,
                ts_init=self._clock.timestamp_ns(),
            ),
        )

    # -- Execution events -----------------------------------------------------------------------

    def _on_execution_event(self, event: oa.ProtoOAExecutionEvent) -> list[Record]:
        """The one way an execution event reaches the model, pushed or as a request's response.

        Returns the records it produced, so a command can tell what its response meant.
        """
        if self._buffer is not None:
            self._buffer.append(event)
            return []
        try:
            records = self._book.apply(event, self._operations)
        except Exception as e:
            # The model does not mark it seen, so a repeat of the event is applied again.
            self._log.exception("An execution event could not be applied to the venue model", e)
            records = []
        self._handle_records(records)
        return records

    def _on_order_error_event(self, event: oa.ProtoOAOrderErrorEvent) -> None:
        # One answering a request goes to that request; this one found nobody waiting.
        self._log.warning(
            "Order error with no request waiting: "
            f"{_reason(event.errorCode, event.description or None)}",
        )

    def _handle_records(self, records: Iterable[Record]) -> None:
        for record in records:
            try:
                if isinstance(record, OrderEvent):
                    self._order_event(record)
                elif isinstance(record, ExternalOrder):
                    self._external_order(record)
                elif isinstance(record, Activity):
                    self._activity(record)
                elif isinstance(record, Notice):
                    self._log.warning(record.text)
                else:
                    self._on_protection(record)
            except Exception as e:
                self._log.exception(f"{type(record).__name__} could not be reported", e)

    def _on_protection(self, record: AwaitProtection | ProtectionMissing) -> None:
        self._log.debug(f"{type(record).__name__} for position {record.position_id}")

    def _nautilus_order(self, record: OrderEvent) -> Order | None:
        """The order a record is about: the node's by its own id, an external one by venue id."""
        if record.client_order_id is not None:
            return self._cache.order(ClientOrderId(record.client_order_id))
        if record.venue_order_id is None:
            return None
        client_order_id = self._cache.client_order_id(VenueOrderId(record.venue_order_id))
        return None if client_order_id is None else self._cache.order(client_order_id)

    def _order_event(self, record: OrderEvent) -> None:
        order = self._nautilus_order(record)
        if order is None:
            self._log.warning(
                f"No Nautilus order for {record.client_order_id or record.venue_order_id}; "
                f"its {record.kind.value} event is not reported",
            )
            return
        if record.client_order_id is not None:
            # Matched to its broker order: the model knows the close from now on.
            self._operations.end_close(record.client_order_id)
        instrument = self._instrument_provider.find(order.instrument_id)
        venue_order_id = (
            VenueOrderId(record.venue_order_id) if record.venue_order_id else order.venue_order_id
        )
        ids = (order.strategy_id, order.instrument_id, order.client_order_id)
        ts = reports.nanos(record.ts_ms)
        kind = record.kind
        if kind == OrderEventKind.ACCEPTED:
            self.generate_order_accepted(*ids, venue_order_id, ts)
            if record.quantity is not None and record.quantity != order.quantity.as_decimal():
                # A leg takes the protective order's volume, which a partial entry leaves short.
                quantity = reports.quantity(record.quantity, instrument)
                self.generate_order_updated(*ids, venue_order_id, quantity, None, None, ts)
        elif kind == OrderEventKind.FILLED:
            fill = record.fill
            self.generate_order_filled(
                *ids,
                venue_order_id,
                PositionId(fill.venue_position_id),
                TradeId(fill.trade_id),
                reports.order_side(fill.side),
                order.order_type,
                reports.quantity(fill.units, instrument),
                reports.price(fill.price, instrument),
                instrument.quote_currency,
                reports.commission(fill, self._currency),
                LiquiditySide.NO_LIQUIDITY_SIDE,
                ts,
            )
        elif kind == OrderEventKind.UPDATED:
            quantity = (
                order.quantity
                if record.quantity is None
                else reports.quantity(record.quantity, instrument)
            )
            price = None if record.price is None else reports.price(record.price, instrument)
            trigger = (
                None
                if record.trigger_price is None
                else reports.price(record.trigger_price, instrument)
            )
            self.generate_order_updated(*ids, venue_order_id, quantity, price, trigger, ts)
        elif kind == OrderEventKind.CANCELED:
            self.generate_order_canceled(*ids, venue_order_id, ts)
        elif kind == OrderEventKind.REJECTED:
            self.generate_order_rejected(*ids, record.reason or "rejected by the venue", ts)
        elif kind == OrderEventKind.EXPIRED:
            self.generate_order_expired(*ids, venue_order_id, ts)

    def _external_order(self, record: ExternalOrder) -> None:
        """An order Nautilus does not know yet: its report, then a report of each fill."""
        instrument = self._instrument_provider.instrument_for_symbol_id(record.symbol_id)
        if instrument is None:
            self._log.warning(f"Order {record.venue_order_id} is on an instrument no longer loaded")
            return
        ts_init = self._clock.timestamp_ns()
        self._send_order_status_report(
            reports.order_status_report(record, instrument, self.account_id, ts_init),
        )
        for fill in record.fills:
            self._send_fill_report(
                reports.fill_report(
                    fill,
                    record.venue_order_id,
                    instrument,
                    self.account_id,
                    self._currency,
                    ts_init,
                ),
            )

    def _activity(self, record: Activity) -> None:
        symbol = self._symbol_name(record.symbol_id)
        self._msgbus.publish(
            topic=ACCOUNT_ACTIVITY_TOPIC,
            msg=reports.account_activity(record, symbol, self._clock.timestamp_ns()),
        )
        text = (
            f"Account activity: {record.kind.value} {record.action.value} {record.subject} "
            f"{record.side} {record.units} {symbol}"
        )
        if record.kind == ActivityKind.STOP_OUT:
            self._log.warning(text)
        else:
            self._log.info(text)

    def _symbol_name(self, symbol_id: int) -> str:
        for name, light in self._account.light_symbols.items():
            if light.symbolId == symbol_id:
                return name
        return str(symbol_id)

    # -- Reference prices -----------------------------------------------------------------------

    async def _hold_reference_spots(self) -> None:
        """Hold the spots of every loaded instrument: a bracket's levels are measured from them."""
        subscriptions = self._account.subscriptions
        for instrument in self._instrument_provider.list_all():
            symbol_id = self._instrument_provider.symbol_id(instrument.id)
            if symbol_id in self._spot_symbols:
                continue
            self._spot_symbols.add(symbol_id)
            subscriptions.add_spot_listener(symbol_id, self._on_spot)
            try:
                await subscriptions.subscribe_spots(symbol_id, _REFERENCE_CONSUMER, self._owner)
            except CTraderRequestError as e:
                # Brackets on it are refused for want of a price; nothing else depends on it.
                self._log.warning(
                    f"Spots of {instrument.id} refused ({e.error_code}); "
                    "brackets on it cannot be measured",
                )

    async def _release_reference_spots(self) -> None:
        subscriptions = self._account.subscriptions
        for symbol_id in self._spot_symbols:
            subscriptions.remove_spot_listener(symbol_id, self._on_spot)
        self._spot_symbols.clear()
        self._quotes.clear()
        for symbol_id, consumer in sorted(subscriptions.spot_holds(self._owner)):
            await subscriptions.unsubscribe_spots(symbol_id, consumer, self._owner)

    def _on_spot(self, event: oa.ProtoOASpotEvent) -> None:
        if not (event.HasField("bid") or event.HasField("ask")):
            return
        quote = self._quotes.setdefault(event.symbolId, _Quote())
        if event.HasField("bid"):
            quote.bid = Decimal(event.bid) / PRICE_SCALE
        if event.HasField("ask"):
            quote.ask = Decimal(event.ask) / PRICE_SCALE
        quote.at = self._loop.time()
