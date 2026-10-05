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
import contextlib
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from decimal import Decimal

from google.protobuf.message import Message
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import (
    BatchCancelOrders,
    CancelAllOrders,
    CancelOrder,
    GenerateFillReports,
    GenerateOrderStatusReport,
    GenerateOrderStatusReports,
    GeneratePositionStatusReports,
    ModifyOrder,
    QueryAccount,
    SubmitOrder,
    SubmitOrderList,
)
from nautilus_trader.execution.reports import (
    ExecutionMassStatus,
    FillReport,
    OrderStatusReport,
    PositionStatusReport,
)
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import (
    AccountType,
    LiquiditySide,
    OmsType,
    OrderStatus,
    OrderType,
    PositionSide,
)
from nautilus_trader.model.identifiers import (
    AccountId,
    ClientId,
    ClientOrderId,
    InstrumentId,
    PositionId,
    TradeId,
    VenueOrderId,
)
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Currency, Price, Quantity
from nautilus_trader.model.orders import Order

from nautilus_ctrader.activity import ACCOUNT_ACTIVITY_TOPIC
from nautilus_ctrader.common import execution_reports as reports
from nautilus_ctrader.common import history, order_translation
from nautilus_ctrader.common.account import CTraderAccountClient
from nautilus_ctrader.common.errors import (
    CTraderAccountError,
    CTraderConnectionError,
    CTraderError,
    CTraderRequestError,
    CTraderTimeoutError,
)
from nautilus_ctrader.common.operations import OperationsInFlight, PendingBracket, PendingBrackets
from nautilus_ctrader.common.order_record import LegIds
from nautilus_ctrader.common.order_translation import Unsupported
from nautilus_ctrader.common.parsing import PRICE_SCALE
from nautilus_ctrader.common.reconciliation import PositionHistory, reconcile
from nautilus_ctrader.common.session import CTraderSession
from nautilus_ctrader.common.venue_book import PositionView, VenueBook
from nautilus_ctrader.common.venue_records import (
    Activity,
    ActivityKind,
    AwaitProtection,
    ExternalOrder,
    Level,
    Notice,
    OrderEvent,
    OrderEventKind,
    ProtectionMissing,
    Record,
    money_of,
)
from nautilus_ctrader.config import CTraderExecClientConfig
from nautilus_ctrader.constants import (
    BUCKET_HISTORICAL,
    CTRADER,
    CTRADER_VENUE,
    UNLOADED_EXPOSURE_KEY,
)
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from nautilus_ctrader.providers import CTraderInstrumentProvider

_REFERENCE_CONSUMER = "reference"
_NOT_CONNECTED = "not connected to the venue"
# The most pages of one position's order list read; the venue lists newest first.
_MAX_ORDER_PAGES = 20
# Sends of one level amend that got no answer or never left; it sets the whole state, so a
# repeat is safe.
_AMEND_ATTEMPTS = 3
# Correcting amends of one bracket before its levels are left where the broker holds them.
_CORRECTION_ROUNDS = 3
# Nautilus publishes each mass status here once it has reconciled it.
_RECONCILED_TOPIC = f"reports.execution.{CTRADER_VENUE}"
_MINUTE_MS = 60_000
_OPEN = (OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED)
_ENDED = (OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.REJECTED)
# Activity after which what stands on unloaded symbols may have changed.
_EXPOSURE_KINDS = (ActivityKind.UNLOADED_SYMBOL, ActivityKind.STOP_OUT)


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


def _ms(moment) -> int:
    """A datetime as Unix milliseconds."""
    return int(moment.timestamp() * 1000)


def _reason(code: str, description: str | None) -> str:
    # TODO(verify): that the venue's description never carries an account id or login.
    return f"{code}: {description}" if description else code


@dataclass(frozen=True)
class BrokerState:
    """One read of the broker: what stands now, and the lists of the positions it covers.

    `histories` holds each covered position's lists by position id; `window_deals` the account's
    deals of the fill window; `requests` how many requests the read took.
    """

    snapshot: oa.ProtoOAReconcileRes
    histories: dict[int, PositionHistory]
    window_deals: tuple[om.ProtoOADeal, ...]
    requests: int


@dataclass(frozen=True)
class _Refused:
    """A request the broker refused, or one that never left."""

    reason: str
    # The request never reached the broker, or the broker asked to slow down: a level amend,
    # which sets the whole state, may be sent again.
    retryable: bool = False


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
        self._brackets = PendingBrackets()
        # One amend of a position at a time, so each computes from what the last one left.
        self._amend_locks: dict[int, asyncio.Lock] = {}
        self._protection_timers: set[asyncio.TimerHandle] = set()
        # Execution events held while the model is rebuilt, or `None` when it stands.
        self._buffer: list[oa.ProtoOAExecutionEvent] | None = None
        # The start's mass status the held events wait on, and how long they wait at most.
        self._awaited_report: UUID4 | None = None
        self._release_timer: asyncio.TimerHandle | None = None
        self._reconciled_wait_secs = config.connect_timeout_secs
        # The fill window Nautilus asked for at start, which a reconnect reuses.
        self._lookback_mins: int | None = None
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
            trader = await self._trader()
            check_account(trader)
            self._currency = Currency.from_str(self._account.deposit_asset.name)
            self._balance_version = -1
            self._take_trader(trader)
            # Attached only once the account is known, and before the rebuild, whose buffer
            # holds the execution events that come meanwhile.
            self._session = session
            self._msgbus.subscribe(topic=_RECONCILED_TOPIC, handler=self._on_reconciled)
            session.add_event_handler(oa.ProtoOAExecutionEvent, self._on_execution_event)
            session.add_event_handler(oa.ProtoOAOrderErrorEvent, self._on_order_error_event)
            session.add_event_handler(oa.ProtoOATraderUpdatedEvent, self._on_trader_updated)
            session.add_event_handler(oa.ProtoOAMarginChangedEvent, self._on_margin_changed)
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
        for timer in self._protection_timers:
            timer.cancel()
        self._protection_timers.clear()
        self._stop_awaiting()
        # Whatever it held is dropped: its next connect rebuilds anew.
        self._buffer = None
        session, self._session = self._session, None
        if session is None:
            return
        self._msgbus.unsubscribe(topic=_RECONCILED_TOPIC, handler=self._on_reconciled)
        session.remove_event_handler(oa.ProtoOAExecutionEvent, self._on_execution_event)
        session.remove_event_handler(oa.ProtoOAOrderErrorEvent, self._on_order_error_event)
        session.remove_event_handler(oa.ProtoOATraderUpdatedEvent, self._on_trader_updated)
        session.remove_event_handler(oa.ProtoOAMarginChangedEvent, self._on_margin_changed)
        session.remove_restore(self._restore_key)

    async def _reload(self) -> None:
        """Rebuild the model on a reconnect: the broker may have changed meanwhile."""
        self._log.warning("Rebuilding the venue model after a reconnect")
        trader = await self._trader()
        self._take_trader(trader)
        await self._load()
        self._emit_account_state(self._clock.timestamp_ns())

    # -- Reconciliation -------------------------------------------------------------------------

    async def generate_mass_status(
        self,
        lookback_mins: int | None = None,
    ) -> ExecutionMassStatus | None:
        """The start's reconciliation, built from one read of the broker.

        The read also rebuilds the venue model and writes the unloaded exposure. Execution events
        that arrive meanwhile are held until Nautilus has reconciled the returned mass status,
        which it announces on `reports.execution.CTRADER`, or for `connect_timeout_secs` at most:
        applied earlier, an event could address an order Nautilus does not know yet.

        Returns `None`, with a WARNING, when a request of the read fails.
        """
        self._lookback_mins = lookback_mins
        self._hold_buffer()
        try:
            state = await self._read_broker(since_ms=self._since_ms(lookback_mins))
            margins = dict(self._margins)
            self._stand(state)
            status = self._mass_status(state)
            self._write_exposure()
        except CTraderError as e:
            self._release_buffer()
            self._log.warning(f"Reconciliation failed, so nothing is reported: {e}")
            return None
        except BaseException:
            self._release_buffer()
            raise
        if self._margins != margins:
            self._emit_account_state(self._clock.timestamp_ns())
        fills = sum(len(found) for found in status.fill_reports.values())
        positions = sum(len(found) for found in status.position_reports.values())
        self._log.info(
            f"Reconciliation read the broker in {state.requests} requests: "
            f"{len(status.order_reports)} orders, {fills} fills, {positions} positions",
        )
        self._await_reconciliation(status.id)
        return status

    # Answering a query for one order is not built yet; Nautilus resolves orders in flight
    # through its own in-flight check.

    async def generate_order_status_report(
        self,
        command: GenerateOrderStatusReport,
    ) -> OrderStatusReport | None:
        return None

    async def generate_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        """Every open order and live leg; with `open_only` false, also those that ended unfilled.

        A filled order reaches Nautilus only inside a mass status: without its fills Nautilus
        would infer one with no commission.
        """
        since_ms = None if command.open_only else self._command_since_ms(command.start)
        status = await self._reports(since_ms)
        if status is None:
            return []
        return [
            report
            for report in status.order_reports.values()
            if command.instrument_id in (None, report.instrument_id)
            and (
                report.order_status in _OPEN
                or (
                    not command.open_only
                    and report.order_status in _ENDED
                    and report.filled_qty.as_decimal() == 0
                )
            )
        ]

    async def generate_fill_reports(self, command: GenerateFillReports) -> list[FillReport]:
        """The fills of the window, from `command.start` or the reconciliation lookback."""
        since_ms = self._command_since_ms(command.start)
        status = await self._reports(since_ms)
        if status is None:
            return []
        until = None if command.end is None else reports.nanos(_ms(command.end))
        return [
            fill
            for venue_order_id, fills in status.fill_reports.items()
            if command.venue_order_id in (None, venue_order_id)
            for fill in fills
            if command.instrument_id in (None, fill.instrument_id)
            and fill.ts_event >= reports.nanos(since_ms)
            and (until is None or fill.ts_event <= until)
        ]

    async def generate_position_status_reports(
        self,
        command: GeneratePositionStatusReports,
    ) -> list[PositionStatusReport]:
        """The open positions."""
        status = await self._reports(None)
        if status is None:
            return []
        return [
            report
            for instrument_id, found in status.position_reports.items()
            if command.instrument_id in (None, instrument_id)
            for report in found
        ]

    async def _reports(self, since_ms: int | None) -> ExecutionMassStatus | None:
        """One read of the broker as a mass status, the venue model and the buffer untouched."""
        try:
            state = await self._read_broker(since_ms=since_ms)
        except CTraderError as e:
            self._log.warning(f"Reports not generated: {e}")
            return None
        return self._mass_status(state)

    def _mass_status(self, state: BrokerState) -> ExecutionMassStatus:
        found = reconcile(
            state.snapshot,
            state.histories,
            state.window_deals,
            self._price_precision,
            self._book.known_closes(),
            self._operations,
        )
        for notice in found.notices:
            self._log.warning(notice.text)
        status, left_out = reports.mass_status(
            self.id,
            self.account_id,
            self.venue,
            found,
            self._instrument_provider.instrument_for_symbol_id,
            self._currency,
            self._clock.timestamp_ns(),
            held_price=self._held_price,
        )
        for record in left_out:
            # Normal after a restart without a persistent cache.
            self._log.debug(
                f"Leg {record.client_order_id} is not reported: its level is gone and Nautilus "
                "holds no price for it",
            )
        return status

    def _held_price(self, client_order_id: str) -> Decimal | None:
        """The price Nautilus holds for a leg: a stop's trigger price, a limit's price."""
        order = self._cache.order(ClientOrderId(client_order_id))
        if order is None:
            return None
        if order.order_type == OrderType.STOP_MARKET:
            return order.trigger_price.as_decimal()
        if order.order_type == OrderType.LIMIT:
            return order.price.as_decimal()
        return None

    def _since_ms(self, lookback_mins: int | None) -> int:
        if lookback_mins is None:
            lookback_mins = self._config.reconciliation_default_lookback_mins
        return self._clock.timestamp_ms() - lookback_mins * _MINUTE_MS

    def _command_since_ms(self, start) -> int:
        return self._since_ms(self._lookback_mins) if start is None else _ms(start)

    def _hold_buffer(self) -> None:
        # A rebuild that starts while another holds the buffer joins it.
        if self._buffer is None:
            self._buffer = []

    def _await_reconciliation(self, report_id: UUID4) -> None:
        self._stop_awaiting()
        self._awaited_report = report_id
        self._release_timer = self._loop.call_later(
            self._reconciled_wait_secs, self._reconciliation_waited
        )

    def _stop_awaiting(self) -> None:
        if self._release_timer is not None:
            self._release_timer.cancel()
            self._release_timer = None
        self._awaited_report = None

    def _on_reconciled(self, mass_status: ExecutionMassStatus) -> None:
        # Nautilus publishes a mass status more than once; the first match releases.
        if self._awaited_report is not None and mass_status.id == self._awaited_report:
            self._release_buffer()

    def _reconciliation_waited(self) -> None:
        self._release_timer = None
        self._log.warning(
            "Nautilus did not reconcile the mass status within "
            f"{self._reconciled_wait_secs:g}s; applying the execution events held meanwhile",
        )
        self._release_buffer()

    def _release_buffer(self) -> None:
        """Apply the execution events held meanwhile, in order; a second call does nothing."""
        self._stop_awaiting()
        held, self._buffer = self._buffer, None
        # A client detached meanwhile must start nothing; its next connect rebuilds anew.
        if held is None or self._session is None:
            return
        for event in held:
            self._on_execution_event(event)
        # A protective order that came during an outage arrives with the rebuild, no event.
        self._settle_brackets()

    def _write_exposure(self) -> None:
        """Write what stands on unloaded symbols to the cache, whole.

        The key is the application's account of it; a rebuild publishes no activity.
        """
        self._cache.add(
            UNLOADED_EXPOSURE_KEY,
            reports.exposure_json(self._book.exposure(), self._symbol_name),
        )

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
        """Rebuild the venue model from the broker's open positions and their order lists.

        Execution events that arrive meanwhile are applied once it stands, in order.
        """
        self._hold_buffer()
        try:
            self._stand(await self._read_broker(since_ms=None, deals=False))
        finally:
            self._release_buffer()
        self._write_exposure()

    async def _read_broker(
        self,
        *,
        since_ms: int | None,
        positions: Iterable[int] | None = None,
        deals: bool = True,
    ) -> BrokerState:
        """One read of the broker: the snapshot, then each covered position's lists, once each.

        - `since_ms`: the start of the fill window; the positions its deals name are covered
          too. `None` reads no window, so only the open positions.
        - `positions`: exactly these positions are covered instead.
        - `deals`: `False` reads only the order lists, all the venue model needs.

        A position on a symbol not loaded gets no deal list, as nothing is reported from it, and
        a closed one no list at all.
        """
        requests = 0

        async def historical(payload: Message) -> Message:
            nonlocal requests
            requests += 1
            return await self._request(payload, bucket=BUCKET_HISTORICAL)

        account_id = self._account.account_id
        snapshot = await self._request(
            oa.ProtoOAReconcileReq(ctidTraderAccountId=account_id, returnProtectionOrders=True),
        )
        requests += 1
        window: dict[int, om.ProtoOADeal] = {}
        if since_ms is not None:
            complete = True
            for start, end in history.weekly_windows(since_ms, self._clock.timestamp_ms()):
                found, done = await history.deals_between(historical, account_id, start, end)
                complete &= done
                window.update((deal.dealId, deal) for deal in found)
            if not complete:
                self._log.warning(
                    "The deal list of the fill window did not end; a position traded early in "
                    "the window may be missing from the reports",
                )
        symbols = {
            position.positionId: position.tradeData.symbolId for position in snapshot.position
        }
        open_ids = set(symbols)
        for deal in window.values():
            symbols.setdefault(deal.positionId, deal.symbolId)
        covered = sorted(symbols) if positions is None else list(dict.fromkeys(positions))
        histories: dict[int, PositionHistory] = {}
        for position_id in covered:
            symbol_id = symbols.get(position_id)
            loaded = symbol_id is None or self._price_precision(symbol_id) is not None
            if not loaded and position_id not in open_ids:
                continue
            orders = await self._position_orders(position_id, historical)
            found = (
                await history.position_deals(historical, account_id, position_id)
                if deals and loaded
                else []
            )
            histories[position_id] = PositionHistory(tuple(orders), tuple(found))
        return BrokerState(snapshot, histories, tuple(window.values()), requests)

    def _stand(self, state: BrokerState) -> None:
        """Rebuild the venue model and the margins from `state`'s snapshot."""
        orders = {position_id: found.orders for position_id, found in state.histories.items()}
        self._handle_records(self._book.load(state.snapshot, orders))
        self._margins = {}
        self._margin_times = {}
        for position in state.snapshot.position:
            self._set_margin(position)

    async def _position_orders(
        self, position_id: int, request: history.Request
    ) -> list[om.ProtoOAOrder]:
        """Every order of one position: its entry tells whose position it is."""
        found: dict[int, om.ProtoOAOrder] = {}
        payload = oa.ProtoOAOrderListByPositionIdReq(
            ctidTraderAccountId=self._account.account_id,
            positionId=position_id,
        )
        for _ in range(_MAX_ORDER_PAGES):
            response = await request(payload)
            new = [order for order in response.order if order.orderId not in found]
            found.update((order.orderId, order) for order in new)
            if not response.hasMore or not new:
                break
            # TODO(verify): paging backwards by `toTimestamp`; no position with more orders than
            # one page was recorded.
            payload.toTimestamp = min(order.utcLastUpdateTimestamp for order in new)
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

    def _take_trader(self, trader: om.ProtoOATrader) -> None:
        # A read taken before a newer closing deal or deposit was applied must not undo it.
        self._set_balance(
            trader.balance,
            trader,
            trader.balanceVersion if trader.HasField("balanceVersion") else None,
        )

    def _set_balance(self, amount: int, message: Message, version: int | None) -> bool:
        """Take a balance the broker reports; returns whether it changed.

        Events can be applied out of order, so a balance older than the one held is ignored.
        """
        if version is not None:
            if version <= self._balance_version:
                return False
            self._balance_version = version
        balance = self._money(amount, message)
        if balance == self._balance:
            return False
        self._balance = balance
        return True

    def _update_account(self, event: oa.ProtoOAExecutionEvent) -> None:
        """The account state an execution event carries: a margin, or a balance after a change."""
        changed = False
        ts_ms: int | None = None
        if event.HasField("position"):
            changed |= self._set_margin(event.position)
        if event.HasField("deal") and event.deal.HasField("closePositionDetail"):
            detail = event.deal.closePositionDetail
            version = detail.balanceVersion if detail.HasField("balanceVersion") else None
            changed |= self._set_balance(detail.balance, detail, version)
            ts_ms = event.deal.executionTimestamp
        if event.HasField("depositWithdraw"):
            operation = event.depositWithdraw
            version = operation.balanceVersion if operation.HasField("balanceVersion") else None
            changed |= self._set_balance(operation.balance, operation, version)
            ts_ms = operation.changeBalanceTimestamp
        if changed:
            self._emit_account_state(
                self._clock.timestamp_ns() if ts_ms is None else reports.nanos(ts_ms),
            )

    def _on_trader_updated(self, event: oa.ProtoOATraderUpdatedEvent) -> None:
        # TODO(verify): no trader update was recorded; its balance is read as the trader's.
        trader = event.trader
        version = trader.balanceVersion if trader.HasField("balanceVersion") else None
        if self._set_balance(trader.balance, trader, version):
            self._emit_account_state(self._clock.timestamp_ns())

    def _on_margin_changed(self, event: oa.ProtoOAMarginChangedEvent) -> None:
        # TODO(verify): no margin change was recorded; the positions' own events carry margins.
        known = self._margins.get(event.positionId)
        if known is None:
            # The event names no symbol; a position not yet seen states its margin with its own
            # next event.
            return
        margin = self._money(event.usedMargin, event)
        if known[1] != margin:
            self._margins[event.positionId] = (known[0], margin)
            self._emit_account_state(self._clock.timestamp_ns())

    async def _query_account(self, command: QueryAccount) -> None:
        self._take_trader(await self._trader())
        self._emit_account_state(self._clock.timestamp_ns())

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
        if any(isinstance(r, Activity) and r.kind in _EXPOSURE_KINDS for r in records):
            self._write_exposure()
        self._update_account(event)
        self._settle_brackets()
        self._drop_amend_locks()
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
                elif isinstance(record, (AwaitProtection, ProtectionMissing)):
                    self._on_protection(record)
                else:
                    self._log.warning(f"{type(record).__name__} is not a known record; ignored")
            except Exception as e:
                self._log.exception(f"{type(record).__name__} could not be reported", e)

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
        if kind in (OrderEventKind.REJECTED, OrderEventKind.CANCELED, OrderEventKind.EXPIRED):
            bracket = (
                None
                if record.client_order_id is None
                else self._brackets.by_entry(record.client_order_id)
            )
            if bracket is not None and not any(
                self._leg_alive(leg_id) for leg_id in bracket.legs.values()
            ):
                self._end_bracket(bracket.entry_id, record.reason or f"the entry {kind.value}")

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

    # -- Orders -----------------------------------------------------------------------------------

    async def _submit_order(self, command: SubmitOrder) -> None:
        if command.order.is_reduce_only:
            await self._close(command)
        else:
            await self._open([command.order], bracket=False)

    async def _submit_order_list(self, command: SubmitOrderList) -> None:
        await self._open(list(command.order_list.orders), bracket=True)

    async def _open(self, orders: list[Order], *, bracket: bool) -> None:
        entry = orders[0]
        instrument = self._instrument_provider.find(entry.instrument_id)
        try:
            if instrument is None:
                raise Unsupported(f"instrument {entry.instrument_id} is not loaded")
            if bracket:
                bid, ask = self._reference(instrument)
                built = order_translation.bracket(
                    self._account.account_id, instrument, orders, bid=bid, ask=ask
                )
                request = built.request
            else:
                request = order_translation.market_order(
                    self._account.account_id, instrument, entry
                )
        except Unsupported as e:
            self._refuse(orders, str(e))
            return
        if not self._connected():
            self._refuse(orders, _NOT_CONNECTED)
            return
        entry_id = entry.client_order_id.value
        legs: dict[Level, str] = {}
        if bracket:
            requested: dict[Level, Decimal] = {}
            for level, leg_id, price in (
                (Level.STOP_LOSS, built.stop_loss_id, built.stop_loss),
                (Level.TAKE_PROFIT, built.take_profit_id, built.take_profit),
            ):
                if leg_id is not None:
                    legs[level] = leg_id.value
                    requested[level] = price.as_decimal()
            # Before the first await, so a cancel or modify of a leg finds it.
            self._brackets.add(PendingBracket(entry_id, legs, requested))
        ts = self._clock.timestamp_ns()
        for order in orders:
            self.generate_order_submitted(
                order.strategy_id, order.instrument_id, order.client_order_id, ts
            )
        self._log.info(f"Order {entry_id} sent")
        outcome = await self._send(request)
        if isinstance(outcome, _Refused):
            self._entry_refused(
                entry_id,
                LegIds(legs.get(Level.STOP_LOSS), legs.get(Level.TAKE_PROFIT)),
                outcome.reason,
            )
        elif outcome is None:
            self._log.warning(
                f"Order {entry_id}: no answer, so its outcome is unknown; it is not resent",
            )
        else:
            self._on_execution_event(outcome)

    async def _close(self, command: SubmitOrder) -> None:
        order = command.order
        try:
            position_id, side = self._position_to_close(command)
            request = order_translation.close_position(
                self._account.account_id, position_id, order, position_side=side
            )
        except Unsupported as e:
            self._refuse([order], str(e))
            return
        if not self._connected():
            self._refuse([order], _NOT_CONNECTED)
            return
        client_order_id = order.client_order_id.value
        # In flight before it leaves, so the broker's events of the close are matched to it.
        self._operations.begin_close(client_order_id, position_id, request.volume)
        self.generate_order_submitted(
            order.strategy_id,
            order.instrument_id,
            order.client_order_id,
            self._clock.timestamp_ns(),
        )
        self._log.info(f"Close {client_order_id} of position {position_id} sent")
        try:
            outcome = await self._send(request)
            if isinstance(outcome, _Refused):
                self._log.warning(f"Close {client_order_id} refused: {outcome.reason}")
                self.generate_order_rejected(
                    order.strategy_id,
                    order.instrument_id,
                    order.client_order_id,
                    outcome.reason,
                    self._clock.timestamp_ns(),
                )
            elif outcome is None:
                self._log.warning(
                    f"Close {client_order_id}: no answer, so its outcome is unknown; "
                    "it is not resent",
                )
            else:
                self._on_execution_event(outcome)
        finally:
            self._operations.end_close(client_order_id)

    def _position_to_close(self, command: SubmitOrder) -> tuple[int, PositionSide]:
        if command.position_id is None:
            raise Unsupported("on a hedging account a closing order must name its position")
        try:
            position_id = int(command.position_id.value)
        except ValueError:
            raise Unsupported(f"{command.position_id} is not a venue position") from None
        view = self._book.view(position_id)
        if view is None or not view.open:
            raise Unsupported(f"position {position_id} is not open at the venue")
        instrument = self._instrument_provider.instrument_for_symbol_id(view.symbol_id)
        if instrument is None or instrument.id != command.order.instrument_id:
            raise Unsupported(f"position {position_id} is not on {command.order.instrument_id}")
        return position_id, PositionSide.LONG if view.side == "BUY" else PositionSide.SHORT

    def _reference(self, instrument: Instrument) -> tuple[Decimal, Decimal]:
        """The latest bid and ask, when fresh enough to measure a bracket's levels from."""
        quote = self._quotes.get(instrument.info["symbol_id"])
        max_age = self._config.reference_price_max_age_secs
        if (
            quote is None
            or quote.bid is None
            or quote.ask is None
            or self._loop.time() - quote.at > max_age
        ):
            raise Unsupported(
                f"no price of {instrument.id} newer than {max_age:g}s to measure the levels from",
            )
        return quote.bid, quote.ask

    def _refuse(self, orders: list[Order], reason: str) -> None:
        """Refuse a command before anything is sent: the order rejected, its legs cancelled."""
        entry, legs = orders[0], orders[1:]
        self._log.warning(f"Order {entry.client_order_id} refused: {reason}")
        ts = self._clock.timestamp_ns()
        self.generate_order_rejected(
            entry.strategy_id, entry.instrument_id, entry.client_order_id, reason, ts
        )
        for leg in legs:
            self.generate_order_canceled(
                leg.strategy_id, leg.instrument_id, leg.client_order_id, None, ts
            )

    def _entry_refused(self, entry_id: str, legs: LegIds, reason: str) -> None:
        self._log.warning(f"Order {entry_id} refused: {reason}")
        self._end_bracket(entry_id, reason)
        self._handle_records(
            self._book.reject_entry(entry_id, legs, reason, self._clock.timestamp_ms()),
        )

    def _connected(self) -> bool:
        session = self._account.session
        return session is not None and session.is_ready

    async def _send(self, request: Message) -> Message | _Refused | None:
        """`request`'s response, a refusal, or `None` when its outcome is unknown."""
        session = self._account.session
        # Checked again here: the session can drop after a command was checked and submitted.
        if session is None or not session.is_ready:
            return _Refused(_NOT_CONNECTED, retryable=True)
        try:
            response = await session.request(
                request, timeout_secs=self._config.order_request_timeout_secs
            )
        except CTraderRequestError as e:
            return _Refused(
                _reason(e.error_code, e.description),
                retryable=e.retry_after_secs is not None,
            )
        except (CTraderTimeoutError, CTraderConnectionError):
            return None
        if isinstance(response, oa.ProtoOAOrderErrorEvent):
            # Not an error response to the transport: it arrives as the request's answer.
            return _Refused(_reason(response.errorCode, response.description or None))
        return response

    def _on_protection(self, record: AwaitProtection | ProtectionMissing) -> None:
        if isinstance(record, AwaitProtection):

            def waited() -> None:
                self._protection_timers.discard(timer)
                self._protection_waited(record.position_id)

            timer = self._loop.call_later(self._config.protective_order_timeout_secs, waited)
            self._protection_timers.add(timer)
        else:
            self.create_task(
                self._set_missing_levels(record),
                log_msg=f"set the levels of position {record.position_id}",
            )

    def _protection_waited(self, position_id: int) -> None:
        self._handle_records(self._book.protection_timed_out(position_id))

    def _end_bracket(self, entry_id: str, reason: str) -> None:
        """Forget a bracket whose levels will never be corrected; a modify waiting on it fails."""
        bracket = self._brackets.by_entry(entry_id)
        if bracket is None:
            return
        self._brackets.remove(entry_id)
        for level in sorted(bracket.modified - bracket.cancels, key=lambda lv: lv.value):
            self._modify_rejected(ClientOrderId(bracket.legs[level]), reason)

    # -- Legs -------------------------------------------------------------------------------------

    async def _cancel_order(self, command: CancelOrder) -> None:
        await self._cancel([command.client_order_id])

    async def _cancel_all_orders(self, command: CancelAllOrders) -> None:
        selection = {
            "instrument_id": command.instrument_id,
            "strategy_id": command.strategy_id,
            "side": command.order_side,
        }
        orders = self._cache.orders_open(**selection) + self._cache.orders_inflight(**selection)
        # A pending cancel or update is both open and in flight; it is answered once.
        await self._cancel(list(dict.fromkeys(order.client_order_id for order in orders)))

    async def _batch_cancel_orders(self, command: BatchCancelOrders) -> None:
        await self._cancel([cancel.client_order_id for cancel in command.cancels])

    async def _cancel(self, client_order_ids: list[ClientOrderId]) -> None:
        """Cancel legs, one amend per position; anything else is refused."""
        by_position: dict[int, dict[Level, ClientOrderId]] = {}
        for client_order_id in client_order_ids:
            pending = self._brackets.by_leg(client_order_id.value)
            if pending is not None:
                bracket, level = pending
                # Carried by the bracket's correcting amend.
                bracket.cancels.add(level)
                continue
            found = self._book.leg_position(client_order_id.value)
            if found is None:
                self._cancel_rejected(
                    client_order_id,
                    "only a protective leg can be cancelled; a market order fills at once",
                )
            elif not self._leg_alive(client_order_id.value):
                self._cancel_rejected(client_order_id, "the leg is already closed")
            else:
                position_id, level = found
                by_position.setdefault(position_id, {})[level] = client_order_id
        await asyncio.gather(
            *(self._remove_levels(position_id, legs) for position_id, legs in by_position.items()),
        )

    async def _remove_levels(self, position_id: int, legs: dict[Level, ClientOrderId]) -> None:
        outcome = await self._amend(
            position_id,
            lambda view: {lv: p for lv, p in view.levels.items() if lv not in legs},
        )
        if isinstance(outcome, _Refused):
            for client_order_id in legs.values():
                self._cancel_rejected(client_order_id, outcome.reason)
            return
        # Answered from what the broker holds now, not from what was asked.
        view = self._book.view(position_id)
        ts_ms = self._clock.timestamp_ms()
        for level, client_order_id in legs.items():
            if view is not None and level in view.levels:
                self._cancel_rejected(client_order_id, "the broker kept the level")
            else:
                # A level the broker did not hold was not in its answer: nothing cancelled the leg.
                self._handle_records(self._book.cancel_leg(position_id, level, ts_ms))

    async def _modify_order(self, command: ModifyOrder) -> None:
        client_order_id = command.client_order_id
        order = self._cache.order(client_order_id)
        pending = self._brackets.by_leg(client_order_id.value)
        found = self._book.leg_position(client_order_id.value)
        if order is None or (pending is None and found is None):
            self._modify_rejected(client_order_id, "only a protective leg can be modified")
            return
        level = pending[1] if pending is not None else found[1]
        if pending is None and not self._leg_alive(client_order_id.value):
            self._modify_rejected(client_order_id, "the leg is already closed")
            return
        held = self._leg_quantity(order, found)
        if command.quantity is not None and command.quantity != held:
            self._modify_rejected(
                client_order_id,
                f"a protective leg's quantity follows its position ({held}) and cannot be set",
            )
            return
        price = command.trigger_price if level == Level.STOP_LOSS else command.price
        if price is None:
            # Only the quantity the leg already has: nothing to send.
            self.generate_order_updated(
                order.strategy_id,
                order.instrument_id,
                client_order_id,
                order.venue_order_id,
                held,
                None,
                None,
                self._clock.timestamp_ns(),
            )
            return
        if pending is not None:
            bracket = pending[0]
            bracket.requested[level] = price.as_decimal()
            bracket.modified.add(level)
            return
        wanted = price.as_decimal()
        outcome = await self._amend(found[0], lambda view: {**view.levels, level: wanted})
        if isinstance(outcome, _Refused):
            self._modify_rejected(client_order_id, outcome.reason)
            return
        view = self._book.view(found[0])
        held_price = None if view is None else view.levels.get(level)
        if held_price != wanted:
            reason = (
                "the broker holds no such level"
                if held_price is None
                else f"the broker kept the level at {held_price}"
            )
            self._modify_rejected(client_order_id, reason)
            return
        updated = any(
            isinstance(r, OrderEvent)
            and r.kind == OrderEventKind.UPDATED
            and r.client_order_id == client_order_id.value
            for r in outcome
        )
        if not updated:
            # The level stood there already, so no event says so; the modify still needs one.
            self._leg_updated(client_order_id, level, wanted)

    def _leg_alive(self, leg_id: str) -> bool:
        found = self._book.leg_position(leg_id)
        if found is None:
            return False
        view = self._book.view(found[0])
        return view is not None and view.legs.get(found[1], (leg_id, False))[1]

    def _leg_quantity(self, order: Order, found: tuple[int, Level] | None) -> Quantity:
        """What the leg holds: the protective order's volume once accepted, else its own."""
        if found is not None:
            view = self._book.view(found[0])
            units = None if view is None else view.leg_units.get(found[1])
            if units is not None:
                instrument = self._instrument_provider.find(order.instrument_id)
                return reports.quantity(units, instrument)
        return order.quantity

    def _leg_updated(self, client_order_id: ClientOrderId, level: Level, value: Decimal) -> None:
        order = self._cache.order(client_order_id)
        instrument = self._instrument_provider.find(order.instrument_id)
        price = reports.price(value, instrument)
        stop = level == Level.STOP_LOSS
        self.generate_order_updated(
            order.strategy_id,
            order.instrument_id,
            client_order_id,
            order.venue_order_id,
            order.quantity,
            None if stop else price,
            price if stop else None,
            self._clock.timestamp_ns(),
        )

    def _cancel_rejected(self, client_order_id: ClientOrderId, reason: str) -> None:
        order = self._cache.order(client_order_id)
        if order is None:
            self._log.warning(f"Cancel of {client_order_id} refused ({reason}); no such order")
            return
        self.generate_order_cancel_rejected(
            order.strategy_id,
            order.instrument_id,
            client_order_id,
            order.venue_order_id,
            reason,
            self._clock.timestamp_ns(),
        )

    def _modify_rejected(self, client_order_id: ClientOrderId, reason: str) -> None:
        order = self._cache.order(client_order_id)
        if order is None:
            self._log.warning(f"Modify of {client_order_id} refused ({reason}); no such order")
            return
        self.generate_order_modify_rejected(
            order.strategy_id,
            order.instrument_id,
            client_order_id,
            order.venue_order_id,
            reason,
            self._clock.timestamp_ns(),
        )

    # -- Amends -----------------------------------------------------------------------------------

    async def _amend(
        self,
        position_id: int,
        levels_of: Callable[[PositionView], dict[Level, Decimal]],
    ) -> list[Record] | _Refused:
        """Set the position's levels to `levels_of(view)`; returns the records of the answer.

        `levels_of` runs once the amends of the position before it are answered, so it sees
        the levels they left and never undoes them.
        """
        async with self._amend_locks.setdefault(position_id, asyncio.Lock()):
            view = self._book.view(position_id)
            if view is None or not view.open:
                return _Refused(f"position {position_id} is not open at the venue")
            levels = levels_of(view)
            if levels == view.levels:
                return []
            instrument = self._instrument_provider.instrument_for_symbol_id(view.symbol_id)
            request = order_translation.amend_levels(
                self._account.account_id,
                position_id,
                stop_loss=self._level_price(levels, Level.STOP_LOSS, instrument),
                take_profit=self._level_price(levels, Level.TAKE_PROFIT, instrument),
            )
            self._operations.begin_amend(position_id)
            try:
                outcome = await self._send_amend(request)
                if isinstance(outcome, _Refused):
                    return outcome
                if isinstance(outcome, oa.ProtoOAExecutionEvent):
                    return self._on_execution_event(outcome)
                return []
            finally:
                self._operations.end_amend(position_id)

    def _drop_amend_locks(self) -> None:
        """Forget the lock of each position no longer open; an amend there is refused anyway."""
        for position_id, lock in list(self._amend_locks.items()):
            view = self._book.view(position_id)
            if not lock.locked() and (view is None or not view.open):
                del self._amend_locks[position_id]

    @staticmethod
    def _level_price(
        levels: dict[Level, Decimal],
        level: Level,
        instrument: Instrument | None,
    ) -> Price | None:
        value = levels.get(level)
        return None if value is None else reports.price(value, instrument)

    async def _send_amend(self, request: oa.ProtoOAAmendPositionSLTPReq) -> Message | _Refused:
        # TODO(verify): whether an accepted amend sends an execution event besides its answer; a
        # later one would be read as a trader's change once the amend is no longer in flight.
        outcome: Message | _Refused | None = None
        for _ in range(_AMEND_ATTEMPTS):
            outcome = await self._send(request)
            if outcome is not None and not (isinstance(outcome, _Refused) and outcome.retryable):
                return outcome
            await self._wait_ready()
        return outcome if isinstance(outcome, _Refused) else _Refused("the amend got no answer")

    async def _wait_ready(self) -> None:
        session = self._account.session
        if session is None:
            return
        with contextlib.suppress(TimeoutError):
            await session.wait_ready(timeout_secs=self._config.connect_timeout_secs)

    # -- Bracket levels ---------------------------------------------------------------------------

    def _settle_brackets(self) -> None:
        """Start the correcting amend of each bracket whose protective order has come."""
        for bracket in self._brackets:
            if bracket.correcting:
                continue
            view = self._bracket_position(bracket)
            if view is None or view.protective_order_id is None:
                continue
            if self._wanted_levels(bracket, view) == view.levels:
                self._bracket_done(bracket, view)
            elif bracket.rounds >= _CORRECTION_ROUNDS:
                self._bracket_refused(
                    bracket, view.position_id, "the broker kept other levels than asked"
                )
            else:
                bracket.correcting = True
                bracket.rounds += 1
                self.create_task(
                    self._correct(bracket, view.position_id),
                    log_msg=f"set the levels of {bracket.entry_id}",
                )

    def _bracket_position(self, bracket: PendingBracket) -> PositionView | None:
        for leg_id in bracket.legs.values():
            found = self._book.leg_position(leg_id)
            if found is not None:
                return self._book.view(found[0])
        return None

    @staticmethod
    def _wanted_levels(bracket: PendingBracket, view: PositionView) -> dict[Level, Decimal]:
        """The broker's levels, with each live leg's at its asked price and a cancelled one gone.

        The level of a leg the model holds dead stays as the broker holds it: a rebuild marks a
        leg dead whose level had not come yet.
        """
        levels = dict(view.levels)
        for level, leg_id in bracket.legs.items():
            if level in bracket.cancels:
                levels.pop(level, None)
            elif view.legs.get(level, (leg_id, False))[1]:
                levels[level] = bracket.requested[level]
        return levels

    async def _correct(self, bracket: PendingBracket, position_id: int) -> None:
        try:
            outcome = await self._amend(
                position_id, lambda view: self._wanted_levels(bracket, view)
            )
        finally:
            bracket.correcting = False
        if isinstance(outcome, _Refused):
            self._bracket_refused(bracket, position_id, outcome.reason)
            return
        # Checked again: a cancel or modify may have come while the amend was out.
        self._settle_brackets()

    def _bracket_done(self, bracket: PendingBracket, view: PositionView) -> None:
        """The broker holds the levels asked for: what waited on that is answered."""
        self._brackets.remove(bracket.entry_id)
        ts_ms = self._clock.timestamp_ms()
        for level in bracket.cancels:
            self._handle_records(self._book.cancel_leg(view.position_id, level, ts_ms))
        for level in bracket.modified - bracket.cancels:
            if level in view.levels:
                self._leg_updated(ClientOrderId(bracket.legs[level]), level, view.levels[level])

    def _bracket_refused(self, bracket: PendingBracket, position_id: int, reason: str) -> None:
        """The levels stay where the broker set them, and every leg says where that is."""
        self._log.warning(
            f"Levels of position {position_id} stay where the broker set them: {reason}",
        )
        self._brackets.remove(bracket.entry_id)
        view = self._book.view(position_id)
        for level, leg_id in bracket.legs.items():
            client_order_id = ClientOrderId(leg_id)
            if level in bracket.cancels:
                self._cancel_rejected(client_order_id, reason)
            elif level in bracket.modified:
                self._modify_rejected(client_order_id, reason)
            if (
                view is not None
                and view.legs.get(level, (leg_id, False))[1]
                and level in view.levels
            ):
                self._leg_updated(client_order_id, level, view.levels[level])

    async def _set_missing_levels(self, record: ProtectionMissing) -> None:
        """No protective order followed the fill: set the levels the legs asked for."""
        position_id = record.position_id
        self._log.warning(
            f"No protective order followed the fill of position {position_id} within "
            f"{self._config.protective_order_timeout_secs:g}s; setting its levels by an amend",
        )
        missing = {
            level: leg_id
            for level, leg_id in (
                (Level.STOP_LOSS, record.stop_loss_id),
                (Level.TAKE_PROFIT, record.take_profit_id),
            )
            if leg_id is not None
        }
        pending = self._brackets.by_leg(next(iter(missing.values())))
        if pending is None:
            self._levels_refused(position_id, "the levels asked for are no longer known")
            return
        bracket = pending[0]
        ts_ms = self._clock.timestamp_ms()
        for level in bracket.cancels:
            self._handle_records(self._book.cancel_leg(position_id, level, ts_ms))
        if all(level in bracket.cancels for level in missing):
            # Nothing to set; the bracket stays, for its correction to answer what waits on it.
            return

        def levels_of(view: PositionView) -> dict[Level, Decimal]:
            # Read under the amend lock, so a modify that came meanwhile is not undone.
            wanted = {
                level: bracket.requested[level] for level in missing if level not in bracket.cancels
            }
            return {**view.levels, **wanted}

        outcome = await self._amend(position_id, levels_of)
        if isinstance(outcome, _Refused):
            self._end_bracket(bracket.entry_id, outcome.reason)
            self._levels_refused(position_id, outcome.reason)
            return
        self._settle_brackets()

    def _levels_refused(self, position_id: int, reason: str) -> None:
        self._log.error(f"Position {position_id} stands without its levels: {reason}")
        self._handle_records(
            self._book.reject_legs(position_id, reason, self._clock.timestamp_ms()),
        )

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
