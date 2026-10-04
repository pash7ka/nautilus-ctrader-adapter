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
from nautilus_trader.model.enums import AccountType, OmsType
from nautilus_trader.model.identifiers import AccountId, ClientId, InstrumentId
from nautilus_trader.model.objects import Currency

from nautilus_ctrader.common import execution_reports as reports
from nautilus_ctrader.common.account import CTraderAccountClient
from nautilus_ctrader.common.errors import (
    CTraderAccountError,
    CTraderConnectionError,
    CTraderRequestError,
)
from nautilus_ctrader.common.parsing import PRICE_SCALE
from nautilus_ctrader.common.session import CTraderSession
from nautilus_ctrader.common.venue_book import VenueBook
from nautilus_ctrader.common.venue_records import money_of
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
            trader = await self._trader()
            check_account(trader)
            self._currency = Currency.from_str(self._account.deposit_asset.name)
            self._balance = money_of(trader.balance, trader.moneyDigits)
            self._balance_version = trader.balanceVersion
            await self._load()
            self._emit_account_state(self._clock.timestamp_ns())
            await self._hold_reference_spots()
        except BaseException:
            await self._disconnect()
            raise

    async def _disconnect(self) -> None:
        self._session = None
        try:
            await self._release_reference_spots()
        finally:
            if self._holds_account:
                self._holds_account = False
                await self._account.disconnect()

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
        """Rebuild the venue model from the broker's open positions and orders."""
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
        for notice in self._book.load(snapshot, position_orders):
            self._log.warning(notice.text)
        self._margins = {}
        self._margin_times = {}
        for position in snapshot.position:
            self._set_margin(position)

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
