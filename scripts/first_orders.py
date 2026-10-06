"""Send a short, fixed sequence of real orders through the adapter, for the account owner.

**This script places real orders on a real account.** It is for the account owner to run by
hand, watching the trading terminal, and only with `--send-live-orders`; it then asks for the
symbol to be typed again before anything starts. Nothing in the test suite runs it.

It builds a `TradingNode` with this adapter's data and execution clients and, on one symbol,
does exactly this:

1. waits for a quote;
2. buys the instrument's minimum volume with a bracket: a stop-loss `--distance` below the ask
   and a take-profit `--distance` above it;
3. once both levels are accepted, cancels the take-profit;
4. once that is cancelled, closes the position;
5. once the position is closed, buys a second bracket the same way, of the minimum volume plus
   one size step, so that one size step can be closed by hand and the minimum still remains;
6. asks the owner to move the stop-loss and to close one size step of the position by hand
   (the prompt names the exact volume);
7. waits, up to `--manual-wait-secs`, until the adapter reports both changes and Nautilus's
   position shows the smaller volume;
8. closes the rest and stops.

A refusal, or a position that ends some other way, stops the sequence with a message; anything
left open is then the owner's to close in the terminal. Ctrl+C stops the node the way Nautilus
stops it.

    uv run python scripts/first_orders.py --symbol EURUSD --distance 0.00100 --send-live-orders

Credentials and the trader login come from `.env` in the repository root. The script does no
token refresh, so it refuses to start with an access token whose `CTRADER_TOKEN_EXPIRES_AT` has
passed. Every order event, position event and account activity is appended to a JSONL file under
`--log-dir` (by default `tests/recordings/`, which git ignores). A line carries only the fixed
list of the event's fields, which includes the account id, never the whole event. Run
`scripts/record_execution.py` alongside it: the checklist printed at the end says what to
compare between the two.
"""

from __future__ import annotations

import argparse
import datetime
import enum
import importlib.util
import json
import math
import pathlib
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import TextIO

from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core.datetime import unix_nanos_to_iso8601
from nautilus_trader.live.config import LiveExecEngineConfig, TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderSide, OrderType
from nautilus_trader.model.events import (
    OrderAccepted,
    OrderCanceled,
    OrderCancelRejected,
    OrderDenied,
    OrderEvent,
    OrderExpired,
    OrderRejected,
    PositionChanged,
    PositionClosed,
    PositionEvent,
    PositionOpened,
)
from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId, PositionId, Symbol
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Quantity
from nautilus_trader.trading.config import StrategyConfig
from nautilus_trader.trading.strategy import Strategy

from nautilus_ctrader import (
    ACCOUNT_ACTIVITY_TOPIC,
    CTRADER,
    CTRADER_VENUE,
    CTraderAccountActivity,
    CTraderDataClientConfig,
    CTraderExecClientConfig,
    CTraderLiveDataClientFactory,
    CTraderLiveExecClientFactory,
)

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

# scripts/ is not a package; get_tokens.py is loaded by file path, as verify_live_data.py does.
_GET_TOKENS_SPEC = importlib.util.spec_from_file_location(
    "get_tokens",
    pathlib.Path(__file__).resolve().with_name("get_tokens.py"),
)
get_tokens = importlib.util.module_from_spec(_GET_TOKENS_SPEC)
sys.modules[_GET_TOKENS_SPEC.name] = get_tokens
_GET_TOKENS_SPEC.loader.exec_module(get_tokens)

_TRADER_LOGIN_KEY = "CTRADER_TRADER_LOGIN"
_MANUAL_ACTIONS = frozenset({"level_moved", "partially_closed"})
_MANUAL_WAIT_ALERT = "first-orders-manual-wait"


# -- Actions and the events only this script makes ----------------------------------------


@dataclass(frozen=True)
class SubmitBracket:
    """Buy `quantity` with these two levels."""

    quantity: Quantity
    stop_loss: Decimal
    take_profit: Decimal


@dataclass(frozen=True)
class CancelLeg:
    client_order_id: ClientOrderId


@dataclass(frozen=True)
class ClosePosition:
    position_id: PositionId


@dataclass(frozen=True)
class Prompt:
    """Text for the owner. With `wait_secs`, `ManualWaitExpired` is due that long after it."""

    text: str
    wait_secs: float | None = None


@dataclass(frozen=True)
class Done:
    pass


Action = SubmitBracket | CancelLeg | ClosePosition | Prompt | Done


@dataclass(frozen=True)
class BracketSubmitted:
    """The ids of the bracket just built for a `SubmitBracket`, before it is sent."""

    entry: ClientOrderId
    stop_loss: ClientOrderId
    take_profit: ClientOrderId


@dataclass(frozen=True)
class ManualWaitExpired:
    pass


# -- The step driver ----------------------------------------------------------------------


@dataclass(frozen=True)
class Sizes:
    first: Quantity
    second: Quantity
    # The volume the owner closes by hand, as the prompt states it.
    partial_close: str


def bracket_sizes(instrument: Instrument) -> Sizes:
    """The minimum volume for the first bracket; one size step more for the second.

    Where the size step equals the minimum, a position of the minimum cannot be partly closed,
    so the second is sized for a hand close of one step to leave the minimum standing.
    """
    step = instrument.size_increment
    first = instrument.min_quantity if instrument.min_quantity is not None else step
    second = instrument.make_qty(first.as_decimal() + step.as_decimal())
    base = getattr(instrument, "base_currency", None)
    partial = f"{step} {base.code if base is not None else 'units'}"
    if instrument.lot_size is not None:
        lots = (step.as_decimal() / instrument.lot_size.as_decimal()).normalize()
        partial += f" ({lots:f} lots)"
    return Sizes(first=first, second=second, partial_close=partial)


class _Stage(enum.Enum):
    WAIT_QUOTE = enum.auto()
    FIRST_BRACKET = enum.auto()
    CANCELLING_TAKE_PROFIT = enum.auto()
    CLOSING_FIRST = enum.auto()
    SECOND_BRACKET = enum.auto()
    MANUAL = enum.auto()
    CLOSING_SECOND = enum.auto()
    DONE = enum.auto()


class Steps:
    """The fixed sequence as a state machine: events in, actions out, no I/O and no clock.

    It is fed every quote tick, order event, position event and account activity of the run,
    plus `BracketSubmitted` and `ManualWaitExpired`, and only ever acts on the bracket it last
    asked for.
    """

    def __init__(self, *, distance: Decimal, manual_wait_secs: float, sizes: Sizes) -> None:
        self._distance = distance
        self._manual_wait_secs = manual_wait_secs
        self._sizes = sizes
        self._stage = _Stage.WAIT_QUOTE
        self._ask: Decimal | None = None
        self._bracket: BracketSubmitted | None = None
        self._accepted: set[ClientOrderId] = set()
        self._position_id: PositionId | None = None
        self._quantity: Quantity | None = None
        self._seen: set[str] = set()

    @property
    def finished(self) -> bool:
        return self._stage is _Stage.DONE

    def on(self, event: object) -> list[Action]:
        if self._stage is _Stage.DONE:
            return []
        if isinstance(event, QuoteTick):
            return self._on_quote(event)
        if isinstance(event, BracketSubmitted):
            self._bracket = event
            self._accepted.clear()
            self._position_id = None
            return []
        if isinstance(event, (OrderRejected, OrderDenied)):
            return self._stop(f"Order {event.client_order_id} was refused: {event.reason}.")
        if isinstance(event, OrderCancelRejected):
            return self._stop(f"Cancelling {event.client_order_id} was refused: {event.reason}.")
        if isinstance(event, CTraderAccountActivity):
            return self._on_activity(event)
        if isinstance(event, ManualWaitExpired):
            return self._on_manual_wait_expired()
        if self._bracket is None:
            return []
        return self._on_bracket_event(event, self._bracket)

    def _on_quote(self, tick: QuoteTick) -> list[Action]:
        self._ask = tick.ask_price.as_decimal()
        if self._stage is not _Stage.WAIT_QUOTE:
            return []
        self._stage = _Stage.FIRST_BRACKET
        return [self._submit(self._sizes.first)]

    def _submit(self, quantity: Quantity) -> SubmitBracket:
        # A buy fills at the ask, which is also what the adapter measures the levels from.
        return SubmitBracket(
            quantity=quantity,
            stop_loss=self._ask - self._distance,
            take_profit=self._ask + self._distance,
        )

    def _on_bracket_event(self, event: object, bracket: BracketSubmitted) -> list[Action]:
        client_order_id = getattr(event, "client_order_id", None)
        if isinstance(event, (OrderCanceled, OrderExpired)) and client_order_id == bracket.entry:
            return self._stop("The entry ended without a fill.")
        if isinstance(event, PositionOpened) and event.opening_order_id == bracket.entry:
            self._position_id = event.position_id
            self._quantity = event.quantity
            return []
        if isinstance(event, PositionChanged) and event.position_id == self._position_id:
            self._quantity = event.quantity
            return self._close_second_when_ready() if self._stage is _Stage.MANUAL else []
        if isinstance(event, PositionClosed) and event.position_id == self._position_id:
            return self._on_position_closed()
        if isinstance(event, OrderAccepted) and client_order_id in (
            bracket.stop_loss,
            bracket.take_profit,
        ):
            self._accepted.add(client_order_id)
            return self._on_leg_accepted(bracket) if len(self._accepted) == 2 else []
        if (
            isinstance(event, OrderCanceled)
            and client_order_id == bracket.take_profit
            and self._stage is _Stage.CANCELLING_TAKE_PROFIT
        ):
            if self._position_id is None:
                return self._stop("No position was reported for the entry.")
            self._stage = _Stage.CLOSING_FIRST
            return [ClosePosition(self._position_id)]
        return []

    def _on_leg_accepted(self, bracket: BracketSubmitted) -> list[Action]:
        if self._stage is _Stage.FIRST_BRACKET:
            self._stage = _Stage.CANCELLING_TAKE_PROFIT
            return [CancelLeg(bracket.take_profit)]
        if self._stage is _Stage.SECOND_BRACKET:
            self._stage = _Stage.MANUAL
            return [
                Prompt(
                    "Now, by hand in the trading terminal, on the position this script just "
                    "opened: move its stop-loss, then close "
                    f"{self._sizes.partial_close} of it. Waiting up to "
                    f"{self._manual_wait_secs:g} s for the adapter to report both.",
                    wait_secs=self._manual_wait_secs,
                ),
            ]
        return []

    def _on_position_closed(self) -> list[Action]:
        if self._stage is _Stage.CLOSING_FIRST:
            self._stage = _Stage.SECOND_BRACKET
            return [self._submit(self._sizes.second)]
        if self._stage is _Stage.CLOSING_SECOND:
            self._stage = _Stage.DONE
            return [Done()]
        return self._stop("The position closed before this script closed it.")

    def _on_activity(self, activity: CTraderAccountActivity) -> list[Action]:
        if self._stage is not _Stage.MANUAL or activity.kind != "manual_change":
            return []
        if activity.action in _MANUAL_ACTIONS:
            self._seen.add(activity.action)
        return self._close_second_when_ready()

    def _close_second_when_ready(self) -> list[Action]:
        """Close the rest once both changes are reported and the position shows the partial close.

        Closing on the activity alone could read the position's volume from before the fill.
        """
        if self._seen != _MANUAL_ACTIONS:
            return []
        reduced = self._quantity is not None and self._quantity < self._sizes.second
        if self._position_id is not None and not reduced:
            return []
        return self._close_second([])

    def _on_manual_wait_expired(self) -> list[Action]:
        if self._stage is not _Stage.MANUAL:
            return []
        missing = ", ".join(sorted(_MANUAL_ACTIONS - self._seen))
        return self._close_second(
            [Prompt(f"Not reported within the wait: {missing}. Closing the rest.")],
        )

    def _close_second(self, before: list[Action]) -> list[Action]:
        if self._position_id is None:
            return self._stop("No position was reported for the second entry.")
        self._stage = _Stage.CLOSING_SECOND
        return [*before, ClosePosition(self._position_id)]

    def _stop(self, reason: str) -> list[Action]:
        self._stage = _Stage.DONE
        return [Prompt(f"{reason} Stopping; close anything left open in the terminal."), Done()]


# -- The event log ------------------------------------------------------------------------

# A whitelist, so that a line holds named fields and never a dump of the whole event.
_LOGGED_FIELDS = (
    "account_id",
    "instrument_id",
    "client_order_id",
    "venue_order_id",
    "position_id",
    "trade_id",
    "opening_order_id",
    "closing_order_id",
    "order_side",
    "order_type",
    "side",
    "entry",
    "quantity",
    "last_qty",
    "last_px",
    "price",
    "trigger_price",
    "commission",
    "avg_px_open",
    "avg_px_close",
    "realized_pnl",
    "reason",
    "kind",
    "symbol",
    "subject",
    "volume",
    "action",
)


def _text(value: object) -> str:
    return value.name if isinstance(value, enum.Enum) else str(value)


def event_record(event: object, *, status: str | None = None) -> dict[str, object]:
    """The loggable fields of `event`."""
    record: dict[str, object] = {"event": type(event).__name__}
    ts_event = getattr(event, "ts_event", None)
    if ts_event is not None:
        record["ts_event"] = ts_event
        record["time"] = unix_nanos_to_iso8601(ts_event)
    if status is not None:
        record["status"] = status
    for name in _LOGGED_FIELDS:
        value = getattr(event, name, None)
        if value is None:
            continue
        record[name] = _text(value)
    return record


class EventLog:
    """Appends one JSON line per event to `path`, flushed at once."""

    def __init__(self, path: pathlib.Path) -> None:
        self.path = path
        self._file: TextIO | None = None

    def __enter__(self) -> EventLog:
        self._file = self.path.open("a", encoding="utf-8")
        return self

    def __exit__(self, *_exc: object) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    def write(self, event: object, *, status: str | None = None) -> None:
        record = event_record(event, status=status)
        self._file.write(json.dumps(record) + "\n")
        self._file.flush()


# -- The Nautilus side --------------------------------------------------------------------


class _Driver(Strategy):
    """Feeds the run's events to `Steps` and carries out what it answers.

    `Steps` is built in `on_start`, once the instrument, and so the volumes, are known.
    """

    def __init__(
        self,
        instrument_id: InstrumentId,
        *,
        distance: Decimal,
        manual_wait_secs: float,
        log: EventLog,
    ) -> None:
        super().__init__(StrategyConfig(order_id_tag="001"))
        self._instrument_id = instrument_id
        self._distance = distance
        self._manual_wait_secs = manual_wait_secs
        self._event_log = log
        self._instrument: Instrument | None = None
        self._steps: Steps | None = None

    @property
    def finished(self) -> bool:
        return self._steps is not None and self._steps.finished

    def on_start(self) -> None:
        self._instrument = self.cache.instrument(self._instrument_id)
        if self._instrument is None:
            print(f"\n>>> {self._instrument_id} did not load; nothing was sent.\n")
            self.shutdown_system("instrument not loaded")
            return
        sizes = bracket_sizes(self._instrument)
        self._steps = Steps(
            distance=self._distance,
            manual_wait_secs=self._manual_wait_secs,
            sizes=sizes,
        )
        print(
            f"\n>>> Volumes: {sizes.first}, then {sizes.second}. "
            f"Waiting for a quote of {self._instrument_id}.\n",
        )
        self.msgbus.subscribe(topic=ACCOUNT_ACTIVITY_TOPIC, handler=self._on_activity)
        self.subscribe_quote_ticks(self._instrument_id)

    def on_stop(self) -> None:
        if self._instrument is not None:
            self.msgbus.unsubscribe(topic=ACCOUNT_ACTIVITY_TOPIC, handler=self._on_activity)
        if not self.finished:
            print("\n>>> Stopped before the sequence finished: check the terminal.\n")

    def on_quote_tick(self, tick: QuoteTick) -> None:
        self._feed(tick)

    def on_event(self, event: object) -> None:
        if isinstance(event, OrderEvent):
            order = self.cache.order(event.client_order_id)
            self._event_log.write(event, status=None if order is None else order.status_string())
            self._feed(event)
        elif isinstance(event, PositionEvent):
            self._event_log.write(event)
            self._feed(event)

    def _on_activity(self, activity: object) -> None:
        if isinstance(activity, CTraderAccountActivity):
            self._event_log.write(activity)
            self._feed(activity)

    def _feed(self, event: object) -> None:
        if self._steps is None:
            return
        for action in self._steps.on(event):
            self._execute(action)

    def _execute(self, action: Action) -> None:
        match action:
            case SubmitBracket():
                self._submit_bracket(action)
            case CancelLeg(client_order_id=client_order_id):
                print(f"\n>>> Cancelling the take-profit {client_order_id}.\n")
                self.cancel_order(self.cache.order(client_order_id))
            case ClosePosition(position_id=position_id):
                position = self.cache.position(position_id)
                if position is None or position.is_closed:
                    print(f"\n>>> Position {position_id} is not open; nothing to close.\n")
                    return
                print(f"\n>>> Closing position {position_id}.\n")
                self.close_position(position)
            case Prompt(text=text, wait_secs=wait_secs):
                print(f"\n>>> {text}\n")
                if wait_secs is not None:
                    self.clock.set_time_alert(
                        name=_MANUAL_WAIT_ALERT,
                        alert_time=self.clock.utc_now() + datetime.timedelta(seconds=wait_secs),
                        callback=lambda _event: self._feed(ManualWaitExpired()),
                    )
            case Done():
                self.shutdown_system("first-orders sequence finished")

    def _submit_bracket(self, action: SubmitBracket) -> None:
        orders = self.order_factory.bracket(
            instrument_id=self._instrument_id,
            order_side=OrderSide.BUY,
            quantity=action.quantity,
            sl_trigger_price=self._instrument.make_price(action.stop_loss),
            tp_price=self._instrument.make_price(action.take_profit),
        )
        ids = {order.order_type: order.client_order_id for order in orders.orders}
        # Fed before sending, so `Steps` knows the ids before any event about them.
        self._feed(
            BracketSubmitted(
                entry=ids[OrderType.MARKET],
                stop_loss=ids[OrderType.STOP_MARKET],
                take_profit=ids[OrderType.LIMIT],
            ),
        )
        print(
            f"\n>>> Buying {action.quantity} {self._instrument_id}: stop-loss "
            f"{action.stop_loss}, take-profit {action.take_profit}.\n",
        )
        self.submit_order_list(orders)


def instrument_id_of(symbol: str) -> InstrumentId:
    return InstrumentId(Symbol(symbol), CTRADER_VENUE)


class MissingCredentials(Exception):
    pass


def node_config(env: dict[str, str], symbol: str) -> TradingNodeConfig:
    """The node's config from the env file's values; raises `MissingCredentials`."""
    keys = (
        get_tokens.CLIENT_ID_KEY,
        get_tokens.CLIENT_SECRET_KEY,
        get_tokens.ACCESS_TOKEN_KEY,
        _TRADER_LOGIN_KEY,
    )
    missing = [key for key in keys if not env.get(key)]
    if missing:
        raise MissingCredentials(f"missing in .env: {', '.join(missing)}")
    try:
        trader_login = int(env[_TRADER_LOGIN_KEY])
    except ValueError:
        raise MissingCredentials(f"{_TRADER_LOGIN_KEY} in .env is not a number") from None
    # No refresh token: a refresh would rotate it, and this script does not write the new
    # pair back, so the one in .env would stop working. A run is minutes long.
    account = {
        "client_id": env[get_tokens.CLIENT_ID_KEY],
        "client_secret": env[get_tokens.CLIENT_SECRET_KEY],
        "access_token": env[get_tokens.ACCESS_TOKEN_KEY],
        "trader_login": trader_login,
    }
    return TradingNodeConfig(
        data_clients={
            CTRADER: CTraderDataClientConfig(
                **account,
                instrument_provider=InstrumentProviderConfig(
                    load_ids=frozenset([str(instrument_id_of(symbol))]),
                ),
            ),
        },
        exec_clients={CTRADER: CTraderExecClientConfig(**account)},
        exec_engine=LiveExecEngineConfig(
            inflight_check_threshold_ms=5_000,
            inflight_check_retries=18,
        ),
    )


def token_expired(env: dict[str, str], now_secs: float) -> bool:
    """Whether `.env` says the access token expired by `now_secs`; unknown counts as not."""
    text = env.get(get_tokens.TOKEN_EXPIRES_AT_KEY)
    if not text:
        return False
    try:
        expires_at = float(text)
    except ValueError:
        raise MissingCredentials(
            f"{get_tokens.TOKEN_EXPIRES_AT_KEY} in .env is not a number"
        ) from None
    return expires_at <= now_secs


def build_node(config: TradingNodeConfig, driver: _Driver) -> TradingNode:
    node = TradingNode(config=config)
    node.trader.add_strategy(driver)
    node.add_data_client_factory(CTRADER, CTraderLiveDataClientFactory)
    node.add_exec_client_factory(CTRADER, CTraderLiveExecClientFactory)
    node.build()
    return node


def checklist(log_path: pathlib.Path) -> str:
    return "\n".join(
        [
            f"Events of this run: {log_path}",
            "Compare them with the recording scripts/record_execution.py made alongside:",
            "- label, comment and client order id: each entry's label carries its client "
            "order id and its comment the two legs' ids, exactly as logged here;",
            "- levels: the stop-loss and take-profit the broker holds, after the correcting "
            "amend, equal the legs' prices here; the moved stop-loss is an OrderUpdated;",
            "- commission: each OrderFilled's commission equals its deal's, sign flipped;",
            "- volume scaling: each quantity here, in units, is the broker's volume / 100, "
            "for the first entry (the minimum), the second (the minimum plus one size step), "
            "both closes and the partial close of one size step.",
        ],
    )


# -- Command line -------------------------------------------------------------------------


def _positive_decimal(text: str) -> Decimal:
    try:
        value = Decimal(text)
    except InvalidOperation:
        raise argparse.ArgumentTypeError(f"not a number: {text!r}") from None
    if not value.is_finite() or value <= 0:
        raise argparse.ArgumentTypeError(f"must be positive: {text!r}")
    return value


def _positive_float(text: str) -> float:
    value = float(text)
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError(f"must be positive: {text!r}")
    return value


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send a short fixed sequence of REAL orders through the adapter.",
    )
    parser.add_argument(
        "--symbol",
        required=True,
        help="the broker's symbol name, as the terminal shows it",
    )
    parser.add_argument(
        "--distance",
        type=_positive_decimal,
        required=True,
        help="how far from the ask, in price units, the stop-loss and take-profit sit",
    )
    parser.add_argument(
        "--send-live-orders",
        action="store_true",
        help="required: confirms that real orders are to be sent",
    )
    parser.add_argument(
        "--manual-wait-secs",
        type=_positive_float,
        default=600.0,
        help="how long to wait for the changes made by hand (default: 600)",
    )
    parser.add_argument(
        "--log-dir",
        type=pathlib.Path,
        default=_REPO_ROOT / "tests" / "recordings",
        help="where the JSONL event log goes (default: tests/recordings/, ignored by git)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = build_arg_parser().parse_args(argv)
    except SystemExit as e:
        return e.code if isinstance(e.code, int) else 2
    # A presence check rather than `required=True`, so the refusal can say what it is for.
    if not args.send_live_orders:
        print(
            "Refused: this script sends real orders. Add --send-live-orders to run it.",
            file=sys.stderr,
        )
        return 2
    try:
        typed = input(f"This sends real orders on {args.symbol}. Type the symbol again to go on: ")
    except EOFError:
        typed = ""
    if typed.strip() != args.symbol:
        print("Refused: the symbol typed does not match. Nothing was sent.", file=sys.stderr)
        return 2

    try:
        env = get_tokens.load_env(_REPO_ROOT / ".env")
        expired = token_expired(env, time.time())
        if not env.get(get_tokens.TOKEN_EXPIRES_AT_KEY):
            print(
                f"Note: {get_tokens.TOKEN_EXPIRES_AT_KEY} is not in .env, so the access token's "
                "expiry is unknown; going on as if it were valid.",
                file=sys.stderr,
            )
        config = node_config(env, args.symbol)
    except (OSError, MissingCredentials) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    if expired:
        print(
            "Refused: the access token in .env has expired, and this script does not refresh "
            "it. Issue a new one with scripts/get_tokens.py. Nothing was sent.",
            file=sys.stderr,
        )
        return 2

    args.log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
    log_path = args.log_dir / f"first_orders-{stamp}.jsonl"
    with EventLog(log_path) as log:
        driver = _Driver(
            instrument_id_of(args.symbol),
            distance=args.distance,
            manual_wait_secs=args.manual_wait_secs,
            log=log,
        )
        node = build_node(config, driver)
        try:
            node.run()
        except KeyboardInterrupt:
            node.stop()
        finally:
            node.dispose()
    print(checklist(log_path))
    return 0 if driver.finished else 1


if __name__ == "__main__":
    sys.exit(main())
