"""Account activity the node did not start, published for applications.

Every such activity is published on the message bus under `ACCOUNT_ACTIVITY_TOPIC`. An actor
subscribes with `self.msgbus.subscribe(topic=ACCOUNT_ACTIVITY_TOPIC, handler=...)`.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

ACCOUNT_ACTIVITY_TOPIC = "ctrader.account_activity"


@dataclass(frozen=True)
class CTraderAccountActivity:
    """One piece of activity on the account that the node did not initiate.

    - `kind`: `"unloaded_symbol"` (anything on a symbol the node has not loaded),
      `"manual_change"` (a trader acting on the node's own position) or `"stop_out"`.
    - `symbol`: the broker's symbol name.
    - `subject`: `"order"` or `"position"`.
    - `side`: `"BUY"` or `"SELL"`, the side of the order or position.
    - `volume`: in units.
    - `action`: `"opened"`, `"changed"`, `"closed"`, `"partially_closed"`, `"level_moved"`,
      `"level_removed"` or `"level_added"`.
    - `ts_event`, `ts_init`: UNIX nanoseconds.
    """

    kind: str
    symbol: str
    subject: str
    side: str
    volume: Decimal
    action: str
    ts_event: int
    ts_init: int
