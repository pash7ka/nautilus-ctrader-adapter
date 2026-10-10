"""What the node has asked of the broker and is still waiting on.

The execution client keeps it; the venue model reads it to tell the node's own changes from a
trader's. Nothing here changes what the model says the broker holds.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from decimal import Decimal

from nautilus_ctrader.common.venue_records import Level


class OperationsInFlight:
    """The node's level amends and closes the broker has not answered yet.

    An entry lives until its answer, its refusal or its timeout, so a lost answer never leaves one
    behind to take a trader's later change for the node's own.
    """

    def __init__(self) -> None:
        self._amends: dict[int, int] = {}
        # Client order id of each close -> (position id, venue volume, anchor in broker ms).
        self._closes: dict[str, tuple[int, int, int]] = {}
        # Client order id of each close -> the broker order its own answer named.
        self._answers: dict[str, int] = {}

    def begin_amend(self, position_id: int) -> None:
        self._amends[position_id] = self._amends.get(position_id, 0) + 1

    def end_amend(self, position_id: int) -> None:
        left = self._amends.get(position_id, 0) - 1
        if left > 0:
            self._amends[position_id] = left
        else:
            self._amends.pop(position_id, None)

    def amending(self, position_id: int) -> bool:
        return position_id in self._amends

    def begin_close(
        self, client_order_id: str, position_id: int, volume: int, anchor_ms: int
    ) -> None:
        """Record a close sent when the newest broker time the node had seen was `anchor_ms`.

        A broker order created no later than `anchor_ms` cannot be this close's. `-1`: no bound.
        """
        self._closes[client_order_id] = (position_id, volume, anchor_ms)

    def answered(self, client_order_id: str, order_id: int) -> None:
        """The close's own answer named broker order `order_id`: that order is the close's."""
        if client_order_id in self._closes:
            self._answers[client_order_id] = order_id

    def end_close(self, client_order_id: str) -> None:
        """Forget a close: answered, refused, or matched to its broker order."""
        self._closes.pop(client_order_id, None)
        self._answers.pop(client_order_id, None)

    def close_position(self, client_order_id: str) -> int | None:
        """The position the close `client_order_id` is closing, while it is in flight."""
        close = self._closes.get(client_order_id)
        return None if close is None else close[0]

    def closing(self, position_id: int, volume: int, created_ms: int, order_id: int) -> str | None:
        for client_order_id, answer in self._answers.items():
            if answer == order_id:
                return client_order_id
        for client_order_id, (close_position, close_volume, anchor_ms) in self._closes.items():
            if client_order_id in self._answers:
                continue  # answered with another broker order
            if (close_position, close_volume) != (position_id, volume):
                continue
            # Created before the node last heard from the broker: somebody else's.
            if anchor_ms < 0 or created_ms > anchor_ms:
                return client_order_id
        return None


@dataclass
class PendingBracket:
    """A bracket whose levels are not yet where the strategy asked for them.

    The entry carries its levels as distances from the fill, so the broker sets them near the
    asked prices, not at them; one amend after the protective order arrives sets them exactly.
    Until that amend is done, a leg's cancel or modify is recorded here and carried by it.

    - `legs`: each leg's client order id, by level.
    - `requested`: the exact price asked for each leg's level, a later modify included.
    - `cancels`, `modified`: levels whose leg was cancelled or modified meanwhile.
    - `correcting`: the amend is under way; `rounds` counts the amends sent.
    - `unanswered`: the last amend got no answer; no other goes out until a rebuild.
    """

    entry_id: str
    legs: dict[Level, str]
    requested: dict[Level, Decimal]
    cancels: set[Level] = field(default_factory=set)
    modified: set[Level] = field(default_factory=set)
    correcting: bool = False
    rounds: int = 0
    unanswered: bool = False


class PendingBrackets:
    """The node's brackets still awaiting their correcting amend, by entry and by leg."""

    def __init__(self) -> None:
        self._by_entry: dict[str, PendingBracket] = {}

    def add(self, bracket: PendingBracket) -> None:
        self._by_entry[bracket.entry_id] = bracket

    def remove(self, entry_id: str) -> None:
        self._by_entry.pop(entry_id, None)

    def by_entry(self, entry_id: str) -> PendingBracket | None:
        return self._by_entry.get(entry_id)

    def by_leg(self, leg_id: str) -> tuple[PendingBracket, Level] | None:
        for bracket in self._by_entry.values():
            for level, client_order_id in bracket.legs.items():
                if client_order_id == leg_id:
                    return bracket, level
        return None

    def __iter__(self) -> Iterator[PendingBracket]:
        # A copy: settling a bracket removes it.
        return iter(list(self._by_entry.values()))

    def __len__(self) -> int:
        return len(self._by_entry)
