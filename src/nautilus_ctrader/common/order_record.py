"""The adapter's own records in an order's `label` and `comment`.

A cTrader position carries no client order id, only the `label` and `comment` of the order that
opened it. The adapter writes its records there so that, after a restart, a position can be
recognised as the node's own and its protective legs restored under their original ids:

- `label` holds a marker and the entry's client order id. It is what decides ownership.
- `comment` holds the client order ids of the stop-loss and take-profit legs.

Anyone may write to these fields - a trader, another robot. Parsing is therefore strict: a text
that is not exactly a record written here is not a record, and the position is not the node's.
A mistake can then only be "not ours", never the node managing someone else's position.
"""

from __future__ import annotations

from dataclasses import dataclass

# The format's name and version in one token; a change of format changes it.
MARKER = "ntca1"

# TODO(verify): these limits come from the schema's comments. Not yet confirmed live:
# - the limits themselves;
# - whether the venue counts characters or bytes (moot while ids are ASCII);
# - whether it rejects or truncates an over-limit field;
# - whether it returns `label` and `comment` verbatim, with no trimming or case change.
# Placing an order with a limit-length label and comment and reading both back from the
# position would confirm all four.
LABEL_MAX = 100
COMMENT_MAX = 512
CLIENT_ORDER_ID_MAX = 50

_LABEL_PREFIX = f"{MARKER}:"
_FIELD_SEPARATOR = "|"
_STOP_LOSS_KEY = "sl"
_TAKE_PROFIT_KEY = "tp"


class RecordTooLong(ValueError):
    """A record does not fit the venue field it is written to."""


@dataclass(frozen=True)
class LegIds:
    """The client order ids of a position's protective legs; either may be absent."""

    stop_loss: str | None
    take_profit: str | None


def _is_recordable(order_id: str) -> bool:
    # Printable ASCII only: the venue's limit may count bytes, and invisible characters make
    # look-alike ids.
    return bool(order_id) and all("!" <= c <= "~" and c != _FIELD_SEPARATOR for c in order_id)


def _require_recordable(order_id: str) -> str:
    if not _is_recordable(order_id):
        raise ValueError("a client order id must be non-empty, with no whitespace and no '|'")
    return order_id


def _fit(text: str, limit: int, field: str) -> str:
    if len(text) > limit:
        raise RecordTooLong(f"{field} record is {len(text)} characters, the limit is {limit}")
    return text


def check_client_order_id(entry_id: str) -> str:
    """`entry_id` if it fits the venue's `clientOrderId`; raises `RecordTooLong` otherwise."""
    return _fit(_require_recordable(entry_id), CLIENT_ORDER_ID_MAX, "clientOrderId")


def encode_label(entry_id: str) -> str:
    return _fit(f"{_LABEL_PREFIX}{_require_recordable(entry_id)}", LABEL_MAX, "label")


def parse_label(label: str) -> str | None:
    """The entry's client order id, or `None` if `label` is not a record written here.

    `label` is a `str`: an unset protobuf string field is `""`, which is not a record.
    """
    if len(label) > LABEL_MAX or not label.startswith(_LABEL_PREFIX):
        return None
    entry_id = label[len(_LABEL_PREFIX) :]
    return entry_id if _is_recordable(entry_id) else None


def encode_comment(legs: LegIds) -> str:
    parts = [MARKER]
    if legs.stop_loss is not None:
        parts.append(f"{_STOP_LOSS_KEY}={_require_recordable(legs.stop_loss)}")
    if legs.take_profit is not None:
        parts.append(f"{_TAKE_PROFIT_KEY}={_require_recordable(legs.take_profit)}")
    return _fit(_FIELD_SEPARATOR.join(parts), COMMENT_MAX, "comment")


def parse_comment(comment: str) -> LegIds | None:
    """The legs' client order ids, or `None` if `comment` is not a record written here.

    `comment` is a `str`: an unset protobuf string field is `""`, which is not a record.
    """
    if len(comment) > COMMENT_MAX:
        return None
    marker, *fields = comment.split(_FIELD_SEPARATOR)
    if marker != MARKER:
        return None
    found: dict[str, str] = {}
    for item in fields:
        key, separator, order_id = item.partition("=")
        known = key in (_STOP_LOSS_KEY, _TAKE_PROFIT_KEY)
        if not separator or not known or not _is_recordable(order_id):
            return None
        found[key] = order_id
    legs = LegIds(stop_loss=found.get(_STOP_LOSS_KEY), take_profit=found.get(_TAKE_PROFIT_KEY))
    # Only the exact text `encode_comment` writes counts, so a reordered record is foreign.
    return legs if encode_comment(legs) == comment else None
