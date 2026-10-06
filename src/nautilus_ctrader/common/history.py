"""Paged reads of the account's history: deal lists, order lists, and the windows to ask them in.

Each helper takes a request function the caller has already bound to the historical bucket, so
the rate limit stays the caller's concern.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence

from google.protobuf.message import Message

from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om

Request = Callable[[Message], Awaitable[Message]]

WEEK_MS = 604_800_000
# The latest time the venue takes in a request (19 January 2038).
_LATEST_MS = 2_147_483_646_000


async def deals_between(
    request: Request,
    account_id: int,
    from_ms: int,
    to_ms: int,
    *,
    max_pages: int = 50,
) -> tuple[list[om.ProtoOADeal], bool]:
    """The account's deals executed between `from_ms` and `to_ms`, oldest first.

    Returns the deals and whether the list ended within `max_pages`.
    """

    def ask(window: tuple[int, int]) -> Message:
        return oa.ProtoOADealListReq(
            ctidTraderAccountId=account_id, fromTimestamp=window[0], toTimestamp=window[1]
        )

    found, complete = await _paged(
        request,
        ask,
        (from_ms, to_ms),
        lambda page: page.deal,
        lambda deal: deal.dealId,
        lambda deal: deal.executionTimestamp,
        max_pages,
        first=None,
    )
    return _oldest_first(found), complete


async def position_deals(
    request: Request,
    account_id: int,
    position_id: int,
    *,
    max_pages: int = 20,
) -> tuple[list[om.ProtoOADeal], bool]:
    """Every deal of one position, oldest first, and whether the list ended within `max_pages`."""

    def ask(window: tuple[int, int]) -> Message:
        return oa.ProtoOADealListByPositionIdReq(
            ctidTraderAccountId=account_id,
            positionId=position_id,
            fromTimestamp=window[0],
            toTimestamp=window[1],
        )

    # The first page goes without a window, the way it was recorded answering.
    first = oa.ProtoOADealListByPositionIdReq(
        ctidTraderAccountId=account_id, positionId=position_id
    )
    found, complete = await _paged(
        request,
        ask,
        (0, _LATEST_MS),
        lambda page: page.deal,
        lambda deal: deal.dealId,
        lambda deal: deal.executionTimestamp,
        max_pages,
        first=first,
    )
    return _oldest_first(found), complete


async def orders_between(
    request: Request,
    account_id: int,
    from_ms: int,
    to_ms: int,
    *,
    max_pages: int = 50,
) -> list[om.ProtoOAOrder]:
    """The account's orders between `from_ms` and `to_ms`, oldest first.

    What `max_pages` did not reach is left out.
    """

    def ask(window: tuple[int, int]) -> Message:
        return oa.ProtoOAOrderListReq(
            ctidTraderAccountId=account_id, fromTimestamp=window[0], toTimestamp=window[1]
        )

    # TODO(verify): which of an order's times the order list filters by; paging assumes
    # `utcLastUpdateTimestamp`.
    found, _ = await _paged(
        request,
        ask,
        (from_ms, to_ms),
        lambda page: page.order,
        lambda order: order.orderId,
        lambda order: order.utcLastUpdateTimestamp,
        max_pages,
        first=None,
    )
    return sorted(found, key=lambda order: (order.utcLastUpdateTimestamp, order.orderId))


def weekly_windows(from_ms: int, to_ms: int) -> list[tuple[int, int]]:
    """`from_ms` to `to_ms` in windows of a week at most, oldest first.

    Neighbouring windows share their edge, so an item at an edge may be listed twice; callers
    de-duplicate by id.
    """
    windows = []
    start = from_ms
    while start < to_ms:
        end = min(start + WEEK_MS, to_ms)
        windows.append((start, end))
        start = end
    return windows


async def _paged(
    request: Request,
    ask: Callable[[tuple[int, int]], Message],
    window: tuple[int, int],
    items_of: Callable[[Message], Sequence],
    id_of: Callable,
    time_of: Callable,
    max_pages: int,
    *,
    first: Message | None,
) -> tuple[list, bool]:
    """Every item of a list asked page by page, de-duplicated by id, and whether it ended."""
    found: dict = {}
    current: tuple[int, int] | None = window
    for page_number in range(max_pages):
        page = await request(first if page_number == 0 and first is not None else ask(current))
        items = items_of(page)
        for item in items:
            found.setdefault(id_of(item), item)
        if not page.hasMore:
            return list(found.values()), True
        current = _next_window(items, current, time_of)
        if current is None:
            break
    return list(found.values()), False


def _next_window(
    items: Sequence, window: tuple[int, int], time_of: Callable
) -> tuple[int, int] | None:
    """The window left to ask after a page, narrowed at its last item; `None` if it cannot be.

    The last item's own time stays in the window: others of the same millisecond may not have
    fitted in the page. So that item is listed again, and a page of one millisecond only cannot
    be got past.
    """
    # TODO(verify): the order the lists come in, which the schema does not state, and the order
    # of items within one millisecond; an account with more deals than one page shows it.
    if not items:
        return None
    first, last = time_of(items[0]), time_of(items[-1])
    if first < last:
        narrowed = (last, window[1])
    elif first > last:
        narrowed = (window[0], last)
    else:
        return None
    return narrowed if narrowed != window else None


def _oldest_first(deals: list[om.ProtoOADeal]) -> list[om.ProtoOADeal]:
    return sorted(deals, key=lambda deal: (deal.executionTimestamp, deal.dealId))
