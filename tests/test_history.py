"""Tests for the paged history reads, against a request function that serves fixed lists."""

from __future__ import annotations

from google.protobuf.message import Message

from nautilus_ctrader.common import history
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.execution_replay import make_deal, make_order

ACCOUNT = 7654321


class Venue:
    """Answers a list request with up to `page_size` items in its window, in `order`."""

    def __init__(self, items: list, *, page_size: int, newest_first: bool = False) -> None:
        self.items = items
        self.page_size = page_size
        self.newest_first = newest_first
        self.asked: list[Message] = []

    async def request(self, payload: Message) -> Message:
        self.asked.append(payload)
        low = payload.fromTimestamp if payload.HasField("fromTimestamp") else None
        high = payload.toTimestamp if payload.HasField("toTimestamp") else None
        found = sorted(
            (
                item
                for item in self.items
                if (low is None or _time(item) >= low) and (high is None or _time(item) <= high)
            ),
            key=_time,
            reverse=self.newest_first,
        )
        page = found[: self.page_size]
        more = len(found) > self.page_size
        if isinstance(payload, oa.ProtoOAOrderListReq):
            return oa.ProtoOAOrderListRes(ctidTraderAccountId=ACCOUNT, order=page, hasMore=more)
        if isinstance(payload, oa.ProtoOADealListByPositionIdReq):
            return oa.ProtoOADealListByPositionIdRes(
                ctidTraderAccountId=ACCOUNT, deal=page, hasMore=more
            )
        return oa.ProtoOADealListRes(ctidTraderAccountId=ACCOUNT, deal=page, hasMore=more)


def _time(item) -> int:
    if isinstance(item, om.ProtoOADeal):
        return item.executionTimestamp
    return item.utcLastUpdateTimestamp


def deals(*times: int) -> list[om.ProtoOADeal]:
    return [
        make_deal(7_000_000 + n, 6_000_000, 5_000_000, side=om.BUY, volume=100, price=1.0, ts=ts)
        for n, ts in enumerate(times)
    ]


async def test_deal_pages_are_followed_oldest_first() -> None:
    venue = Venue(deals(10, 20, 30, 40, 50), page_size=2)

    found, complete = await history.deals_between(venue.request, ACCOUNT, 0, 100)

    assert complete
    assert [deal.executionTimestamp for deal in found] == [10, 20, 30, 40, 50]
    assert [(r.fromTimestamp, r.toTimestamp) for r in venue.asked] == [
        (0, 100),
        (20, 100),
        (30, 100),
        (40, 100),
    ]


async def test_deal_pages_are_followed_newest_first() -> None:
    venue = Venue(deals(10, 20, 30, 40, 50), page_size=2, newest_first=True)

    found, complete = await history.deals_between(venue.request, ACCOUNT, 0, 100)

    assert complete
    assert [deal.executionTimestamp for deal in found] == [10, 20, 30, 40, 50]


async def test_a_page_of_one_millisecond_ends_the_list_incomplete() -> None:
    venue = Venue(deals(10, 10, 10), page_size=2)

    found, complete = await history.deals_between(venue.request, ACCOUNT, 0, 100)

    assert not complete
    assert len(found) == 2


async def test_deal_pages_stop_at_the_page_limit() -> None:
    venue = Venue(deals(10, 20, 30, 40, 50), page_size=2)

    found, complete = await history.deals_between(venue.request, ACCOUNT, 0, 100, max_pages=2)

    assert not complete
    assert len(venue.asked) == 2
    assert [deal.executionTimestamp for deal in found] == [10, 20, 30]


async def test_position_deals_ask_without_a_window_first() -> None:
    venue = Venue(deals(10, 20, 30), page_size=2)

    found = await history.position_deals(venue.request, ACCOUNT, 5_000_000)

    assert [deal.executionTimestamp for deal in found] == [10, 20, 30]
    first, second = venue.asked
    assert not first.HasField("fromTimestamp")
    assert not first.HasField("toTimestamp")
    assert second.fromTimestamp == 20


async def test_order_pages_are_deduplicated() -> None:
    orders = [make_order(6_000_000 + n, 5_000_000, utc=ts) for n, ts in enumerate((10, 20, 30))]
    venue = Venue(orders, page_size=2)

    found = await history.orders_between(venue.request, ACCOUNT, 0, 100)

    assert [order.orderId for order in found] == [6_000_000, 6_000_001, 6_000_002]


def test_weekly_windows_are_a_week_at_most_oldest_first() -> None:
    week = history.WEEK_MS

    assert history.weekly_windows(0, 2 * week + 5) == [
        (0, week),
        (week, 2 * week),
        (2 * week, 2 * week + 5),
    ]
    assert history.weekly_windows(3, 10) == [(3, 10)]
    assert history.weekly_windows(10, 10) == []
