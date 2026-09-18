"""Reference-counted venue subscriptions for one account, kept alive across reconnects.

Each key - `("spots", symbol_id)` or `("trendbar", symbol_id, period)` - is subscribed at the
venue while at least one consumer holds it, and is re-subscribed by a session restore after
every reconnect. Keys and log lines carry the symbol id and period only, never the account.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING

from google.protobuf.message import Message
from nautilus_trader.common.component import Logger

from nautilus_ctrader.common.errors import (
    CTraderConnectionError,
    CTraderError,
    CTraderRequestError,
)
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa

if TYPE_CHECKING:
    from nautilus_ctrader.common.account import CTraderAccountClient
    from nautilus_ctrader.common.session import CTraderSession

SpotListener = Callable[[oa.ProtoOASpotEvent], None]

_Key = tuple[str, int] | tuple[str, int, int]

# TODO(verify): the venue answers a duplicate subscribe with ALREADY_SUBSCRIBED rather than
# silently accepting it or failing with another code.
_ALREADY_SUBSCRIBED = "ALREADY_SUBSCRIBED"


def _spots_key(symbol_id: int) -> _Key:
    return ("spots", symbol_id)


def _trendbar_key(symbol_id: int, period: int) -> _Key:
    return ("trendbar", symbol_id, period)


def _trendbar_spot_consumer(period: int, consumer: str) -> str:
    return f"trendbar:{period}:{consumer}"


class SubscriptionRegistry:
    """
    The account's spot and trendbar subscriptions, shared by consumer name.

    - A consumer is counted only once the venue has accepted the subscription; the same
      consumer subscribing twice counts once.
    - (Un)subscribing needs a ready session and raises `CTraderConnectionError` otherwise.
    - It outlives sessions: the account client `attach`es each new session before starting it
      and `detach`es it when dropping it.
    """

    def __init__(self, account: CTraderAccountClient, logger: Logger) -> None:
        self._account = account
        self._log = logger
        self._session: CTraderSession | None = None
        self._consumers: dict[_Key, set[str]] = {}
        # Serialise the first-subscribe and last-unsubscribe requests of each key.
        self._locks: dict[_Key, asyncio.Lock] = {}
        self._spot_listeners: dict[int, list[SpotListener]] = {}

    def attach(self, session: CTraderSession) -> None:
        """Route `session`'s spot events here and register a restore for every live key."""
        self.detach()
        self._session = session
        session.add_event_handler(oa.ProtoOASpotEvent, self._on_spot)
        for key in self._consumers:
            session.add_restore(key, self._restore(session, key))

    def detach(self) -> None:
        session, self._session = self._session, None
        if session is not None:
            session.remove_event_handler(oa.ProtoOASpotEvent, self._on_spot)

    async def subscribe_spots(self, symbol_id: int, consumer: str) -> None:
        await self._acquire(_spots_key(symbol_id), consumer)

    async def unsubscribe_spots(self, symbol_id: int, consumer: str) -> None:
        self._ready_session()
        await self._release(_spots_key(symbol_id), consumer)

    async def subscribe_trendbars(self, symbol_id: int, period: int, consumer: str) -> None:
        """Subscribe live trendbars, holding the spot subscription the venue requires for them."""
        spot_consumer = _trendbar_spot_consumer(period, consumer)
        key = _trendbar_key(symbol_id, period)
        await self._acquire(_spots_key(symbol_id), spot_consumer)
        try:
            await self._acquire(key, consumer)
        except BaseException:
            if consumer not in self._consumers.get(key, ()):
                await self._release(_spots_key(symbol_id), spot_consumer)
            raise

    async def unsubscribe_trendbars(self, symbol_id: int, period: int, consumer: str) -> None:
        self._ready_session()
        # Trendbar first: the venue requires the spot subscription while it is live.
        await self._release(_trendbar_key(symbol_id, period), consumer)
        await self._release(_spots_key(symbol_id), _trendbar_spot_consumer(period, consumer))

    def add_spot_listener(self, symbol_id: int, listener: SpotListener) -> None:
        self._spot_listeners.setdefault(symbol_id, []).append(listener)

    def remove_spot_listener(self, symbol_id: int, listener: SpotListener) -> None:
        listeners = self._spot_listeners.get(symbol_id)
        if listeners is None or listener not in listeners:
            return
        listeners.remove(listener)
        if not listeners:
            del self._spot_listeners[symbol_id]

    def consumers(self, symbol_id: int) -> frozenset[str]:
        """The consumers holding `symbol_id`'s spot subscription, trendbar holders included."""
        return frozenset(self._consumers.get(_spots_key(symbol_id), ()))

    def _ready_session(self) -> CTraderSession:
        session = self._session
        if session is None or not session.is_ready:
            raise CTraderConnectionError("account session not ready")
        return session

    def _lock(self, key: _Key) -> asyncio.Lock:
        return self._locks.setdefault(key, asyncio.Lock())

    async def _acquire(self, key: _Key, consumer: str) -> None:
        async with self._lock(key):
            session = self._ready_session()
            consumers = self._consumers.get(key)
            if consumers is not None:
                consumers.add(consumer)
                return
            await _subscribe(session, [self._subscribe_request(key)])
            self._consumers[key] = {consumer}
            # The session attached now, which may not be the one the request went through.
            if self._session is not None:
                self._session.add_restore(key, self._restore(self._session, key))

    async def _release(self, key: _Key, consumer: str) -> None:
        """Drop `consumer`'s reference; the last one also unsubscribes, if a session is ready.

        The reference is dropped even if the unsubscribe fails: its consumer is gone either
        way, and spots the venue keeps sending are dropped for want of a listener.
        """
        async with self._lock(key):
            consumers = self._consumers.get(key)
            if consumers is None or consumer not in consumers:
                return
            consumers.discard(consumer)
            if consumers:
                return
            del self._consumers[key]
            session = self._session
            if session is None:
                return
            session.remove_restore(key)
            if not session.is_ready:
                return
            try:
                await session.request(self._unsubscribe_request(key))
            except CTraderError as e:
                detail = e.error_code if isinstance(e, CTraderRequestError) else type(e).__name__
                self._log.warning(f"Unsubscribe {key!r} failed: {detail}")

    def _restore(self, session: CTraderSession, key: _Key):
        requests = [self._subscribe_request(key)]
        if key[0] == "trendbar":
            # Spots first, so the trendbar restore works whatever order the restores run in.
            requests.insert(0, self._subscribe_request(_spots_key(key[1])))

        async def restore() -> None:
            await _subscribe(session, requests)

        return restore

    def _subscribe_request(self, key: _Key) -> Message:
        account_id = self._account.account_id
        if key[0] == "spots":
            return oa.ProtoOASubscribeSpotsReq(
                ctidTraderAccountId=account_id,
                symbolId=[key[1]],
                subscribeToSpotTimestamp=True,
            )
        return oa.ProtoOASubscribeLiveTrendbarReq(
            ctidTraderAccountId=account_id,
            symbolId=key[1],
            period=key[2],
        )

    def _unsubscribe_request(self, key: _Key) -> Message:
        account_id = self._account.account_id
        if key[0] == "spots":
            return oa.ProtoOAUnsubscribeSpotsReq(ctidTraderAccountId=account_id, symbolId=[key[1]])
        return oa.ProtoOAUnsubscribeLiveTrendbarReq(
            ctidTraderAccountId=account_id,
            symbolId=key[1],
            period=key[2],
        )

    def _on_spot(self, event: oa.ProtoOASpotEvent) -> None:
        listeners = self._spot_listeners.get(event.symbolId)
        if not listeners:
            self._log.debug(f"Dropped spot for symbol {event.symbolId}: no listener")
            return
        for listener in tuple(listeners):
            try:
                listener(event)
            except Exception as e:
                self._log.exception(f"Spot listener for symbol {event.symbolId} raised", e)


async def _subscribe(session: CTraderSession, requests: list[Message]) -> None:
    for request in requests:
        try:
            await session.request(request)
        except CTraderRequestError as e:
            if e.error_code != _ALREADY_SUBSCRIBED:
                raise
