"""Reference-counted venue subscriptions for one account, kept alive across reconnects.

Each key - `("spots", symbol_id)` or `("trendbar", symbol_id, period)` - is subscribed at the
venue while at least one consumer holds it, and is re-subscribed by a session restore after
every reconnect. Keys and log lines carry the symbol id and period only, never the account.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from google.protobuf.message import Message
from nautilus_trader.common.component import Logger

from nautilus_ctrader.common.errors import (
    CTraderConnectionError,
    CTraderError,
    CTraderRequestError,
)
from nautilus_ctrader.common.session import SessionState
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa

if TYPE_CHECKING:
    from nautilus_ctrader.common.account import CTraderAccountClient
    from nautilus_ctrader.common.session import CTraderSession

SpotListener = Callable[[oa.ProtoOASpotEvent], None]

_Key = tuple[str, int] | tuple[str, int, int]

# TODO(verify): the venue answers a duplicate subscribe with ALREADY_SUBSCRIBED rather than
# silently accepting it or failing with another code.
_ALREADY_SUBSCRIBED = "ALREADY_SUBSCRIBED"
_ACCEPTING = (SessionState.READY, SessionState.RESTORING)


def _spots_key(symbol_id: int) -> _Key:
    return ("spots", symbol_id)


def _trendbar_key(symbol_id: int, period: int) -> _Key:
    return ("trendbar", symbol_id, period)


def _trendbar_spot_consumer(period: int, consumer: str) -> str:
    return f"trendbar:{period}:{consumer}"


class SubscriptionRegistry:
    """
    The account's spot and trendbar subscriptions, shared by consumer name.

    - While the session accepts requests, the venue request goes first and the reference is
      counted once it succeeds. Otherwise (no session, or reconnecting) only the intent is
      recorded, and the next bring-up's restores act on it; (un)subscribing never raises for
      want of a connection.
    - The same consumer subscribing twice counts once.
    - A consumer joining a key whose restore is in the session's `failed_restores` gets no
      data until the restore retry succeeds.
    - It outlives sessions: the account client `attach`es each new session before starting it
      and `detach`es it when dropping it.
    """

    def __init__(self, account: CTraderAccountClient, logger: Logger) -> None:
        self._account = account
        self._log = logger
        self._session: CTraderSession | None = None
        self._consumers: dict[_Key, set[str]] = {}
        # Per key; a trendbar operation takes its trendbar lock before its spots lock, never
        # the other way round.
        self._locks: dict[_Key, asyncio.Lock] = {}
        self._spot_listeners: dict[int, list[SpotListener]] = {}
        self._failing_listeners: set[SpotListener] = set()

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
        key = _spots_key(symbol_id)
        async with self._lock(key):
            await self._acquire(key, consumer)

    async def unsubscribe_spots(self, symbol_id: int, consumer: str) -> None:
        key = _spots_key(symbol_id)
        async with self._lock(key):
            await self._release(key, consumer)

    async def subscribe_trendbars(self, symbol_id: int, period: int, consumer: str) -> None:
        """Subscribe live trendbars, holding the spot subscription the venue requires for them.

        If the venue refuses the trendbar, the spot reference is released again. If the call
        is cancelled or times out, whether the trendbar reached the venue is unknown, and the
        spot reference is kept: spots without trendbars are harmless, trendbars without spots
        are not.
        """
        key = _trendbar_key(symbol_id, period)
        spots = _spots_key(symbol_id)
        spot_consumer = _trendbar_spot_consumer(period, consumer)
        async with self._lock(key):
            if consumer in self._consumers.get(key, ()):
                return
            async with self._lock(spots):
                await self._acquire(spots, spot_consumer)
            try:
                await self._acquire(key, consumer)
            except CTraderRequestError:
                async with self._lock(spots):
                    await self._release(spots, spot_consumer)
                raise

    async def unsubscribe_trendbars(self, symbol_id: int, period: int, consumer: str) -> None:
        key = _trendbar_key(symbol_id, period)
        spots = _spots_key(symbol_id)
        async with self._lock(key):
            if consumer not in self._consumers.get(key, ()):
                return
            try:
                # Trendbar first: the venue requires the spot subscription while it is live.
                await self._release(key, consumer)
            finally:
                async with self._lock(spots):
                    await self._release(spots, _trendbar_spot_consumer(period, consumer))

    def add_spot_listener(self, symbol_id: int, listener: SpotListener) -> None:
        self._spot_listeners.setdefault(symbol_id, []).append(listener)

    def remove_spot_listener(self, symbol_id: int, listener: SpotListener) -> None:
        listeners = self._spot_listeners.get(symbol_id)
        if listeners is None or listener not in listeners:
            return
        listeners.remove(listener)
        self._failing_listeners.discard(listener)
        if not listeners:
            del self._spot_listeners[symbol_id]

    def consumers(self, symbol_id: int) -> frozenset[str]:
        """The consumers holding `symbol_id`'s spot subscription, trendbar holders included."""
        return frozenset(self._consumers.get(_spots_key(symbol_id), ()))

    def _lock(self, key: _Key) -> asyncio.Lock:
        return self._locks.setdefault(key, asyncio.Lock())

    def _accepting_session(self) -> CTraderSession | None:
        session = self._session
        if session is not None and session.state in _ACCEPTING:
            return session
        return None

    async def _acquire(self, key: _Key, consumer: str) -> None:
        """Count `consumer` on `key`, subscribing first if it is new. Caller holds the key lock."""
        consumers = self._consumers.get(key)
        if consumers is not None:
            consumers.add(consumer)
            return
        session = self._accepting_session()
        if session is not None:
            # Lost mid-request: left to the next bring-up, like any other intent.
            with contextlib.suppress(CTraderConnectionError):
                await _subscribe(session, [self._subscribe_request(key)])
        self._record(key, consumer)

    def _record(self, key: _Key, consumer: str) -> None:
        # No await in here, so a bring-up either sees both the reference and its restore or
        # neither.
        self._consumers.setdefault(key, set()).add(consumer)
        if self._session is not None:
            self._session.add_restore(key, self._restore(self._session, key))

    async def _release(self, key: _Key, consumer: str) -> None:
        """Drop `consumer`'s reference; the last one also unsubscribes. Caller holds the key lock.

        The reference is dropped even if the unsubscribe fails: its consumer is gone either
        way, and spots the venue keeps sending are dropped for want of a listener.
        """
        consumers = self._consumers.get(key)
        if consumers is None or consumer not in consumers:
            return
        if len(consumers) > 1:
            consumers.discard(consumer)
            return
        try:
            session = self._accepting_session()
            # TODO(verify): the venue drops all subscriptions when the connection drops, so a
            # key released while not connected needs no unsubscribe.
            if session is not None:
                await session.request(self._unsubscribe_request(key))
        except CTraderError as e:
            detail = e.error_code if isinstance(e, CTraderRequestError) else type(e).__name__
            self._log.warning(f"Unsubscribe {key!r} failed: {detail}")
        finally:
            del self._consumers[key]
            if self._session is not None:
                self._session.remove_restore(key)

    def _restore(self, session: CTraderSession, key: _Key) -> Callable[[], Awaitable[None]]:
        requests = [self._subscribe_request(key)]
        if key[0] == "trendbar":
            # Spots first, so the trendbar restore works whatever order the restores run in.
            requests.insert(0, self._subscribe_request(_spots_key(key[1])))

        async def restore() -> None:
            # The lock is retaken per request, so a release queued meanwhile runs in between
            # and the next request sees the key gone.
            for request in requests:
                async with self._lock(key):
                    if key not in self._consumers:
                        return
                    await _subscribe(session, [request])

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
                message = f"Spot listener for symbol {event.symbolId} raised"
                # ERROR once per listener; a listener failing on every tick would flood it.
                if listener in self._failing_listeners:
                    self._log.debug(f"{message} {type(e).__name__} again")
                else:
                    self._failing_listeners.add(listener)
                    self._log.exception(message, e)


async def _subscribe(session: CTraderSession, requests: list[Message]) -> None:
    for request in requests:
        try:
            await session.request(request)
        except CTraderRequestError as e:
            if e.error_code != _ALREADY_SUBSCRIBED:
                raise
