"""Reference-counted venue subscriptions for one account, kept alive across reconnects.

Each key - `("spots", symbol_id)` or `("trendbar", symbol_id, period)` - is subscribed at the
venue while at least one consumer holds it, and is re-subscribed by a session restore after
every reconnect. Keys and log lines carry the symbol id and period only, never the account.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

from google.protobuf.message import Message
from nautilus_trader.common.component import Logger

from nautilus_ctrader.common.errors import (
    CTraderConnectionError,
    CTraderError,
    CTraderRequestError,
    CTraderTimeoutError,
)
from nautilus_ctrader.common.session import SessionState
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa

if TYPE_CHECKING:
    from nautilus_ctrader.common.account import CTraderAccountClient
    from nautilus_ctrader.common.session import CTraderSession

SpotListener = Callable[[oa.ProtoOASpotEvent], None]

_Key = tuple[str, int] | tuple[str, int, int]
# `(owner, name)`: the same name under two owners is two consumers.
_Consumer = tuple[str, str]

# TODO(verify): the venue answers a duplicate subscribe with ALREADY_SUBSCRIBED rather than
# silently accepting it or failing with another code.
_ALREADY_SUBSCRIBED = "ALREADY_SUBSCRIBED"
_ACCEPTING = (SessionState.READY, SessionState.RESTORING)
# Only picks DEBUG over WARNING for an unsubscribe refused during a restore.
# TODO(verify): the code the venue refuses an unsubscribe of a never-subscribed key with, for
# live trendbars as well as spots. A *subscribe* without spots is refused with INVALID_REQUEST,
# not with this code (see docs/protocol.md), so the venue does not use it for every missing
# subscription. INVALID_REQUEST is deliberately not added here: it is the venue's generic
# refusal, and treating it as expected would hide a genuinely malformed request.
_NOT_SUBSCRIBED = frozenset({"NOT_SUBSCRIBED_TO_SPOTS"})


def _spots_key(symbol_id: int) -> _Key:
    return ("spots", symbol_id)


def _trendbar_key(symbol_id: int, period: int) -> _Key:
    return ("trendbar", symbol_id, period)


def _trendbar_spot_consumer(period: int, consumer: _Consumer) -> _Consumer:
    owner, name = consumer
    return owner, f"trendbar:{period}:{name}"


@dataclass
class _Intent:
    """What one consumer's latest (un)subscribe call asked for, while that is still unsettled."""

    # Whether the consumer should receive data: set by a subscribe, cleared by an unsubscribe.
    wanted: bool = False
    # Subscribe calls still running, which may yet leave the consumer counted.
    pending: int = 0


class SubscriptionRegistry:
    """
    The account's spot and trendbar subscriptions, shared by consumer.

    - A consumer is a name under an `owner`, the client that holds it: every client of the
      account shares this registry, and the same name under two owners is two consumers.
    - While the session accepts requests, the venue request goes first and the reference is
      counted once it succeeds, or once its outcome is unknown (timeout, cancellation, loss);
      only a venue refusal leaves it uncounted. Otherwise (no session, or reconnecting) only
      the intent is recorded, and the next bring-up's restores act on it; (un)subscribing
      never raises for want of a connection.
    - The same consumer subscribing twice counts once.
    - Counted and active are different facts. A consumer is active - should receive data -
      from the moment its subscribe is called until its unsubscribe is, or until the
      subscribe ends uncounted; it stays counted until that unsubscribe has run its course.
      `active_consumers()` answers the first, and `spot_holds()` / `trendbar_holds()` what an
      owner still has to release.
    - A consumer joining a key whose restore is in the session's `failed_restores` gets no
      data until the restore retry succeeds.
    - It outlives sessions: the account client `attach`es each new session before starting it
      and `detach`es it when dropping it.
    """

    def __init__(self, account: CTraderAccountClient, logger: Logger) -> None:
        self._account = account
        self._log = logger
        self._session: CTraderSession | None = None
        self._consumers: dict[_Key, set[_Consumer]] = {}
        # Every consumer an (un)subscribe call has named and that is counted or may yet be,
        # trendbar spot legs excepted: they belong to their trendbar consumer.
        self._intents: dict[_Key, dict[_Consumer, _Intent]] = {}
        # Per key; a trendbar operation takes its trendbar lock before its spots lock, never
        # the other way round.
        self._locks: dict[_Key, asyncio.Lock] = {}
        self._spot_listeners: dict[int, list[SpotListener]] = {}
        # `id()` of each listener that has raised, since listeners need not be hashable.
        self._failing_listeners: set[int] = set()

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

    async def subscribe_spots(self, symbol_id: int, consumer: str, owner: str) -> None:
        """Hold `symbol_id`'s spot subscription for `consumer`.

        Raises `CTraderRequestError` only if the venue refuses it. A cancelled or timed-out
        call is still recorded, so the caller must still call `unsubscribe_spots`.
        """
        key = _spots_key(symbol_id)
        held = (owner, consumer)
        with self._subscribing(key, held):
            async with self._lock(key):
                await self._acquire(key, held)

    async def unsubscribe_spots(self, symbol_id: int, consumer: str, owner: str) -> None:
        """Release `consumer`'s spot reference.

        Idempotent, and a cancelled call keeps the reference, so repeating it re-sends the
        unsubscribe rather than finding nothing left to do.
        """
        key = _spots_key(symbol_id)
        held = (owner, consumer)
        with self._releasing(key, held):
            async with self._lock(key):
                await self._release(key, held)

    async def subscribe_trendbars(
        self,
        symbol_id: int,
        period: int,
        consumer: str,
        owner: str,
    ) -> None:
        """Subscribe live trendbars, holding the spot subscription the venue requires for them.

        Raises `CTraderRequestError` only if the venue refuses it, and then releases the spot
        reference again. A cancelled or timed-out call is still recorded, possibly only its
        spots leg, so the caller must still call `unsubscribe_trendbars`.
        """
        key = _trendbar_key(symbol_id, period)
        spots = _spots_key(symbol_id)
        held = (owner, consumer)
        spot_consumer = _trendbar_spot_consumer(period, held)
        with self._subscribing(key, held):
            async with self._lock(key):
                if held in self._consumers.get(key, ()):
                    return
                async with self._lock(spots):
                    await self._acquire(spots, spot_consumer)
                try:
                    await self._acquire(key, held)
                except CTraderRequestError:
                    async with self._lock(spots):
                        await self._release(spots, spot_consumer)
                    raise

    async def unsubscribe_trendbars(
        self,
        symbol_id: int,
        period: int,
        consumer: str,
        owner: str,
    ) -> None:
        """Release both of `consumer`'s references.

        Idempotent, and a cancelled call keeps whichever reference its request was still in
        flight for, so repeating it re-sends that unsubscribe.

        The spot reference is released even without a trendbar reference, which a cancelled
        `subscribe_trendbars` leaves behind when cut short in its spots leg.
        """
        key = _trendbar_key(symbol_id, period)
        spots = _spots_key(symbol_id)
        held = (owner, consumer)
        with self._releasing(key, held):
            async with self._lock(key):
                try:
                    # Trendbar first: the venue requires the spot subscription while it is live.
                    await self._release(key, held)
                finally:
                    async with self._lock(spots):
                        await self._release(spots, _trendbar_spot_consumer(period, held))

    def add_spot_listener(self, symbol_id: int, listener: SpotListener) -> None:
        self._spot_listeners.setdefault(symbol_id, []).append(listener)

    def remove_spot_listener(self, symbol_id: int, listener: SpotListener) -> None:
        listeners = self._spot_listeners.get(symbol_id)
        if listeners is None or listener not in listeners:
            return
        # The stored object, not `listener`: an equal bound method is a different object.
        stored = listeners.pop(listeners.index(listener))
        self._failing_listeners.discard(id(stored))
        if not listeners:
            del self._spot_listeners[symbol_id]

    def consumers(self, symbol_id: int) -> frozenset[str]:
        """Names counted on `symbol_id`'s spots, of every owner, trendbar holders included."""
        return frozenset(name for _, name in self._consumers.get(_spots_key(symbol_id), ()))

    def trendbar_consumers(self, symbol_id: int, period: int, owner: str) -> frozenset[str]:
        """`owner`'s names counted on `symbol_id`'s live trendbars for `period`.

        Unlike `trendbar_holds()`, a consumer whose spot leg alone is counted is not included:
        it has no live trendbars.
        """
        key = _trendbar_key(symbol_id, period)
        return frozenset(name for o, name in self._consumers.get(key, ()) if o == owner)

    def active_consumers(self, symbol_id: int, owner: str) -> frozenset[str]:
        """`owner`'s consumers of `symbol_id`'s spots that should receive data now.

        Subscribed, or with a subscribe in flight, and with no unsubscribe started since.
        Trendbar holders are not included: they hold the spots for the trendbars only.
        """
        intents = self._intents.get(_spots_key(symbol_id), {})
        return frozenset(name for (o, name), i in intents.items() if o == owner and i.wanted)

    def has_active_consumer(self, symbol_id: int, owner: str) -> bool:
        """Whether `active_consumers()` is non-empty, without building it: asked on every spot."""
        intents = self._intents.get(_spots_key(symbol_id), {})
        return any(o == owner and i.wanted for (o, _), i in intents.items())

    def spot_holds(self, owner: str) -> frozenset[tuple[int, str]]:
        """Every `(symbol id, consumer)` spot hold `owner` still has to release.

        Counted, or with a subscribe in flight - active or not, so a release that was
        cancelled stays here until one runs its course.
        """
        return frozenset((key[1], name) for key, name in self._held(owner, "spots"))

    def trendbar_holds(self, owner: str) -> frozenset[tuple[int, int, str]]:
        """Every `(symbol id, period, consumer)` trendbar hold `owner` still has to release.

        Either leg counted is enough, so this includes a consumer whose spot leg alone is left
        by a cancelled call.
        """
        return frozenset((key[1], key[2], name) for key, name in self._held(owner, "trendbar"))

    def _held(self, owner: str, kind: str) -> Iterator[tuple[_Key, str]]:
        for key, intents in self._intents.items():
            if key[0] == kind:
                yield from ((key, name) for o, name in intents if o == owner)

    @contextlib.contextmanager
    def _subscribing(self, key: _Key, consumer: _Consumer) -> Iterator[None]:
        # Entered before the key lock, so the consumer is active while the call waits for it.
        intent = self._intents.setdefault(key, {}).setdefault(consumer, _Intent())
        intent.wanted = True
        intent.pending += 1
        try:
            yield
        finally:
            intent.pending -= 1
            self._settle(key, consumer)

    @contextlib.contextmanager
    def _releasing(self, key: _Key, consumer: _Consumer) -> Iterator[None]:
        # Entered before the key lock: nothing is delivered for a release that has started.
        self._intents.setdefault(key, {}).setdefault(consumer, _Intent()).wanted = False
        try:
            yield
        finally:
            self._settle(key, consumer)

    def _settle(self, key: _Key, consumer: _Consumer) -> None:
        """Forget `consumer`'s intent once nothing is counted or pending for it."""
        intents = self._intents.get(key, {})
        intent = intents.get(consumer)
        if intent is None or intent.pending or self._counted(key, consumer):
            return
        del intents[consumer]
        if not intents:
            del self._intents[key]

    def _counted(self, key: _Key, consumer: _Consumer) -> bool:
        """Whether any of `consumer`'s references on `key` is counted, a trendbar's spot leg too."""
        if consumer in self._consumers.get(key, ()):
            return True
        if key[0] != "trendbar":
            return False
        spot_consumer = _trendbar_spot_consumer(key[2], consumer)
        return spot_consumer in self._consumers.get(_spots_key(key[1]), ())

    def _lock(self, key: _Key) -> asyncio.Lock:
        return self._locks.setdefault(key, asyncio.Lock())

    def _accepting_session(self) -> CTraderSession | None:
        session = self._session
        if session is not None and session.state in _ACCEPTING:
            return session
        return None

    async def _acquire(self, key: _Key, consumer: _Consumer) -> None:
        """Count `consumer` on `key`, subscribing first if it is new. Caller holds the key lock."""
        consumers = self._consumers.get(key)
        if consumers is not None:
            consumers.add(consumer)
            return
        session = self._accepting_session()
        if session is None:
            self._record(key, consumer)
            return
        # Only a venue refusal is a definite "no". Any other failure leaves the outcome unknown
        # and is recorded as an intent, so the consumer's unsubscribe still cleans it up and
        # the next bring-up makes the venue consistent.
        try:
            await _subscribe(session, [self._subscribe_request(key)])
        except CTraderRequestError:
            raise
        except (CTraderConnectionError, CTraderTimeoutError):
            self._record_unknown(session, key, consumer)
        except CTraderError:
            # A loss can surface as another error type.
            if self._accepting_session() is session:
                raise
            self._record_unknown(session, key, consumer)
        except asyncio.CancelledError:
            self._record_unknown(session, key, consumer)
            raise
        else:
            # Recorded against whichever session is attached now: if that is no longer the one
            # the request went through, its next bring-up restores the key.
            self._record(key, consumer)

    def _record_unknown(self, session: CTraderSession, key: _Key, consumer: _Consumer) -> None:
        self._record(key, consumer)
        # A session that stays up would not repeat the restore before its next bring-up; the
        # account's retry loop runs it instead, and ALREADY_SUBSCRIBED keeps that idempotent.
        if self._accepting_session() is session:
            session.mark_restore_failed(key)

    def _record(self, key: _Key, consumer: _Consumer) -> None:
        # No await in here, so a bring-up either sees both the reference and its restore or
        # neither.
        self._consumers.setdefault(key, set()).add(consumer)
        if self._session is not None:
            self._session.add_restore(key, self._restore(self._session, key))

    async def _release(self, key: _Key, consumer: _Consumer) -> None:
        """Drop `consumer`'s reference; the last one also unsubscribes. Caller holds the key lock.

        The reference is dropped once the request has run its course, failure included: its
        consumer is gone either way, and spots the venue keeps sending are dropped for want of
        a listener. A cancellation is the one outcome that keeps it, mirroring `_acquire`: the
        venue may still be streaming the key, and only a repeated unsubscribe settles that.
        """
        consumers = self._consumers.get(key)
        if consumers is None or consumer not in consumers:
            return
        if len(consumers) > 1:
            consumers.discard(consumer)
            return
        session = self._accepting_session()
        restoring = session is not None and session.state is SessionState.RESTORING
        try:
            # TODO(verify): the venue drops all subscriptions when the connection drops, so a
            # key released while not connected needs no unsubscribe.
            if session is not None:
                await session.request(self._unsubscribe_request(key))
        except CTraderError as e:
            detail = e.error_code if isinstance(e, CTraderRequestError) else type(e).__name__
            if restoring and detail in _NOT_SUBSCRIBED:
                # Expected: the new connection had not re-subscribed the key yet.
                self._log.debug(f"Unsubscribe {key!r} during restore: {detail}")
            else:
                self._log.warning(f"Unsubscribe {key!r} failed: {detail}")
        del self._consumers[key]
        if self._session is not None:
            self._session.remove_restore(key)

    def _restore(self, session: CTraderSession, key: _Key) -> Callable[[], Awaitable[None]]:
        requests = [self._subscribe_request(key)]
        if key[0] == "trendbar":
            # Spots first, so the trendbar restore works whatever order the restores run in.
            requests.insert(0, self._subscribe_request(_spots_key(key[1])))

        async def restore() -> None:
            # The lock is retaken per request. `asyncio.Lock` hands off to its queued waiters
            # in FIFO order, so a release queued meanwhile runs in between and the next
            # request sees the key gone.
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
                if id(listener) in self._failing_listeners:
                    self._log.debug(f"{message} {type(e).__name__} again")
                else:
                    self._failing_listeners.add(id(listener))
                    self._log.exception(message, e)


async def _subscribe(session: CTraderSession, requests: list[Message]) -> None:
    for request in requests:
        try:
            await session.request(request)
        except CTraderRequestError as e:
            if e.error_code != _ALREADY_SUBSCRIBED:
                raise
