"""Outbound rate limiting.

Spotware's SDK drains a send queue on a one-second timer (`TcpProtocol._sendStrings`, run by
`LoopingCall(...).start(1)`). Here each named bucket is a token
bucket that callers await, so a caller is delayed rather than parked behind a timer, and a
message is either sent or its caller sees an error.

The venue reports a breach as `BLOCKED_PAYLOAD_TYPE` carrying `retryAfter`, so the limiter
does not have to guess the real limits: `pause_for` applies exactly what the venue asked for.
"""

from __future__ import annotations

import asyncio


class TokenBucket:
    """A single rate-limited bucket.

    The lock is held across the wait, which serialises waiters and so preserves the order in
    which they arrived.
    """

    def __init__(self, rate_per_sec: float, capacity: float | None = None) -> None:
        if rate_per_sec <= 0:
            raise ValueError("rate_per_sec must be positive")
        self._rate = rate_per_sec
        self._capacity = rate_per_sec if capacity is None else capacity
        self._tokens = self._capacity
        self._updated: float | None = None
        self._paused_until = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until one token is available, then consume it."""
        loop = asyncio.get_running_loop()
        async with self._lock:
            if self._updated is None:
                self._updated = loop.time()
            while True:
                now = loop.time()
                if now < self._paused_until:
                    await asyncio.sleep(self._paused_until - now)
                    continue
                self._tokens = min(
                    self._capacity,
                    self._tokens + (now - self._updated) * self._rate,
                )
                self._updated = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                await asyncio.sleep((1.0 - self._tokens) / self._rate)

    def pause_for(self, seconds: float) -> None:
        """Block the bucket for `seconds`, as instructed by the venue.

        Never shortens a pause already in effect. Nothing accrues while paused: the bucket
        resumes with one token and refills at the normal rate from there.
        """
        self._paused_until = max(
            self._paused_until,
            asyncio.get_running_loop().time() + seconds,
        )
        # A full burst on resume could re-trigger the block.
        self._tokens = min(self._capacity, 1.0)
        self._updated = self._paused_until


class RateLimiter:
    """Named token buckets, so historical requests get their own much tighter budget."""

    def __init__(self, rates: dict[str, float]) -> None:
        self._buckets = {name: TokenBucket(rate) for name, rate in rates.items()}

    async def acquire(self, bucket: str = "default") -> None:
        await self._buckets[bucket].acquire()

    def pause(self, bucket: str, seconds: float) -> None:
        self._buckets[bucket].pause_for(seconds)
