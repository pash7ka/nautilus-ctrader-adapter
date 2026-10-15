"""A fake venue limiter, modelled on the request limits measured on a live account."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Callable


class FakeLimiter:
    """Refuses with `BLOCKED_PAYLOAD_TYPE` and `retryAfter`, by one of two window shapes.

    - `groups` maps a payload type to the key it is counted under and that key's limit:
      payload types sharing a key share one limit;
    - `shape` is `"sliding"` (any 1 s span) or `"fixed"` (calendar seconds);
    - a refusal blocks its key for `retry_after` seconds; refusals inside the block do not
      extend it.
    """

    def __init__(
        self,
        clock: Callable[[], float],
        groups: dict[int, tuple[str, int]],
        *,
        shape: str,
        retry_after: int = 1,
    ) -> None:
        self._clock = clock
        self._groups = groups
        self._shape = shape
        self._retry_after = retry_after
        self._admitted: dict[str, list[float]] = defaultdict(list)
        self._blocked_until: dict[str, float] = {}

    def admit(self, payload_type: int) -> int | None:
        """`None` if the request is answered, else the `retryAfter` it is refused with."""
        if payload_type not in self._groups:
            return None
        key, limit = self._groups[payload_type]
        now = self._clock()
        if now < self._blocked_until.get(key, -math.inf):
            return self._retry_after
        admitted = self._admitted[key]
        if self._shape == "sliding":
            used = sum(1 for t in admitted if now - t < 1.0)
        else:
            used = sum(1 for t in admitted if math.floor(t) == math.floor(now))
        if used >= limit:
            self._blocked_until[key] = now + self._retry_after
            return self._retry_after
        admitted.append(now)
        return None
