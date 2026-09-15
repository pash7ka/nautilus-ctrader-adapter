"""Bounded polling for tests that wait on asynchronous state.

A fixed sleep either wastes time or flakes on a slow machine; polling returns as soon as the
condition holds and fails loudly when it never does.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable


async def wait_until(condition: Callable[[], bool], attempts: int = 300) -> None:
    for _ in range(attempts):
        if condition():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met within the polling budget")
