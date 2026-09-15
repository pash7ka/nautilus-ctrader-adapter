"""Bounded polling for tests that wait on asynchronous state.

A fixed sleep either wastes time or flakes on a slow machine; polling returns as soon as the
condition holds and fails loudly when it never does.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable


async def wait_until(
    condition: Callable[[], bool],
    timeout_secs: float = 3.0,
    description: str = "condition",
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_secs
    while not condition():
        if loop.time() >= deadline:
            raise AssertionError(f"{description} not met within {timeout_secs}s")
        await asyncio.sleep(0.01)
