"""One check that a secret leaked nowhere an exception or the terminal could show it."""

from __future__ import annotations

import traceback


def assert_secret_free(error: BaseException | None, *secrets: str, stderr: str = "") -> None:
    """Assert no `secrets` appear in `error` or `stderr`.

    The search covers, for `error` and every exception reachable through `__cause__` and
    `__context__`: its `str`, `repr`, `args`, attributes and the line a traceback prints for it.
    Source lines are left out, since they quote the test's own code.
    """
    texts = [stderr]
    seen: set[int] = set()
    pending = [error]
    while pending:
        current = pending.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        texts += [
            str(current),
            repr(current),
            repr(current.args),
            repr(vars(current)),
            "".join(traceback.format_exception_only(current)),
        ]
        pending += [current.__cause__, current.__context__]
    for secret in secrets:
        for text in texts:
            assert secret not in text, f"{secret!r} leaked into: {text[:300]!r}"
