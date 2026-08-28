"""Smoke test: the package imports and is installed."""

import nautilus_ctrader


def test_version_is_exposed() -> None:
    assert nautilus_ctrader.__version__
