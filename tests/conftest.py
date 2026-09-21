"""Shared pytest fixtures.

Tests run offline against recorded protobuf fixtures and the fake server. Anything needing a
real broker connection must carry the ``broker`` marker.
"""

import pytest

from nautilus_ctrader.common import account as account_module


@pytest.fixture(autouse=True)
def _clear_account_cache():
    account_module._clear_account_cache()
    yield
    account_module._clear_account_cache()
