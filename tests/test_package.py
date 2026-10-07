"""Smoke test: the package imports and is installed."""

import nautilus_ctrader


def test_version_is_exposed() -> None:
    assert nautilus_ctrader.__version__


def test_documented_names_are_importable_from_the_package_root() -> None:
    """Anything a caller is told to catch or inspect must be reachable without a submodule."""
    for name in nautilus_ctrader.__all__:
        assert hasattr(nautilus_ctrader, name), name
    assert {"InstrumentLoadError", "InstrumentLoadFailure"} <= set(nautilus_ctrader.__all__)


def test_the_setup_api_is_importable_from_the_package_root() -> None:
    setup_names = {
        "TokenPair",
        "build_authorization_url",
        "wait_for_authorization_code",
        "exchange_code",
        "GrantedAccount",
        "SymbolInfo",
        "list_accounts",
        "list_symbols",
        "CTraderError",
        "CTraderAuthError",
        "CTraderAuthorizationDenied",
        "CTraderAuthorizationTimeout",
        "CTraderTokenExchangeError",
    }
    assert setup_names <= set(nautilus_ctrader.__all__)
