"""cTrader Open API adapter for NautilusTrader.

Transport and translation only: this package speaks the cTrader wire protocol and implements
the NautilusTrader live client interfaces. It holds no trading logic of any kind.
"""

from nautilus_ctrader.common.account import (
    AccountCredentials,
    CTraderAccountClient,
    get_cached_ctrader_account_client,
)
from nautilus_ctrader.config import CTraderDataClientConfig
from nautilus_ctrader.constants import CTRADER, CTRADER_VENUE
from nautilus_ctrader.data import CTraderDataClient
from nautilus_ctrader.factories import CTraderLiveDataClientFactory
from nautilus_ctrader.providers import CTraderInstrumentProvider

__version__ = "0.0.1"

__all__ = [
    "CTRADER",
    "CTRADER_VENUE",
    "AccountCredentials",
    "CTraderAccountClient",
    "CTraderDataClient",
    "CTraderDataClientConfig",
    "CTraderInstrumentProvider",
    "CTraderLiveDataClientFactory",
    "__version__",
    "get_cached_ctrader_account_client",
]
