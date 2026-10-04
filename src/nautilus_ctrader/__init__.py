"""cTrader Open API adapter for NautilusTrader.

Transport and translation only: this package speaks the cTrader wire protocol and implements
the NautilusTrader live client interfaces. It holds no trading logic of any kind.
"""

from nautilus_ctrader.activity import ACCOUNT_ACTIVITY_TOPIC, CTraderAccountActivity
from nautilus_ctrader.common.account import (
    AccountCredentials,
    CTraderAccountClient,
    account_client_from_config,
    get_cached_ctrader_account_client,
)
from nautilus_ctrader.common.errors import CTraderAccountError
from nautilus_ctrader.config import CTraderDataClientConfig, CTraderExecClientConfig
from nautilus_ctrader.constants import CTRADER, CTRADER_VENUE
from nautilus_ctrader.data import CTraderDataClient
from nautilus_ctrader.execution import CTraderExecutionClient
from nautilus_ctrader.factories import CTraderLiveDataClientFactory, CTraderLiveExecClientFactory
from nautilus_ctrader.providers import (
    CTraderInstrumentProvider,
    InstrumentLoadError,
    InstrumentLoadFailure,
)

__version__ = "0.0.1"

__all__ = [
    "ACCOUNT_ACTIVITY_TOPIC",
    "CTRADER",
    "CTRADER_VENUE",
    "AccountCredentials",
    "CTraderAccountActivity",
    "CTraderAccountClient",
    "CTraderAccountError",
    "CTraderDataClient",
    "CTraderDataClientConfig",
    "CTraderExecClientConfig",
    "CTraderExecutionClient",
    "CTraderInstrumentProvider",
    "CTraderLiveDataClientFactory",
    "CTraderLiveExecClientFactory",
    "InstrumentLoadError",
    "InstrumentLoadFailure",
    "__version__",
    "account_client_from_config",
    "get_cached_ctrader_account_client",
]
