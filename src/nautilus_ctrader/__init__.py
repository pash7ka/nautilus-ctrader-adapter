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
from nautilus_ctrader.common.errors import (
    CTraderAccountError,
    CTraderAuthError,
    CTraderAuthorizationDenied,
    CTraderAuthorizationTimeout,
    CTraderConnectionError,
    CTraderError,
    CTraderNotSentConnectionError,
    CTraderNotSentError,
    CTraderProtocolError,
    CTraderQueueTimeoutError,
    CTraderRequestError,
    CTraderTimeoutError,
    CTraderTokenExchangeError,
)
from nautilus_ctrader.common.order_record import (
    LegIds,
    RecordTooLong,
    encode_comment,
    encode_label,
    parse_comment,
    parse_label,
)
from nautilus_ctrader.config import CTraderDataClientConfig, CTraderExecClientConfig
from nautilus_ctrader.constants import CTRADER, CTRADER_VENUE
from nautilus_ctrader.data import CTraderDataClient
from nautilus_ctrader.discovery import GrantedAccount, SymbolInfo, list_accounts, list_symbols
from nautilus_ctrader.execution import CTraderExecutionClient
from nautilus_ctrader.factories import CTraderLiveDataClientFactory, CTraderLiveExecClientFactory
from nautilus_ctrader.failures import REQUEST_FAILED_TOPIC, CTraderRequestFailed
from nautilus_ctrader.legs import leg_position_id
from nautilus_ctrader.oauth import (
    RedirectUriError,
    TokenPair,
    build_authorization_url,
    exchange_code,
    wait_for_authorization_code,
)
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
    "REQUEST_FAILED_TOPIC",
    "AccountCredentials",
    "CTraderAccountActivity",
    "CTraderAccountClient",
    "CTraderAccountError",
    "CTraderAuthError",
    "CTraderAuthorizationDenied",
    "CTraderAuthorizationTimeout",
    "CTraderConnectionError",
    "CTraderDataClient",
    "CTraderDataClientConfig",
    "CTraderError",
    "CTraderExecClientConfig",
    "CTraderExecutionClient",
    "CTraderInstrumentProvider",
    "CTraderLiveDataClientFactory",
    "CTraderLiveExecClientFactory",
    "CTraderNotSentConnectionError",
    "CTraderNotSentError",
    "CTraderProtocolError",
    "CTraderQueueTimeoutError",
    "CTraderRequestError",
    "CTraderRequestFailed",
    "CTraderTimeoutError",
    "CTraderTokenExchangeError",
    "GrantedAccount",
    "InstrumentLoadError",
    "InstrumentLoadFailure",
    "LegIds",
    "RecordTooLong",
    "RedirectUriError",
    "SymbolInfo",
    "TokenPair",
    "__version__",
    "account_client_from_config",
    "build_authorization_url",
    "encode_comment",
    "encode_label",
    "exchange_code",
    "get_cached_ctrader_account_client",
    "leg_position_id",
    "list_accounts",
    "list_symbols",
    "parse_comment",
    "parse_label",
    "wait_for_authorization_code",
]
