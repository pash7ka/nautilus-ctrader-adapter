# nautilus-ctrader-adapter

A [cTrader Open API](https://help.ctrader.com/open-api/) adapter for
[NautilusTrader](https://github.com/nautechsystems/nautilus_trader): market data and order
execution against any broker that exposes cTrader Open API.

> **Status: early development (pre-alpha).** The transport layer is being built first. The
> public API is not stable, there is no PyPI release yet, and nothing here should be pointed
> at a live account. See [Roadmap](#roadmap).

## Why this exists

NautilusTrader is an event-driven trading platform where the same strategy code runs in
backtest and in live trading. As of release 1.231 it ships adapters for `architect_ax`,
`betfair`, `binance`, `bitmex`, `bybit`, `databento`, `deribit`, `dydx`, `hyperliquid`,
`interactive_brokers`, `kraken`, `okx`, `polymarket`, `sandbox` and `tardis` — **none for
cTrader**.

cTrader is a good fit for headless deployment: Open API is a server-side protobuf API over
TLS, so a bot needs no desktop terminal and no Windows host — unlike the MetaTrader route,
which requires a running terminal and synchronous IPC. Traders who want Nautilus and a
cTrader broker currently have to write the integration themselves. This repository closes
that gap once, in the open.

The author builds and maintains this adapter to run automated strategies on their own
cTrader accounts. It is MIT-licensed from the first commit
so that anyone with a cTrader broker can use it, test it and contribute fixes.

## Scope

This package is a **transport and translation layer, and nothing else.** It knows about the
cTrader wire protocol and about the NautilusTrader client interfaces. It knows nothing about
what to trade or when.

**In scope**

| Component | What it does |
|---|---|
| Transport | asyncio TLS client for the Open API protobuf protocol: framing, request/response correlation, heartbeat, reconnect with backoff, outbound rate limiting |
| Authentication | Application-level and account-level auth; OAuth 2.0 token exchange and refresh |
| Instrument provider | Builds Nautilus `Instrument` objects from broker symbol specifications (precision, lot size, volume step and limits) |
| Data client | Live trendbar (OHLC) and spot (bid/ask) subscriptions, plus historical trendbar requests for indicator warm-up |
| Execution client | Order submission, modification and cancellation; translation between the Nautilus order model and the cTrader position model; execution reports for state reconciliation |
| Configuration & factories | `LiveDataClientConfig` / `LiveExecClientConfig` subclasses and the factories a `TradingNode` needs |
| Test doubles | Recorded protobuf fixtures and a fake cTrader server, so reconnect, heartbeat, timeout and re-subscription logic can be tested without a broker |

**Explicitly out of scope**

Trading strategies, signals and indicators; backtesting; risk or prop-firm rule engines;
notification channels; deployment tooling; portfolio management. Those belong in the
application that *uses* this adapter. Any of them appearing in this repository is a bug in
its boundaries, not a feature.

## Requirements

- Python 3.13+
- `nautilus-trader` >= 1.228
- A cTrader Open API application of your own (see below) and a broker account reachable
  through it

## Credentials

**This repository contains no credentials and never will.** Every user registers their own
Open API application:

1. Register an application under your cTID at
   <https://openapi.ctrader.com/> and obtain a `clientId` / `clientSecret` pair.
2. Grant it access to your trading accounts at
   <https://id.ctrader.com/my/settings/openapi/grantingaccess/> with `scope=trading`,
   which returns an authorization code.
3. Exchange the code for an `accessToken` / `refreshToken` pair; the access token is
   refreshed periodically from the refresh token.

The adapter receives these values from the host application (environment variables or a
token store you control). `clientSecret` and both tokens are treated as secrets: they are
masked in the transport layer and are never written to logs.

## Usage

> Planned API, subject to change while the package is pre-alpha.

```python
from nautilus_trader.live.node import TradingNode
from nautilus_trader.live.config import TradingNodeConfig

from nautilus_ctrader import CTRADER, CTraderDataClientConfig, CTraderExecClientConfig
from nautilus_ctrader.factories import CTraderLiveDataClientFactory, CTraderLiveExecClientFactory

config = TradingNodeConfig(
    data_clients={CTRADER: CTraderDataClientConfig(demo=True)},
    exec_clients={CTRADER: CTraderExecClientConfig(demo=True)},
)

node = TradingNode(config=config)
node.add_data_client_factory(CTRADER, CTraderLiveDataClientFactory)
node.add_exec_client_factory(CTRADER, CTraderLiveExecClientFactory)
node.build()
```

## Protocol notes

- Endpoints: `demo.ctraderapi.com:5035` and `live.ctraderapi.com:5035`, TLS.
- Messages are length-prefixed frames carrying a `ProtoMessage` envelope
  (`payloadType`, `payload`, `clientMsgId`); responses are correlated to requests by
  `clientMsgId`.
- A heartbeat must be sent periodically or the server drops the connection. After any
  reconnect, both authentication levels and all subscriptions must be re-established.
- Outbound requests are rate-limited, with a separate and much tighter budget for historical
  data requests.
- Volumes are not expressed in lots, and monetary values carry an explicit digit scale
  (`moneyDigits`). Getting a scale factor wrong is the most expensive mistake available in
  this API, so scaling is covered by fixtures and verified against a live account with a
  minimum-size order before anything else is trusted.

Protobuf message definitions come from Spotware's MIT-licensed
[openapi-proto-messages](https://github.com/spotware/openapi-proto-messages); the Python
bindings are generated from them at build time. This package does **not** depend on the
official `ctrader-open-api` SDK at runtime: it is built on Twisted, while NautilusTrader is
asyncio, and it hard-pins `protobuf==3.20.1`, which conflicts with the rest of a modern
stack.

## Development

```bash
uv sync
uv run pytest
uv run ruff check .
```

Tests run entirely offline against recorded fixtures and the fake server. Tests that need a
real broker connection are opt-in and never run in CI.

## Roadmap

| Milestone | Contents |
|---|---|
| M1 | Transport: connection, framing, both authentication levels, token refresh, fake server, tests |
| M2 | Data: instrument provider, trendbar and spot subscriptions, historical warm-up |
| M3 | Execution: order submission and modification, Nautilus/cTrader order model translation, execution reports |
| M4 | Reconciliation, hardening, first tagged release |

## Disclaimer

Trading carries risk of financial loss. This software is provided as-is, without warranty of
any kind; you are responsible for anything it does with your account. This project is not
affiliated with, endorsed by or supported by Spotware Systems Ltd. or any broker. "cTrader"
is a trademark of Spotware Systems Ltd.

## License

[MIT](LICENSE)
