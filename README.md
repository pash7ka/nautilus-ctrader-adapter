# nautilus-ctrader-adapter

A [cTrader Open API](https://help.ctrader.com/open-api/) adapter for
[NautilusTrader](https://github.com/nautechsystems/nautilus_trader): market data and order
execution against any broker that exposes cTrader Open API.

> **Status: early development (pre-alpha).** The transport layer is in place and tested
> offline; the instrument provider, data client and execution client come next. The public API
> is not stable, there is no PyPI release yet, and nothing here should be pointed at a live
> account yet. See [Roadmap](#roadmap).

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
| Authentication | Application-level and account-level auth; access-token refresh over the socket (the one-time authorization-code exchange stays with the host application — see Credentials) |
| Instrument provider | Builds Nautilus `Instrument` objects from broker symbol specifications (precision, lot size, volume step and limits) |
| Data client | Live trendbar (OHLC) and spot (bid/ask) subscriptions, plus historical trendbar requests |
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
2. Send yourself through `https://openapi.ctrader.com/apps/auth` with your `client_id`,
   a `redirect_uri` and `scope=trading`, sign in with your cTID and select the accounts to
   expose. The redirect carries back an authorization code.
3. Exchange the code at `https://openapi.ctrader.com/apps/token` for an `accessToken` /
   `refreshToken` pair; the access token is refreshed from the refresh token before it
   expires.

Steps 2 and 3 are one-time steps you run yourself — step 2 needs a browser and a redirect
URI — and the adapter performs neither. The adapter refreshes the access token while running
and hands the new pair back to your application to persist.

`scripts/get_tokens.py` performs steps 2 and 3 locally: run `uv run python scripts/get_tokens.py`.
It reads `CTRADER_CLIENT_ID` and `CTRADER_CLIENT_SECRET` from a `.env` file (ignored by git),
writes the resulting tokens back into it, and lists the accounts the token grants. The
application's redirect URI must be `http://localhost:8080/callback`, or whatever
`--redirect-uri` says.

The adapter receives these values from the host application (environment variables or a
token store you control). `clientSecret` and both tokens are treated as secrets and are
never written to logs: the transport logs no payload bytes at all.

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
- Each frame is a 4-byte big-endian length prefix followed by a serialised `ProtoMessage`
  envelope (`payloadType`, `payload`, `clientMsgId`), capped at 15 MB. Responses are
  correlated to requests by `clientMsgId`.
- A heartbeat must be sent if the connection would otherwise be idle for more than 30
  seconds. This adapter sends one after 10 seconds of outbound silence and does not answer
  the server's own heartbeats. 90 seconds without any data from the server is treated as a
  lost connection, detected within a few seconds after the 90 seconds.
- After any reconnect, both authentication levels and all subscriptions must be
  re-established.
- Outbound requests are rate-limited, with a separate and much tighter budget for historical
  data requests.
- Volumes are not expressed in lots, and monetary values carry an explicit digit scale
  (`moneyDigits`). Getting a scale factor wrong is the most expensive mistake available in
  this API. None of that conversion exists yet: when it is built, each converter will be
  checked against a recorded real response, and scaling verified against a live account with
  a minimum-size order, before anything else is trusted.

Protobuf message definitions come from Spotware's MIT-licensed
[openapi-proto-messages](https://github.com/spotware/openapi-proto-messages); the Python
bindings are generated from them by `scripts/gen_protobuf.py` and committed, so installing
needs no protoc toolchain. This package does **not** depend on the official
`ctrader-open-api` SDK at runtime: it is built on Twisted, while NautilusTrader is asyncio,
and it hard-pins `protobuf==3.20.1`, which conflicts with the rest of a modern stack.

[docs/protocol.md](docs/protocol.md) documents the wire protocol in full, including what is
still unknown about scaling, the easiest thing to get expensively wrong.

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
