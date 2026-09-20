# Market data: what `CTraderDataClient` delivers, and what it means

`CTraderDataClient` serves quote ticks and bars for one cTrader account. It translates and
routes; it decides nothing about what the data is for. This document says exactly what arrives,
when, and what it does *not* guarantee — the parts an application has to design around.

Read [Instruments](instruments.md) first for how a symbol becomes a Nautilus instrument.

## Bars are BID bars, always

cTrader's only bar product is the trendbar, and trendbars are built from bid prices. The
adapter labels them accordingly: a bar subscription or request is accepted only for the `BID`
price type and `EXTERNAL` aggregation, and `LAST`, `ASK` or `MID` is rejected with a reason
that says so.

```python
bar_type = BarType.from_str("EURUSD.CTRADER-15-MINUTE-BID-EXTERNAL")
```

Labelling bid bars as `LAST` would be cheaper and would be a lie: everything downstream that
keys on price type — fill simulation, spread handling, any comparison against another venue —
would quietly treat a bid as a trade price.

The consequence is worth stating plainly, because it bites later rather than immediately: data
recorded here is bid data. An application that replays it elsewhere, or that builds a strategy
against bars from a source whose bars are trade prices or mid prices, is comparing two
different series. The difference is roughly a spread, it is not constant, and it does not
announce itself.

Supported periods are the ones the venue serves: 1, 2, 3, 4, 5, 10, 15 and 30 minutes, 1, 4 and
12 hours, and 1 day. Anything else is rejected rather than approximated.

## `ts_event` is the bar's close

A bar's `ts_event` is the end of the period it covers: its period-aligned open boundary plus
the period. A 15-minute bar covering 09:00–09:15 carries `ts_event = 09:15`.

The venue timestamps a trendbar with the minute of its *opening tick*, which is not necessarily
the period boundary — a quiet market can open a bar late. The adapter aligns that timestamp
down to the period boundary and adds the period, so bars of the same type are evenly spaced
whatever the venue's opening tick happened to be.

## When a bar closes

cTrader has no bar-close event. Live trendbars arrive *inside* spot events: each event carries
the currently forming bar for every subscribed period. So "the bar is finished" has to be
decided, not received. Four things can close a bar, and each closed bar is emitted exactly
once, in ascending order, whichever of them got there first:

- **The stream.** An update arrives for a newer boundary. The previous bar cannot change any
  more, so it is closed with the last state the stream showed.
- **A timer.** If no such update arrives — an illiquid symbol, a session break, a weekend — a
  timer fires shortly after the bar's end (`bar_close_grace_secs`, 1 second by default) and the
  bar is requested from history instead.
- **History.** The historical request is the authority when it is used: the venue's own closed
  bar replaces the streamed state. History can lag a just-closed bar, so it is asked up to
  `1 + bar_close_history_retries` times, a grace period apart. If it still has nothing, the last
  streamed state is emitted with a WARNING, and a repeat of the same condition drops to DEBUG
  rather than flooding the log. A request that finds no connection is not one of those
  attempts: the bar keeps waiting, and the backfill below closes it.
- **A backfill after a reconnect.** Bars that closed while the connection was down are fetched
  from history and emitted before the stream resumes.

Two rules follow from this, and applications depend on both:

- **The first update after connecting is a baseline, not a new bar.** A subscription's first
  trendbar may be a bar that has already closed — the venue sends the last bar even when the
  market is shut. It is treated as state, not as a close, and is resolved against history like
  any other closed boundary. Nothing before the bar forming at subscription time is ever
  emitted.
- **No empty bars are invented.** A period during which the venue reported nothing produces no
  bar. Bar streams from this adapter have gaps — over weekends, holidays and market breaks —
  and an application that needs a continuous series must fill it itself. Fabricating a flat bar
  would be inventing prices that never traded.

## Warm-up: subscribe first, then request

To start a strategy with history and no gap between the history and the live stream, subscribe
before requesting, and end the request at the moment you subscribed:

```python
from datetime import timedelta

from nautilus_trader.model.data import BarType


def on_start(self) -> None:
    bar_type = BarType.from_str("EURUSD.CTRADER-15-MINUTE-BID-EXTERNAL")
    self.subscribe_bars(bar_type)
    subscribed_at = self.clock.utc_now()
    self.request_bars(bar_type, start=subscribed_at - timedelta(days=5), end=subscribed_at)
```

In the other order — request first, subscribe afterwards — the bars that close between the two
calls belong to neither, and nothing reports them missing.

Doing it this way is safe because the client remembers what a request delivered: the newest bar
a request served is marked as emitted for that subscription, so the live stream will not send
it a second time. A historical request is paged backwards from its end, and the forming bar the
venue also serves is dropped, since it is not a bar yet. With neither a `start` nor a `limit`
exactly one page is fetched: that call is asking for "the most recent bars", not for the whole
history the venue holds.

Historical requests are rate-limited separately from everything else and much more tightly, so
a large warm-up cannot starve the subscriptions running beside it.

## Quotes, and the conversion symbols nobody asked for

A quote subscription publishes a `QuoteTick` per spot event, once both sides are known. Spot
events are one-sided — an event carries only the side that moved — so the last bid and the last
ask are kept per symbol and a tick is published as soon as a pair exists. After a reconnect both
sides are forgotten, so a price from before an outage is never paired with one from after it.

Beyond the instruments you subscribe to, the client also subscribes the symbols needed to
convert each instrument's quote currency into the account's deposit currency. This is on by
default (`subscribe_conversion_quotes`) and it is the difference between a correct account
valuation and a silently wrong one: Nautilus builds its exchange rates from the quotes in the
cache, not from the subscription list, so an instrument quoted in a currency the account is not
denominated in cannot be valued unless a conversion symbol is quoted too. A quote tick that no
one subscribed to still reaches the cache, which is exactly what is needed here.

The chain is resolved at the venue, not guessed, and each one is reported once at INFO:

```
Conversion EUR->USD: subscribed EURUSD
```

For the instruments named in `load_ids`, the chains are prepared during connect, before the
client reports connected — so an instrument that cannot be valued is known to be unusable
before anything can trade it. An instrument that arrived through `load_all` gets its chain on
its first subscription instead — quotes or bars alike, since the valuation needs it either
way: resolving a chain for every symbol a broker offers would be pointless work. Chains are resolved once per connection, and again if the venue announces that
one of their symbols changed.

Two different failures are reported differently, because they need different responses:

- The chain cannot be built at all — the venue has no route, or one of its symbols cannot be
  loaded. The instrument can never be valued in the account currency, so it is dropped from the
  provider and recorded as a load failure, at ERROR.
- The request was refused or never answered — a venue refusal, a timeout, or a session that is
  reconnecting. The instrument is fine; the request was not, and nothing has been learned about
  the chain. It stays loaded and unconverted, with a WARNING, and the next subscribe tries
  again.

## `synthetic_quote_size`

cTrader spot events carry no size. Nautilus `QuoteTick` requires one on each side, so by
default both sides carry zero.

Zero is honest, and for some consumers it is unusable: an L1 order book built from zero-size
quotes has no best bid or ask, and a simulated venue fed those quotes fills nothing. Setting
`synthetic_quote_size` puts that one size on both sides of every quote. It is a fiction — the
venue said nothing about size — and it exists so that a component requiring a nonzero size can
run. It must never be read as depth.

## Instruments that could not be prepared

The provider records every instrument it could not build or could not value, with the symbol
name and the reason, and exposes them read-only:

```python
for failure in client.instrument_provider.failures:
    print(failure.symbol, failure.reason)
```

The adapter does not decide whether a failure should bring the node down. What it guarantees is
narrower and firmer: **it will not serve an instrument it could not prepare.** Such an
instrument is not published, not subscribed and not quoted, and every attempt to use it is
refused with a reason.

What that should mean for the application is the application's call. With the default
`fail_on_instrument_error=False`, the rest of the instruments load and the node starts without
the broken one. With `fail_on_instrument_error=True` the failure is raised instead, so bringing
up the node fails rather than starting with a gap. A transport layer has no way to know whether
a missing instrument is a nuisance or a disaster.

## Persisting refreshed tokens

The adapter refreshes the access token over the socket while it runs, and a refresh rotates the
refresh token as well: the old one may stop working. The new pair exists only in memory unless
the application stores it, and losing it means the next process start has nothing valid to
authenticate with.

One connection per account does the refreshing, so the listener has to be registered on that
one client. Get it from the config, register the listener, then build the node:

```python
from nautilus_trader.common.component import Logger
from nautilus_trader.live.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode

from nautilus_ctrader import (
    CTRADER,
    CTraderDataClientConfig,
    CTraderLiveDataClientFactory,
    account_client_from_config,
)

data_config = CTraderDataClientConfig(
    client_id=client_id,
    client_secret=client_secret,
    access_token=access_token,
    refresh_token=refresh_token,
    account_id=account_id,
)

account = account_client_from_config(data_config, Logger("CTRADER"))
account.add_token_listener(store_tokens)  # (access_token, refresh_token, expires_at_secs)

node = TradingNode(config=TradingNodeConfig(data_clients={CTRADER: data_config}))
node.add_data_client_factory(CTRADER, CTraderLiveDataClientFactory)
node.build()
```

`account_client_from_config` returns the one client for that account, building it on the first
call and returning the same instance afterwards. **This call is the authoritative one.** A
second config for the same account gets that same client, and its connection settings —
timeouts, retry intervals — are ignored; an `environment` that differs from the first one is
ignored too and logged at WARNING, because it would have chosen a different host.

If the listener raises, the failure is logged at ERROR and the session carries on with the new
tokens in memory. Persisting them is the only thing that survives a restart, so a listener that
can fail should say so loudly.
