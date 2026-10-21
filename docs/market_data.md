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

A bar's `ts_event` is the end of the period it covers: the open time the venue gave the bar,
plus the period. A 15-minute bar covering 09:00–09:15 carries `ts_event = 09:15`.

That open time is used exactly as sent and is never rounded to a multiple of the period, because
a period's boundaries are not necessarily aligned to midnight: a daily bar covers the venue's
trading day rather than a calendar day, opening at 21:00 or 22:00 UTC depending on the time of
year. Rounding it would put every daily bar on the wrong day.

The venue timestamps a bar with its *opening tick*. On the intraday periods that has always been
the period boundary itself; an open time that is not one is logged and published as it came,
rather than corrected into an assumption about where the boundary should be.

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
  from history and emitted before the stream resumes. The range is fetched once; bars that close
  while that fetch runs are then asked from history one by one. The backfill runs in the
  background, without delaying the session's readiness, at the lowest request priority, so
  commands and other requests are not held back by it; live bars arriving meanwhile are held
  and replayed after it. Another reconnect restarts a running backfill, and an unsubscribe
  cancels it, as does a disconnect, which returns only once every backfill has ended. A
  backfill that fails for any reason but the lost connection is logged at ERROR and retried
  every `restore_retry_interval_secs`, like any other failed restore: a failed retry is a
  WARNING, and the third failed retry in a row an ERROR again.
- **A backfill after the live trendbars come back.** If the live trendbar subscription itself
  could not be restored, or a first subscribe got no answer, no stream follows the backfill.
  The subscription is retried like any failed restore, and the retry that the venue confirms
  starts another backfill at once, so the bars that closed in between are emitted, in order,
  without waiting for the market to trade. A subscribe whose answer was lost may still have
  reached the venue; if its live trendbars arrive before a retry confirms it, the first of them
  starts that backfill instead. Either way it runs once.

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

**A period's boundaries are learnt, not assumed.** The 4-hour, 12-hour and daily periods do not
divide the trading day's offset, so their boundaries are not multiples of the period, and the
offset itself moves by an hour with US daylight saving. Which bars have already closed is
therefore worked out from an offset read from the bars the venue sends — the stream, and the
history a backfill fetches — and replaced whenever a bar disagrees with it. A move is logged at
INFO. Until the first bar has settled the offset, a backfill leaves the bar that closed most
recently to the stream, because it does not yet know which boundary that is.

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

A request never changes what the live stream emits. Nautilus hands a response to the requester
only, never to live bar subscribers, so the two are independent, and a bar can reach both: if
the request is answered after the bar forming at subscription time has closed, the response
holds that bar too. Compare `ts_event` if a strategy must not count it twice.

Nautilus does put the response's bars in its cache, though. With
`DataEngineConfig.validate_data_sequence=True` (default `False`), its engine then drops, with a
WARNING, a live bar whose `ts_event` is older than the newest bar cached for its bar type, so a
live bar that closes before a response's last bar but arrives after the response never reaches
the subscribers.

A historical request is paged backwards from its end, and the forming bar the venue also serves
is dropped, since it is not a bar yet. A request with a `start` delivers no
bar opening before it, although the venue's pages reach back past it, and one with a `limit`
delivers the newest `limit` closed bars. With neither a `start` nor a `limit` exactly one page
is fetched: that call is asking for "the most recent bars", not for the whole history the venue
holds.

Historical requests are rate-limited separately from everything else and much more tightly, so
a large warm-up cannot starve the subscriptions running beside it.

## When a request fails

Nautilus has no failure response: a request is answered with data or with none, and the
callback receives only the request id. So a request this client cannot serve - bars, an
instrument, or a kind of history the adapter does not provide - is answered in two steps, in
this order:

1. a `CTraderRequestFailed` is published under `REQUEST_FAILED_TOPIC`;
2. the request is answered with no data, so the callback fires and the request is complete.

Every request the client receives gets exactly one answer: bars, or a failure and no bars. This
holds even for a request cancelled by a disconnect before it started. A failure part way through
a request that needs several pages delivers none of the pages already served, because a
silently shortened warm-up is worse than an obvious failure.

Nautilus may split one strategy request into several client requests, for example when a data
catalog already holds part of the range. The strategy can then get bars from one part and a
failure from another, under the same id: treat any failure under an id as the whole request
having failed.

A strategy that needs to tell "no bars" from "failed" records the failures and checks for one
in its callback:

```python
from nautilus_trader.core.uuid import UUID4

from nautilus_ctrader import REQUEST_FAILED_TOPIC, CTraderRequestFailed


def on_start(self) -> None:
    self.failed: dict[UUID4, CTraderRequestFailed] = {}
    self.msgbus.subscribe(topic=REQUEST_FAILED_TOPIC, handler=self.on_request_failed)
    self.request_bars(bar_type, start=start, limit=500, callback=self.on_bars_requested)


def on_request_failed(self, failure: CTraderRequestFailed) -> None:
    self.failed[failure.request_id] = failure


def on_bars_requested(self, request_id: UUID4) -> None:
    failure = self.failed.pop(request_id, None)
    if failure is not None:
        self.log.warning(f"Warm-up failed: {failure.reason}")
```

The failure always arrives before the callback, with the live and the plain data engine alike.
`request_id` is the id `request_bars` returned and the callback receives;
`data_type.metadata["bar_type"]` names the bar type. `reason` is short and fit to show a person:
`venue refused: <error code>`, `timed out`, `not connected`, `connection lost`,
`connection closed`, `not sent`, `instrument <id> is not loaded`, `invalid venue response`,
`<data> requests are not supported`, why a bar type has no trendbars (such as
`trendbars are only available for the BID price type`), and `request failed` or
`internal error` for anything else. It carries no venue text beyond an error code.

Nautilus logs the empty answer at WARNING as `Received <Bar[]> data with no bar for <bar type>`;
after a failure that line is expected. A subscriber that raises is logged and does not hold the
answer back, but it can keep the failure from the subscribers after it.

A request made with a `time_range_generator` param is split by Nautilus into a series of
requests of its own, one interval each. A failure then carries the id of one of those rather
than the strategy's, and several failures may arrive, one per failed interval. Nautilus cannot
tell a failed interval from an empty one: its default generator tries the next of
`durations_seconds`, and ends the series once every duration has come back empty, so a failure
can end the request early. Bars from intervals already served are still delivered, so the
guarantee against a shortened warm-up does not hold for such a request.

## Quotes, and the conversion symbols nobody asked for

A quote subscription publishes a `QuoteTick` per spot event, once both sides are known. Spot
events are one-sided — an event carries only the side that moved — so the last bid and the last
ask are kept per symbol and a tick is published as soon as a pair exists. After a reconnect both
sides are forgotten, so a price from before an outage is never paired with one from after it.

A spot subscription is shared by every client of the account, and a client joining one that is
already live sends the venue nothing, so whatever the venue sent when the subscription opened is
not repeated for it. A quote subscription, or a conversion leg, that joins a live one therefore
publishes the account's last spot of the symbol at once, when:

- both sides of it are known;
- it came since the last reconnect, and the connection is up;
- the data client is not already publishing quotes of the symbol.

Its `ts_event` is that spot's time at the broker, which may be well before the subscription on a
quiet instrument; its `ts_init` is when it was published. Judge such a quote's age by `ts_event`.

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
way: resolving a chain for every symbol a broker offers would be pointless work. Chains are
resolved once each time the account connects, and again if the venue announces that one of
their symbols changed. A network drop the connection recovers from on its own keeps them.
Instruments that need the same chain at the same time share one request for it.

Two different failures are reported differently, because they need different responses:

- The chain cannot be built at all — the venue answers with an empty chain, or one of its
  symbols cannot be built as an instrument. The instrument can never be valued in the account
  currency, so it is dropped from the provider and recorded as a load failure, at ERROR.
- The request was refused or never answered — a venue refusal of the chain request or of a
  leg's symbol request, whatever its error code, a timeout, or a session that is reconnecting.
  The instrument is fine; the request was not, and nothing has been learned about the chain.
  It stays loaded and unconverted, with a WARNING. A quote or bar subscription refused this way
  stays subscribed in Nautilus, and its chain is retried every `restore_retry_interval_secs` of
  the data client's own config: once per instrument, however many of its subscriptions wait.
  A failed retry is a WARNING, and the third failed retry in a row an ERROR. Once a retry
  succeeds, every waiting subscription of the instrument completes as if the first attempt had
  succeeded, for every subscriber, and one INFO line reports each. If a retry finds that the
  chain cannot be built at all, the instrument is dropped with an ERROR and the retry ends. An
  unsubscribe of the last waiting subscription, or a disconnect, stops the retry; a subscription
  refused while the client is not connected is not retried. A chain refused during connect is
  retried by the first subscription to the instrument.

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
refresh token as well: a refresh token is single-use, so the old one stops working (confirmed;
see [protocol.md](protocol.md) section 5). The new pair exists only in memory unless the
application stores it, and losing it means the next process start has nothing valid to
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
    trader_login=trader_login,
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

When the config leaves the tokens to the environment (`CTRADER_ACCESS_TOKEN` and the others;
see [execution.md](execution.md#3-configuration)), the adapter only reads them there. The
listener must store the new pair wherever the next process start will find it in its
environment, for example the file the environment is loaded from.

If the listener raises, the failure is logged at ERROR and the session carries on with the new
tokens in memory. Persisting them is the only thing that survives a restart, so a listener that
can fail should say so loudly.

The instrument provider belongs to the account client as well, so every client of one account
sees the same instruments: one dropped as unusable is dropped for all of them. It is built from
the instrument settings of the first config that asks for it — `instrument_provider`,
`asset_class_overrides` and `fail_on_instrument_error`. A later config for the same account
with different instrument settings fails the node build with a `ValueError` naming them,
rather than having them silently ignored. Passing the same config object, as above, never
trips this.
