# Execution: what `CTraderExecutionClient` does, and what it does not guarantee

`CTraderExecutionClient` sends Nautilus orders to one cTrader account and turns the broker's
answers and pushed events back into Nautilus events and reports. It translates; it never decides
whether an order should be sent. This document says what it accepts, how the cTrader position
model maps onto Nautilus orders, how it recovers after a restart or a lost connection, and the
places where an application has to design around a difference.

Read [Instruments](instruments.md) first for how a symbol becomes a Nautilus instrument, and
[Protocol notes](protocol.md) (section 10) for what was observed on the wire. Nothing here has
yet been confirmed with a live order: see [Not yet confirmed](#10-not-yet-confirmed-against-a-live-endpoint).

## 1. What it trades

- **Hedging accounts only.** The account type is read from the broker, never assumed. Connecting
  fails with a `CTraderAccountError` naming the reason when the account is not a hedging account,
  when its access rights are not full, or when it is a limited-risk account (which needs a
  guaranteed stop on every order). Nautilus sees `OmsType.HEDGING` and a margin account.
- **The account's instruments.** The execution client trades the instruments the account's
  instrument provider holds, which the data client's configuration builds. A data client for the
  same account must therefore be configured, and `instrument_provider` must not be set on the
  execution config. An order on an instrument that is not loaded is refused.
- **Market orders**, `MARKET` with time in force `GTC` or `IOC`. Both mean "fill now" and both go
  out as immediate-or-cancel; `FOK` is refused.
- **Market brackets**: a `MARKET` entry with one or two protective legs, a `STOP_MARKET`
  stop-loss and a `LIMIT` take-profit (see [Brackets and legs](#2-brackets-and-legs)).
- **Closing a position**: a reduce-only `MARKET` order that names the position it closes, for the
  whole position or part of it.

Everything else is refused before anything is sent, with a reason. The order is rejected, and the
legs of a refused bracket are cancelled, so a refused bracket is one rejection and not three.

| Refused | Reason |
|---|---|
| An entry that is not `MARKET` | Pending entries are not supported |
| A reduce-only order that does not name its position | On a hedging account a close must say which position |
| A close on the wrong side, or of a position that is not open or not on that instrument | It would add to the position, or there is nothing to close |
| A quantity finer than a hundredth of a unit | The venue counts volume in hundredths; a rounded volume is a different order |
| A quantity in the quote currency, an emulated order, an execution algorithm | The venue has no equivalent |
| An order linked to others that is not submitted as a bracket | The links cannot be expressed on their own |
| A bracket of any size but an entry and one or two legs | A bracket is an entry with a stop-loss, a take-profit or both |
| A protective leg that is not `STOP_MARKET` or `LIMIT` | Only those two map onto a position's levels |
| Two stop-losses or two take-profits on one position | A position holds one of each |
| A leg that is not reduce-only, not on the opposite side, not the entry's size, or not the entry's child | A level covers the whole position and cannot open one |
| A stop-loss with a trigger type other than the default; a take-profit with a display quantity | The venue has no such level |
| A level at or beyond the market, or off the instrument's price grid | It has no valid distance from the fill |
| A bracket with no spot price newer than `reference_price_max_age_secs` | The levels cannot be measured (see below) |
| A client order id longer than 50 characters, or with a space, a vertical bar or a non-ASCII character | It must fit the venue's fields (see [Restarts](#4-restarts-and-reconnects)) |
| A new order or close while the connection is down | Nothing can be sent |

An order or a close that was sent and got **no answer** within `order_request_timeout_secs` is
never sent again: the lost answer may hide a fill. It stays in flight, with a WARNING, until
Nautilus's own in-flight check or a reconnect settles it ([section 5](#5-order-queries-and-in-flight-settings)).
A level amend, which sets the whole state of a position's levels, is the one request that is
repeated, up to three times.

A refusal by the broker is an `OrderRejected` (or `OrderCancelRejected`, `OrderModifyRejected`)
whose reason is the broker's error code and description. The adapter adds no account identifier
to it; whether the broker's own text could contain one is not confirmed.

## 2. Brackets and legs

In cTrader a stop-loss and a take-profit are levels on a **position**, not orders. A Nautilus
bracket, an entry with a stop-loss leg and a take-profit leg, is three orders. The adapter
translates between the two literally and never guesses what an application intends.

**Going out.** The whole bracket is one new-order request carrying both levels. The venue takes
a market order's levels only as distances, which it applies to the fill price, so the adapter
measures each level from the latest spot of the instrument (the ask for a buy, the bid for a
sell). It holds spot subscriptions for every loaded instrument for this purpose; a spot older
than `reference_price_max_age_secs` refuses the bracket.

**Correcting amend.** The fill happens at a price other than the reference, so the levels the
broker sets differ from the prices the application asked for. Once the broker's protective order
exists, the adapter amends the position's levels to the exact prices requested.

- If the broker keeps other levels than asked after three rounds, or refuses the amend, the levels
  stay where the broker set them, a WARNING is logged, and each leg reports its actual price in an
  `OrderUpdated`. Anything still showing the requested price shows the request, not the position.
- If no protective order follows the fill within `protective_order_timeout_secs`, the levels are
  set by an amend instead. If that is refused too, the legs are rejected and an ERROR is logged:
  the position stands without its levels.
- A cancel or a modify of a leg that arrives while its entry is still in flight is recorded and
  carried by this amend. A rejected entry rejects the pending modify.

**Events.** The legs' events follow the venue, not the request:

| At the broker | Entry | Legs |
|---|---|---|
| Order accepted | `OrderAccepted`, with the broker's order id | none |
| Fill (or partial fill) | `OrderFilled`, with the deal's commission | none |
| The protective order appears | none | `OrderAccepted` for each level present, at the broker's price, after the entry's fill: the levels exist only from then on |
| Entry cancelled or expired without a fill | `OrderCanceled` / `OrderExpired` | `OrderCanceled` |
| Entry refused | `OrderRejected` | `OrderCanceled` |
| A level triggers | none | the triggered leg `OrderFilled` for the volume actually closed; the other leg `OrderCanceled` |

**Venue order ids of legs.** Both levels of a position live in one broker order, and Nautilus
maps a venue order id to a single order. Each leg is therefore named after its entry order:
`<entry order id>-SL` for the stop-loss and `<entry order id>-TP` for the take-profit, for
example `6000001-SL`. The entry's order id never changes, so the names survive a restart.

**Which level triggered.** No field says so; the protective order's semantics do. For a position
that is long (the protective order sells), a fill at or above the take-profit price is the
take-profit, otherwise the stop-loss; for a short, the reverse. With one level present, that
level. The rule rests on a take-profit never filling worse than its price, which is unconfirmed.

**Cancelling a leg** removes that level from the position at the broker, by an amend that keeps
the other level. It is reported as `OrderCanceled`, or `OrderCancelRejected` if the broker
refuses or keeps the level. `cancel_all_orders` and batch cancels send one amend per position.
The adapter never refuses a cancel to keep a stop in place; closing the position first and
cleaning up the remaining legs afterwards is the application's job. Cancelling a market order is
refused: it fills at once.

**Modifying a leg** moves that level with an amend that keeps the other: `OrderUpdated`, or
`OrderModifyRejected` with the reason. A quantity can only be set to what the leg already holds,
which follows the position (the protective order's volume); anything else is refused.

**A partial close** leaves the position and its legs. The broker reduces the protective order to
the position's new volume, and the adapter reports it: an `OrderUpdated` with the new quantity on
each live leg. When a level later closes the rest, the leg is filled for the volume actually
closed and, if that still falls short of the leg, cancelled for the difference. An entry that was
only partly filled gives legs of the partial size, with the same `OrderUpdated`.

**When a position closes**, for any reason (a level, a close, a stop-out, a trader), its
remaining legs are reported as `OrderCanceled`.

**What the venue model cannot express** is rejected with a reason: a second stop on one position,
or a protective stop with no position to attach to.

**With a strategy's order manager on**, cancelling one leg makes Nautilus cancel the other
(the legs are linked as one-cancels-other), and the adapter then removes that level too. Whether
that is wanted is a setting of the application, not of the adapter.

## 3. Configuration

`CTraderExecClientConfig` carries the account settings of `CTraderDataClientConfig` and a few
of its own. It carries no instrument settings. Every client of one account shares one
connection, and the connection settings of the first config built for the account win.

| Field | Default | Meaning |
|---|---|---|
| `client_id`, `client_secret` | required | The registered application's credentials |
| `access_token` | required | An access token granting the application the account |
| `trader_login` | required | The broker's own account number; the adapter looks up the protocol's account id itself |
| `refresh_token` | `None` | Renews `access_token`; without it an expired token ends the session |
| `token_expires_at` | `None` | Unix seconds the access token expires at; without it no proactive refresh happens |
| `environment` | `"auto"` | `"auto"` reads the account's own live flag and picks the host; `"demo"` or `"live"` force it |
| `connect_timeout_secs` | `60.0` | Bound on the whole account bring-up. It also bounds how long events are held while the start's reconciliation is applied, and how long a request waits for a rebuilt model: past it, a waiting amend or close goes ahead on the old model, with a WARNING |
| `restore_retry_interval_secs` | `30.0` | How often to retry what a reconnect failed to restore |
| `reference_price_max_age_secs` | `10.0` | The oldest spot a bracket's levels may be measured from |
| `protective_order_timeout_secs` | `2.0` | How long after a bracket's fill to wait for the broker's protective order before setting the levels by an amend |
| `order_request_timeout_secs` | `30.0` | Response timeout for an order, a close or a level amend. An order or close with no answer by then is never resent, so a slow answer must not be taken for a lost one |
| `reconciliation_default_lookback_mins` | `1440` | How far back reconciliation reads fills and closed orders when Nautilus passes no lookback of its own |
| `balance_checkpoint_hour` | `None` | The hour of day, 0 to 23, at which the balance is kept in the cache key `ctrader.balance_checkpoint`. Unset, the key says `"off"` |
| `balance_checkpoint_timezone` | `"UTC"` | The IANA time zone `balance_checkpoint_hour` is in |

The config refuses, with a `ValueError`: an `instrument_provider`, an `environment` other than
the three above, a value that is not positive in `reference_price_max_age_secs`,
`protective_order_timeout_secs`, `order_request_timeout_secs` or
`reconciliation_default_lookback_mins`, an hour outside 0 to 23, and an unknown time zone.

The in-flight check belongs to Nautilus's `LiveExecEngineConfig`, not to this config; see
[section 5](#5-order-queries-and-in-flight-settings) for the values to set there.

## 4. Restarts and reconnects

### What the broker is asked

One pass reads the broker, in this order:

1. The deals of the fill window, which covers `lookback_mins` when Nautilus passes one and
   `reconciliation_default_lookback_mins` otherwise.
2. The snapshot: every open position and pending order, with the protective orders.
3. For each position that is open, has a deal in the window, or is open in Nautilus's cache on
   this account, its own order list and deal list. A position on an instrument that is not loaded
   gets no deal list, and a closed position on such an instrument is not read at all.

The positions open in Nautilus's cache matter with a persistent cache. A position that closed
while the node was down, earlier than the fill window reaches, is named by nothing else the
broker lists, and Nautilus does nothing about a cached position the broker reports no position
for. Read by its own lists, it is reported closed with its real fills, and its legs filled or
cancelled. The cache only says which positions to ask about; what is reported comes from the
broker. A cached position the broker refuses to list, or lists nothing of, is left out with a
WARNING; it never fails the pass.

**A persistent cache must not be reused across a change of trading account.** The Nautilus
account id is the same for every account (`CTRADER-001`), so a cache kept from another account
holds that account's positions. They are asked about, found unknown and left open in Nautilus.

The broker keeps trading while these are read, so the reads do not describe one moment. Their
order keeps them consistent:

- The window comes first, so a position that opens after the snapshot is not named by it. Its
  execution events, held during the pass, bring it to Nautilus.
- The position's own lists, read last, decide whether it is open and how much of it. A position
  whose deals add up to nothing left is reported closed, with its real fills, even if the
  snapshot still holds it. An open position is reported with the volume its deals leave, so a
  partial close after the snapshot does not make Nautilus add a fill of its own.
- A deal list that did not end, which a WARNING reports, may miss deals. It then never closes a
  position the snapshot holds open, and the position's volume is the snapshot's.
- A position whose deals leave it open but which the snapshot lacks makes the pass read the
  snapshot once more. If it is still missing, the position is left out with a WARNING, and its
  execution events bring it.

The venue model is rebuilt from that same snapshot. The request count is logged at INFO. The
history lists use the historical rate-limit bucket, so they never queue ahead of orders.

### At start

Nautilus asks for a mass status, and the pass above builds it. Execution events that arrive
while it is being built are held and applied only after Nautilus has reconciled the mass status
(Nautilus announces that on `reports.execution.CTRADER`), or after `connect_timeout_secs` with a
WARNING. Applied earlier, an event could address an order Nautilus does not know yet. If a
request of the pass fails, no mass status is returned, the reason is logged, and the held events
are released.

A **persistent Nautilus cache is not needed** for the node's own positions to be recognised. The
adapter writes its own record into each order it sends: the `label` holds a marker and the
entry's client order id, the `comment` holds the client order ids of the legs. A trader or another
program can write those fields too, so parsing is strict: a text that is not exactly a record
written by the adapter is not a record, and the position is then treated as foreign. The mistake
can only be "not the node's", never the node managing someone else's position.

What the mass status holds, for a position the node opened:

- the **entry** as a filled order under its own client order id, with the broker's real venue
  order id, its fills with their commission, the average price and the position id, so Nautilus
  rebuilds the position under the node's ids;
- **each leg named in the comment**, always as an explicit report, never left to Nautilus to
  infer: accepted at the broker's level while the level stands, cancelled once it is gone, and for
  a position that closed in the gap, filled (with its fill) if its level triggered and cancelled
  otherwise. The legs keep the entry as parent and each other as linked; a one-leg bracket's leg
  is reported without a contingency and keeps its parent;
- **the node's closes** under the node's client order id, when the venue model has matched them
  or the node's close of that volume on that position is still in flight (see below);
- a position report for each open position.

For a position or an order that is not the node's, on a loaded instrument:

- its entry as a filled order under the broker's id, with no client order id, with its fills;
- its closing orders with their fills, and a position report. Its levels are never synthesized
  as legs. When a protective order filled, it is reported as a reduce-only order under its own
  id, a `STOP_MARKET` or a `LIMIT` according to which level triggered;
- pending orders as they stand.

An open protective order is never reported as an order: it is its position's levels. **Every
closing order reported to Nautilus from the broker's data is reduce-only and carries its position
id**, whoever placed it; reported with Nautilus's default it would look like an opening order.

Anything on an instrument that is not loaded is left out of the mass status and counted in
[the unloaded exposure](#6-account-activity). Reports are ordered chronologically, with an
entry's report before any report whose fill closes that position, because Nautilus applies a fill
for a position it does not hold yet by opening that position.

Two consequences after a restart:

- **A leg whose level was removed while the node was down.** Without a persistent cache, Nautilus
  holds no price for the leg and cannot build the order, so the leg is not shown to the
  application as a closed order (a DEBUG line says so). With a persistent cache, the leg is
  cancelled. Legs that stand, or that triggered, are reported either way, and an application that
  claims the position sees them as lifecycle events (accepted, then cancelled or filled) for an
  order it did not submit in this run.
- **The node's own close.** The close request has no field for a client order id. After a restart
  without a persistent cache the closing order comes back as a reduce-only order with the broker's
  id and no client order id, and the application sees the close inferred, not its own order filled.

### On a reconnect

A reconnect hands Nautilus a **full mass status, the same as at start**, built by the same pass
over the same fill window. Nautilus reconciles it at once and turns what changed into events: a
leg filled in the gap becomes an `OrderFilled` with the real deal, a level removed an
`OrderCanceled`, a level moved an `OrderUpdated`. What Nautilus already holds identically is
skipped, fills included. The adapter never compares the old model with the new one itself. Events
pushed during the rebuild are applied once the mass status is sent and the balance checkpoint
has been rewritten; the account state is sent again after them. The rebuild is logged at
WARNING.

**Closed orders and fills from before the current moment reach Nautilus only inside a mass
status, never as order events.** An order that filled while the connection was down is seen in
the reconnect's mass status; no event is replayed for it.

A close whose answer the connection lost stays the node's until the next successful reconnect
pass: the pass reports it under the node's client order id if it reached the broker.

### Reports outside a mass status

The three report generators answer from the same read, without rebuilding the model:

- order reports: every open order and every live leg; with `open_only` false, also orders that
  ended without a fill. A filled order is never returned here, because without its fills Nautilus
  would infer one with no commission. It arrives through a mass status or a query;
- fill reports: the fills of the window;
- position reports: the open positions.

## 5. Order queries and in-flight settings

A `QueryOrder`, and Nautilus's own check of an order left in flight, are answered from the
broker's lists:

- a leg is answered from the venue model;
- a close is answered from its broker order, once the model has matched it. Before that, while
  the close is still in flight, it is answered from its position's lists, where the closing order
  of the same volume is named by the node's close;
- an entry or a market order is looked up in the broker's pending orders, then in the order list
  over the fill window. A partly filled entry is answered from its position, with its fills.

The answer is built from that one position's own lists. **An order that already filled is
answered with its real fills**, as a one-order mass status carrying the report and its fills
together, never as a bare filled status; Nautilus would otherwise infer a fill of its own with no
commission and refuse the real one. An answer without fills goes as a plain report. When the
query finds a bracket's entry filled, the venue model is rebuilt so that the position's legs are
recognised and its levels corrected.

**While disconnected, nothing is answered.** Nothing is answered either while a leg's entry is
still in flight, for an order on an instrument that is not loaded, or when nothing matches. In
each case there is no report, no exception, and a DEBUG line saying why.

### Recommended settings

Nautilus asks about an order that has been in flight longer than `inflight_check_threshold_ms`,
and gives up after `inflight_check_retries` more tries. The adapter answers nothing while the
connection is down, so an order sent just before a drop is asked about until the connection
returns. Set, in `LiveExecEngineConfig`:

```
inflight_check_threshold_ms × inflight_check_retries  ≥  connect_timeout_secs, with margin
```

For the default `connect_timeout_secs` of 60 seconds, `5000 × 18` (90 seconds) is a sensible
choice. The engine's default of 5 retries gives 25 seconds, which a reconnect can outlast.

If the retries run out after the entry had filled, the entry's position still opens under the
node's id when the fill arrives, but the order stays `REJECTED` with nothing filled, and its legs
are missing in Nautilus until the next restart. The late `OrderFilled` is published all the same:
read the event, not the order. An application that treats a position without legs as
unprotected will see this one that way.

## 6. Account activity

Two things the node did not start reach an application: a message for each piece of activity seen
live, and a cache key holding what stands now on instruments the node has not loaded.

### The `ctrader.account_activity` topic

Each piece of live activity is published on the message bus under the topic
`ctrader.account_activity` (`nautilus_ctrader.ACCOUNT_ACTIVITY_TOPIC`), as a frozen
`CTraderAccountActivity`. An actor subscribes with
`self.msgbus.subscribe(topic=ACCOUNT_ACTIVITY_TOPIC, handler=...)`.

| Field | Values |
|---|---|
| `kind` | `"unloaded_symbol"`: anything on an instrument the node has not loaded. `"manual_change"`: a trader acting on the node's own position. `"stop_out"`: the broker closing a position for lack of margin |
| `symbol` | The broker's symbol name |
| `subject` | `"order"` or `"position"` |
| `side` | `"BUY"` or `"SELL"`, the side of the order or position |
| `volume` | In units, a `Decimal` |
| `action` | `"opened"`, `"changed"`, `"closed"`, `"partially_closed"`, `"level_moved"`, `"level_removed"` or `"level_added"` |
| `ts_event`, `ts_init` | Unix nanoseconds |

Notes on reading it:

- A trader's close or partial close of the node's position is reported **twice**: as a
  `manual_change` activity and, because Nautilus needs it for its own bookkeeping, as an external
  closing order. A trader's move, removal or addition of a level is the activity alone, plus the
  node's leg events (`OrderUpdated` or `OrderCanceled`).
- A stop-out closes under an external `MARKET` order, so deriving the close reason from the
  closing order's type reads it as manual. The `stop_out` activity is what marks it. How a stop-out
  is flagged on the wire is unconfirmed.
- **The topic carries only activity seen live.** A reconciliation pass, at start or on a
  reconnect, publishes no activity. Activity during a disconnect shows up only in the key below.

### The `ctrader.unloaded_exposure` key

The key holds, as UTF-8 JSON in the Nautilus cache, **what stands now** on instruments the node
has not loaded: open positions, and pending orders by their remaining volume. It is the only
account of that. A position on an unloaded instrument is not in the Nautilus cache, so the key
belongs in any "is the account empty" check.

```json
[{"symbol": "XAUUSD", "subject": "position", "side": "BUY", "volume": "0.50"},
 {"symbol": "XAUUSD", "subject": "order", "side": "SELL", "volume": "1.00"}]
```

The array is sorted by symbol, subject, side and volume; `volume` is plain decimal text; an
empty account is `[]`. The key is written at connect, in every reconciliation pass (always,
whole), and after any live activity of kind `unloaded_symbol` or `stop_out`.

### The contract for an application

1. **Read the key in `on_start`.** The key is written at connect, before the node reconciles
   and the trader starts; the start-time activity has already gone out by then.
2. **Read it again whenever the topic `reports.execution.CTRADER` fires.** Nautilus publishes the
   mass status there after every reconciliation, at start and at each reconnect, and the key is
   always written before the mass status is returned or sent. The topic may fire more often, for
   instance when a query is answered with fills; a read is cheap.
3. Subscribe to `ctrader.account_activity` for what happens live.

```python
import json

from nautilus_trader.common.actor import Actor

from nautilus_ctrader import ACCOUNT_ACTIVITY_TOPIC


class ExposureWatcher(Actor):
    def on_start(self) -> None:
        self.msgbus.subscribe(topic=ACCOUNT_ACTIVITY_TOPIC, handler=self._on_activity)
        self.msgbus.subscribe(topic="reports.execution.CTRADER", handler=self._on_reconciled)
        self._read_exposure()

    def _on_reconciled(self, _mass_status) -> None:
        self._read_exposure()

    def _read_exposure(self) -> None:
        raw = self.cache.get("ctrader.unloaded_exposure")
        exposure = [] if raw is None else json.loads(raw)
        ...

    def _on_activity(self, activity) -> None: ...
```

## 7. Balance checkpoint

The protocol has no request for a past balance. The adapter rebuilds the balance at a daily
checkpoint from the broker's history of closing deals and cash flows, and keeps it in the cache
key `ctrader.balance_checkpoint`. It is a literal reading of that history; nothing judges the
value.

**Timing.** The key is written at connect, before the connect returns and so before the node
reconciles and the trader starts; again on every reconnect; and by a timer at the checkpoint plus
30 seconds, every day. With `balance_checkpoint_hour` unset, the key is written once at connect
and says it is off; a reconnect writes nothing, and no timer runs:

```json
{"status": "off", "checkpoint": null, "balance": null, "currency": null,
 "reason": null, "first_deposit": null}
```

**Format.** UTF-8 JSON with the keys `status`, `checkpoint`, `balance`, `currency`, `reason` and
`first_deposit`.

- `status` is `"available"`, `"unavailable"` (with a `reason`) or `"off"`.
- `checkpoint` is the moment `T` the value belongs to, `YYYY-MM-DDTHH:MM:SS.sssZ`. A value read
  just after a new `T`, before the timer has rewritten it, is recognisably the previous day's.
- `balance` and `first_deposit` are decimal text with exactly the account's money digits after the
  point, never an exponent. `currency` is the deposit currency.

**The rule.** `T` is the most recent occurrence of the configured hour in the configured time
zone. The balance at `T` is the balance after the last change strictly before `T`. A change is a
closing deal at its execution time or a cash flow at its own time; opening a position does not
change the balance, and an open position's floating result never counts. The value is
`available` only when the balance versions run without a gap from that change to the newest one
seen, which reaches the trader's current version, and each balance follows from the one before it.
A checkpoint after the account's registration and before its first deposit reads `available` with
a zero balance.

**Daylight saving.** An hour that a clock change skips means the first instant after it that
exists. An hour that occurs twice means its first occurrence.

**How far back it reads.** At connect and at reconnect, at most 8 weekly windows of history are
read inline, so neither is held up. If the answer is further back, the key says `unavailable`
with the reason `history incomplete` until a background walk, capped at 156 weeks, settles it and
writes the key again. The first-deposit walk works the same way; until it finds the deposit,
`first_deposit` is `null`. A walk cut short by a lost connection writes nothing: the key keeps its
previous value, with its own `checkpoint`, until the reconnect writes it.

**`unavailable` reasons.** The reason is always one of a fixed list:

| `reason` | Meaning |
|---|---|
| `history request failed` | A history request was refused or failed |
| `history incomplete` | History could not be read far enough back, or a list did not end, or the background walk has not finished yet |
| `balance versions do not chain` | A gap in the versions, a balance that does not follow from the one before, or the account reports no balance version |
| `account opened after the checkpoint` | The account's registration is later than `T`: it did not exist then |
| `mixed money scales` | Items of the history carry different money digits, so there is no single scale to state them in |

**`first_deposit`** is the balance after the earliest cash flow that is a balance deposit by its
operation type and was made on a zero balance (its balance equals its amount). Only a balance
deposit counts. With no registration time known, there is no forward walk from the registration,
and `first_deposit` may then be a later deposit that was made on a zero balance.

## 8. Account state

The account is a margin account in the deposit currency. The adapter emits an `AccountState`:

- at connect, and again after a reconnect;
- after every closing deal and every deposit or withdrawal, with the balance the broker states;
- whenever a position's used margin changes;
- on an account query.

The balance is the broker's own, scaled by its `moneyDigits`; nothing is locked, because the venue
reports no such figure, so free equals total. Margin is the broker's one figure per position, summed
per instrument and given as both initial and maintenance margin; positions on instruments the node
has not loaded go into one account-wide entry. Nautilus replaces its whole margin set with each
state, so every emission carries every open position's margin.

The broker's balance changes only when volume closes: an opening deal shows its commission on the
fill, and the balance stays as it was. Every `OrderFilled` carries the deal's commission, with the
sign flipped to Nautilus's convention (a charge is positive).

**Known differences from Nautilus's own figures.**

- **Swap** accrues on the position at the broker, not on the balance. It is not in Nautilus's
  equity until the position closes, and is then realised in the closing deal. When and how it
  accrues on an open position is unconfirmed. An application whose limits count equity including
  swap must account for that.
- **Realised profit** in Nautilus differs from the broker's by swap, by conversion fees (both
  reach the balance through the closing deal and never through a fill), and by the timing of the
  opening commission: Nautilus counts it at the open, the broker's balance takes it at the close.

## 9. Known limits

- **A manual change at the same moment as the node's own is taken for the node's.** The adapter
  tells a trader's change from its own by what it has in flight: a level amend of that position,
  or a close of that volume on that position. A trader's change landing while one is in flight is
  read as the answer to it. For a close, the broker's answer to the node's request is always the
  node's close. Any other closing order of that volume on that position is taken for it if the
  broker created it after the newest broker time the node had seen when it sent the close. That
  time comes from the broker's own stamps on execution events and spots, never from the node's
  clock. So a trader's close created after the last broker stamp the node saw, but before the
  node's close reached the broker, is taken for the node's. This includes a close whose answer
  was lost: until the next successful reconnect pass, such a trader's close of the same volume on
  that position during the outage is taken for the node's.
- **A level re-added by hand after its leg was cancelled is adopted after a restart.** After a
  restart a leg lives exactly while its level does, so the level a trader put back makes the leg
  alive again under its original id. Before the restart, the same re-added level is only a
  `manual_change` activity.
- **A position whose leg record does not parse gets no legs.** If the `comment` of the node's own
  entry cannot be read, the position is still the node's, but its levels stay at the broker with
  no legs in Nautilus, and a WARNING says so.
- **The node's close after a restart without a persistent cache is inferred**, as described in
  [section 4](#4-restarts-and-reconnects).
- **A leg removed while the node was down is not shown as closed** without a persistent cache,
  also described there.
- **With the order manager on, cancelling one leg cancels the other**, and the adapter removes
  both levels.
- **A late fill after the in-flight retries ran out** leaves the order `REJECTED`; see
  [section 5](#5-order-queries-and-in-flight-settings).
- **A close's event later than its timeout, with no query in between, is a trader's close.** On
  a live connection a close stops being in flight once `order_request_timeout_secs` passes with no
  answer. If a query named its broker order meanwhile, that order's later events stay the node's
  close's. Without one, an event of it after the timeout is reported as an external reduce-only
  order and a `manual_change`.

## 10. Not yet confirmed against a live endpoint

No order has yet been sent through the adapter on a live account, so everything about sending is
read from the schema and the broker's own terminal's behaviour, not observed from the adapter.
Each such point is marked in the source with a `TODO(verify):` comment saying what would settle
it. The main groups:

- **Scales and defaults on a new order**: that volume in hundredths of a unit agrees with each
  instrument's quantity units; that relative levels are distances in 1/100000 of a price applied
  from the fill, in the direction assumed; that immediate-or-cancel fills a market order the way
  the broker's own terminal's does; which trigger method a stop-loss set by a relative distance
  uses.
- **Amends**: that leaving a level out of an amend removes it; whether an accepted amend sends an
  execution event besides its answer.
- **The records in `label` and `comment`**: their limits, whether the venue counts characters or
  bytes, whether it truncates or rejects an over-long field, and whether it returns them verbatim.
- **Events not yet seen**: a stop-out and how it is flagged; a trader-update and a margin-change
  event; a protective order that triggers partially, and whether a partly filled protective order
  reports its total volume or its rest.
- **Matching the node's close**: whether the broker's closing order carries the volume the node's
  close asked for. The match of a close to the node's order rests on it. Also, that a closing
  order's creation time, the execution events' times and a spot's `timestamp` are all on the
  broker's one clock: the bound that keeps a trader's earlier close from being taken for the
  node's compares them.
- **Rejected orders**: whether a rejected order carries a position id.
- **Error text**: whether the broker's description of a refusal can ever contain an account
  identifier. The adapter adds none, but it passes the broker's text through.
- **History lists**: the order lists come in, which of an order's times the order list filters by,
  paging of the order and deal lists past one page, whether the list holds a rejected order,
  whether the edges of a window are inclusive, and whether the cash-flow list has no pages and
  takes at most a week.
- **The balance checkpoint**: that an account's first funding is a balance deposit, and the sign
  of a withdrawal's amount.

[Protocol notes](protocol.md), section 10, states which of the facts underneath are confirmed
and which are not.
