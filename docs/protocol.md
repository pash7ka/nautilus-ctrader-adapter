# The cTrader Open API wire protocol, as this adapter implements it

This document describes the transport-level behaviour of the cTrader Open API protobuf
protocol and how this adapter implements it. It is written for an outside reader — a
Spotware reviewer, or anyone integrating cTrader with something other than this adapter —
and it names its sources in prose rather than assuming familiarity with this codebase.

Message definitions come from Spotware's MIT-licensed
[openapi-proto-messages](https://github.com/spotware/openapi-proto-messages) schema. Where a
fact below is not yet confirmed against a live connection, it is marked **unconfirmed** and
carries a `TODO(verify):` comment at the corresponding place in the source.

A fact marked **confirmed** was observed on a real connection to the API, against a real
broker account, and is fixed in a recorded fixture where it is a value rather than a
behaviour. Everything else is read from the schema or its documentation and is an assumption
until a connection settles it.

## 1. Transport

The API is reached over TLS at `demo.ctraderapi.com:5035` or `live.ctraderapi.com:5035` — the
same port for both.

Each frame on the wire is a 4-byte big-endian length prefix followed by a serialised
`ProtoMessage` envelope, capped at 15,000,000 bytes. This is exactly the framing Twisted's
`Int32StringReceiver` implements (`structFormat = "!I"`, a 4-byte prefix, `MAX_LENGTH =
15_000_000`), which is what Spotware's own reference SDK is built on. A length prefix
declaring more than the cap is treated as a protocol violation and the connection is dropped
rather than read further.

### Which host an account belongs to

**The account's own `isLive` flag decides the host** (confirmed), and nothing else does:
account authentication on the wrong host is refused with `CANT_ROUTE_REQUEST`. How the access
token was issued makes no difference — a token obtained through a sandbox authorization flow
still routes by the account's flag.

The account list (`ProtoOAGetAccountListByAccessTokenReq`) is served on **either** host
(confirmed), which is what makes host selection automatic: this adapter opens a short
connection to one host, reads the flag for the configured account, and connects to the host
that flag names. Naming the environment explicitly skips that step.

## 2. Envelope and correlation

Every message on the wire is wrapped in a `ProtoMessage{payloadType, payload, clientMsgId}`
envelope: `payloadType` identifies which message `payload` contains, and `clientMsgId` is an
opaque string the sender chooses.

A response to a request carries back the same `clientMsgId` the request was sent with, and so
does an error response. This is why a rejected request fails immediately with the venue's
error rather than sitting until its own timeout: the error is matched to the pending request
by `clientMsgId` exactly like a normal response would be, and delivered as an exception
instead of a result.

A `clientMsgId` does not by itself make a message a response. The execution events that another
client's request causes, such as a close made in the broker's terminal, arrive carrying that
client's `clientMsgId` (confirmed). A message whose `clientMsgId` matches no request the
connection is waiting for is therefore an event, delivered as one.

## 3. Heartbeat

The venue requires a heartbeat if the connection would otherwise be idle for more than 30
seconds, per the schema's own comment on the heartbeat message. The server also sends its own
heartbeats when it has been idle.

This adapter sends a heartbeat after 10 seconds of outbound silence — a full missed beat of
headroom inside the server's 30-second tolerance — and deliberately does **not** answer the
server's inbound heartbeats. Spotware's reference SDK replies to every inbound heartbeat, but
the schema describes heartbeats as a keep-alive signal, not a ping/pong exchange, and never
replying to one removes any way for a peer to drive a heartbeat reply loop. **Unconfirmed**:
that the venue expects no reply to its own heartbeats.

Outbound silence is only half the picture: on a half-open TCP connection (network loss without
a reset, a NAT timeout), writes keep succeeding into the kernel buffer long after nothing is
actually arriving. This adapter also treats 90 seconds with no inbound frame — three missed
server heartbeats at the 30-second tolerance — as a lost connection and reconnects. Silence is
checked periodically, so the loss is detected within a few seconds after the 90 seconds, not at
exactly 90 seconds.
The server does send frames on an otherwise idle authenticated connection (confirmed): in a
recorded session with no market-data subscription, stretches of up to two minutes with no
other message passed without the 90-second detector firing. Heartbeats themselves were not
recorded, so this is inferred from the detector staying silent.

## 4. Authentication

Authentication has two independent levels, each its own request:

1. `ProtoOAApplicationAuthReq` with the application's `clientId` and `clientSecret`.
2. `ProtoOAAccountAuthReq` with the target `ctidTraderAccountId` and an access token.

Both levels must be re-established after any reconnect, along with every subscription that
was active before the disconnect — the venue does not remember either across a new socket.

## 5. Tokens and losing authentication

The authorization-code exchange (the interactive, browser-based step that produces the first
access/refresh token pair) is an HTTP call, not part of this wire protocol, and is a one-time
job for the host application. Everything below happens over the already-authenticated socket.

**Refresh.** `ProtoOARefreshTokenReq` carries the current refresh token and returns a new
access token, a new refresh token, and `expiresIn`. This adapter refreshes proactively, ahead
of the access token's known expiry, and reactively, when account authentication is rejected.

- *Proactive*: refresh runs 15 minutes before the token's expiry. A refresh that times out or
  fails for another transient reason is retried once the minimum interval between refreshes
  has passed, and the margin is several times that interval, so an active session never idles
  into expiry. A refresh already due when the session connects or reconnects runs on that
  connection before account authentication, so the session authenticates with the new token.
  Refreshing under an authenticated session makes the venue end it — it sends
  `ProtoOAAccountsTokenInvalidatedEvent` and `ProtoOAAccountDisconnectEvent` (confirmed) — and
  the requests in flight then fail; at start-up those would be the clients' connect. A refresh
  that fails here is logged, and the session goes on with the current token until the next
  attempt.
- *Reactive*: refresh runs once when `ProtoOAAccountAuthReq` is rejected with one of two error
  codes the schema defines for a token problem — `OA_AUTH_TOKEN_EXPIRED` or
  `CH_ACCESS_TOKEN_INVALID` — and never for any other rejection reason, since a new token
  cannot fix those. A reactive refresh is also never issued more often than a fixed minimum
  interval, so a venue that keeps rejecting the same token as expired cannot drive a
  refresh loop. **Unconfirmed**: that an expired or invalid token surfaces as a rejected
  account authentication at all, and which of these two codes a live venue actually returns
  for it.

After every refresh, the new pair is handed to the host application through a callback so it
can be persisted; if that callback raises, the failure is logged at ERROR (the new tokens stay
usable for the rest of the session, but only in memory — losing them here means the next
process restart falls back to the old, now-invalid refresh token).

How tokens behave was checked on a live connection, on the account's own (live) host, on
2026-10-07:

- **A refresh works over a connection authenticated at application level only** (confirmed).
  Every refresh went through `ProtoOARefreshTokenReq` on a connection that had passed
  `ProtoOAApplicationAuthReq` and nothing more — the state the reactive refresh runs in, since
  account authentication has just been rejected.
- **A refresh token is single-use** (confirmed). Sending one that was already used is refused
  with `CH_ACCESS_TOKEN_INVALID`, and that refused replay revoked no other grant. So once a
  refresh request reaches the venue, the old refresh token stops working, and its reply must
  not be lost. This adapter adopts a reply that arrives after its request timed out, unless a
  newer pair has been taken since that request was sent. A refresh made while a connection is
  being brought up — on the short pre-connection that resolves the account's host, or after
  account authentication was rejected — keeps that connection open up to ten seconds more for
  the reply, and gives up at once if the connection is lost. Within the first `connect()`, or a
  later user's wait to join, that time counts against the account's `connect_timeout_secs`; a
  value under about 15 seconds (the 5-second request timeout plus this wait) cuts the wait
  short, and a pair arriving after that is lost. A bring-up the session runs on its own after a
  connection loss has no such bound, and there the wait delays the reconnect backoff by up to
  ten seconds.
- **A refreshed access token expires 30 days after it is issued** (confirmed).
- **Each authorization is an independent grant** (confirmed). Authorizing the same application
  twice for one cTrader ID gave two token pairs; neither the second authorization nor a
  refresh of the second pair affected the first pair's access or refresh token.
- **One access token can authenticate the same account on two connections at once**
  (confirmed). Neither connection received a disconnect or token-invalidation event within
  15 seconds, and a trader read succeeded on each.

**Unconfirmed**: a refresh for a live account's token over the short pre-connection to the
demo host that resolves the account's host (Section 1). This adapter refreshes there at
start-up when the account list rejects the access token it was given; every refresh confirmed
so far went through the account's own host.

**Authentication lost on a live socket.** The venue can drop authentication without closing
the connection, through three events:

| Event | Meaning |
|---|---|
| `ProtoOAAccountDisconnectEvent` | The account session was dropped. |
| `ProtoOAAccountsTokenInvalidatedEvent` | The access token for one or more accounts was invalidated. |
| `ProtoOAClientDisconnectEvent` | The venue cancelled the application-level connection. |

This adapter treats all three the same way: as a loss of session, handled by re-authenticating
through the normal reconnect path and replaying every registered subscription, since
authentication and subscriptions belong together as one recoverable unit.

A token invalidation does **not**, by itself, trigger a refresh. The schema lists "token was
refreshed" as one of the possible causes of `ProtoOAAccountsTokenInvalidatedEvent`, so
refreshing in direct response to it could feed itself: a refresh causes an invalidation event,
which triggers another refresh. Re-authenticating with the current token (or acquiring a new
one only through the reactive path in the previous section, if the venue then rejects that
re-authentication as an expired token) breaks that cycle.

## 6. Rate limiting

A rate-limit breach is reported as `ProtoOAErrorRes` with `errorCode = BLOCKED_PAYLOAD_TYPE`
(confirmed) and a `retryAfter` value in seconds, scoped to the specific payload type that was
throttled.

This adapter treats `retryAfter` as authoritative: on a breach, the affected bucket is paused
for exactly the duration the venue reports, rather than guessing a backoff. Requests are
throttled locally before they are ever sent, using separate budgets for ordinary requests and
for historical requests, so that a burst of historical requests cannot starve everything
else. **Unconfirmed**: the real budgets. Published figures for this API are inconsistent, so
the defaults this adapter ships with are deliberately conservative until a live connection's
own `BLOCKED_PAYLOAD_TYPE` responses settle the real numbers.

## 7. Scaling — read this before trusting any number

Every scale in this protocol is implicit. Getting one wrong by a factor produces a perfectly
valid request for the wrong size, which is why each of the values below is fixed in a recorded
response and asserted in the test suite rather than reasoned about.

- **Volumes are in cents of a unit** (confirmed): `units = volume / 100`. They are not lots.
  What a "unit" is depends on the symbol — a unit of the base currency for an FX pair, a
  contract for an index or a metal — and the symbol's own `measurementUnits` field names it.
  A lot is `lotSize` in those same units.
- **Spot prices and trendbar prices are integers in 1/100000 of a price unit** (confirmed),
  regardless of the symbol's `digits`. A symbol quoted to 2 decimals still sends
  `2528164000` for a price of `25281.64`. `digits` says how many decimals are meaningful, not
  how the integer is scaled.
- **Trendbar prices are deltas from `low`** (confirmed): a trendbar carries `low` as an
  absolute price and `deltaOpen`, `deltaHigh`, `deltaClose` as non-negative offsets from it, so
  `open = low + deltaOpen` and so on.
- **Monetary values carry a `moneyDigits` scale.** The account reports its own digit count, and
  the same integer means different amounts of money at different counts.

Trendbar timestamps are minutes since epoch, of the bar's **opening tick** rather than of the
period boundary; see [§8](#8-market-data-messages).

## 8. Market data messages

### Spot events

`ProtoOASpotEvent` is the only live price message; there is no separate trade or bar-close
event.

- Spot events are **one-sided** (confirmed): an event carries `bid`, or `ask`, or both, and
  bid-only and ask-only events are both entirely normal. A consumer that needs a two-sided
  quote has to remember the last value of each side.
- A spot event's `timestamp` is in **milliseconds** (confirmed).
- Spot events carry no size at all. There is no depth in this protocol.

### Live trendbars travel inside spot events

There is no bar message and no bar-close event. `ProtoOASpotEvent.trendbar` is a repeated
field carrying the currently forming bar of **each** period the connection is subscribed to,
and a live trendbar identifies its own period through `ProtoOATrendbar.period` (confirmed).

A **historical** trendbar does not (confirmed): in a `ProtoOAGetTrendbarsRes`, the individual
bars leave `period` unset and the period is carried once, on the response. This asymmetry is a
trap in the protobuf encoding rather than in the documentation — an unset enum field reads back
as the first enum value, which here is the one-minute period. Reading a historical bar's own
`period` therefore produces a plausible wrong answer rather than an error, on every period
except the one-minute one.

**Subscribing to live trendbars requires an active spot subscription** for the same symbol
(confirmed). On an open market, a live-trendbar subscribe sent with no spot subscription for
that symbol was refused, while the same subscribe sent after a spot subscription succeeded and
delivered live bars. The refusal came back as `INVALID_REQUEST`, **not** as the
`NOT_SUBSCRIBED_TO_SPOTS` the schema carries for exactly this case, so do not match on that
code to recognise the condition. This adapter holds a spot subscription for as long as it holds
a live trendbar, so a consumer that asks only for bars still gets one.

### Historical trendbars

`ProtoOAGetTrendbarsReq` takes a symbol, a period, a `fromTimestamp`/`toTimestamp` window in
milliseconds and a `count`, and pages backwards: `count` is counted back from `toTimestamp`.

- **History reaches back before the account's own registration timestamp** (confirmed). A
  freshly created account can still warm a strategy up.
- **A page can come back short without any error.** A request for `count = 500` returned 499
  bars with `hasMore = true` (confirmed). A caller must therefore never treat "fewer bars than
  asked for" as "the history is exhausted"; this adapter continues the next window from the
  oldest bar a page actually served, rather than from where the window was asked to start, so a
  short page skips nothing.
- **A window far wider than the page is truncated in silence** (confirmed). A request spanning
  400 days, with `count = 500`, was accepted and answered with 500 one-minute bars — a few
  hours of the 400 days — with no `INCORRECT_BOUNDARIES` and no other error. Nothing in the
  response says the window's start was never reached, so "the page stopped short of
  `fromTimestamp`" carries no information about whether older bars exist. The same rule as
  above covers it: continue from the oldest boundary the page actually served.
- **`count` is honoured well past a page** (confirmed): `count = 5000` was answered with 4999
  bars, and `hasMore` was still true. The cap near 500 that this adapter's default page size
  was chosen around does not exist. Across runs a request for `count = N` has come back with
  both `N` and `N - 1` bars — never more than `N` — so the count is an upper bound, not a
  promise.
- **History serves a bar almost as soon as it closes** (confirmed): polling for a just-closed
  one-minute bar returned it between 0.06 s and 1.0 s after its boundary, over five
  consecutive bars. In the same run every one of those bars was closed by the live stream
  rather than by a timeout, so the history fallback is the exception, not the normal path.
- **A trendbar day is not a calendar day, and its offset is not even constant** (confirmed).
  A daily bar covers the venue's trading day, which rolls at 17:00 in New York, so its open
  time is an offset into the calendar day — and that offset follows US daylight saving. Two
  years of daily bars show it alternating, changing on exactly the US transition dates:

  | Daily bars from | to | open at |
  |---|---|---|
  | 2024-11-03 | 2025-03-06 | 22:00 UTC |
  | 2025-03-09 | 2025-10-30 | 21:00 UTC |
  | 2025-11-02 | 2026-03-05 | 22:00 UTC |
  | 2026-03-08 | 2026-09-22 | 21:00 UTC |

  A consumer must therefore not pin the offset once. Note also that the US and EU transition
  dates differ by a week or two each spring and autumn, so the offset from any European wall
  clock moves at different moments than the offset from UTC does.
- **Which periods keep epoch alignment follows from that offset** (confirmed). The trading day
  starts on the hour, so every period dividing an hour divides the offset too and stays aligned
  whatever the offset currently is: M1, M15 and H1 were observed opening on multiples of their
  own length. H4, H12 and D1 do not divide 21 or 22 hours, so they carry the offset instead —
  every observed H4 and H12 bar, across two symbols, opened one hour into its own grid while
  the offset was 21 hours, and it moves with the offset. A consumer that floors a timestamp by
  the period to find a boundary is therefore correct up to H1 and wrong from H4 up; take the
  open time the venue sends instead, and derive a boundary from an observed bar rather than
  from the epoch.
- **`fromTimestamp` did not bound the answer** (confirmed for a one-minute M1 window with
  `count = 10`). The venue served up to `count` bars counted back from `toTimestamp`, selecting
  by open time and including a bar that opens exactly on `toTimestamp`. Asked with
  `count = 10` for the one-minute M1 window from 11:33:00.000 to 11:34:00.000, the venue
  served ten bars opening 11:25 to 11:34. The same window narrowed by 1 ms at each end was
  served bars opening 11:24 to 11:33, and widened by 1 ms at each end, 11:25 to 11:34 again.
  So a page reaches back past its window's start whenever `count` allows: this adapter drops
  every bar opening before the start a request asked for, and every bar opening at or after
  its end, and de-duplicates the boundary each page repeats at its `toTimestamp`.

**Unconfirmed**: whether `fromTimestamp` bounds the answer in any case the probe did not
cover; its window had plenty of history behind it. This adapter still sends the window's
start, which the schema makes optional, and relies on it for nothing: a venue that honoured it
would only serve less per page, and the paging continues from the oldest bar a page served.

**Unconfirmed**: the venue's maximum `count` per request, and whether a maximum span exists at
all. Neither limit has been reached: 5000 was served in full, and the 400-day window was
answered without complaint by a page that simply stopped at its `count`. `INCORRECT_BOUNDARIES`
exists in the schema but has never been seen, so a span cap may sit above the spans asked for,
or may be enforced only by truncation. Both show up as a page that did not reach its window's
start, which the paging already handles.

## 9. A known inconsistency: `maintenanceEndTimestamp`

The API has two error message types, and they document the same field name in two different
units: `maintenanceEndTimestamp` is in milliseconds on `ProtoErrorRes`, and in seconds on
`ProtoOAErrorRes`. Nothing on the wire says which unit a given value is in — it is implied
only by which of the two message types carries it.

This adapter reads each message type according to its own documented unit and normalises both
to seconds before the value reaches application code, so callers never have to know which
error type they got. **Unconfirmed**: this is inferred from the two messages' documentation,
not yet observed together on a live connection; the first real maintenance window will settle
it.

## 10. Execution events

Everything in this section up to [Commands on orders placed by hand](#commands-on-orders-placed-by-hand)
was observed in one recorded session on a hedging account, in which a person traded one symbol
priced near 85,000 with 2-decimal prices by hand in the broker's terminal while a read-only
connection listened; the history lists also hold one earlier trade on another symbol. The
scrubbed recording is the fixture `tests/fixtures/m3_execution_recorded.json`. What it did not
exercise is marked **unconfirmed**.

### Delivery

- **Execution events are pushed after account authentication alone** (confirmed). No
  subscription request exists for them, and none is needed: every order, position and deal
  change made in the session arrived as a `ProtoOAExecutionEvent`, including changes made by
  another client such as the broker's own terminal.
- `isServerEvent` is `false` for an action a client asked for (placing, amending, cancelling)
  and `true` for what the venue did on its own (creating the protective order, a level
  triggering, a protective order following a partial close) (confirmed).
- **Prices in execution messages are doubles in price units** (`executionPrice`, `stopLoss`,
  `stopPrice`, `limitPrice`, position `price`), unlike the integer spot and trendbar prices of
  §7. The relative distances on a new order are integers in 1/100000 of a price unit, like spot
  prices: a distance of `90.01` travels as `9001000` (confirmed, on a 2-decimal symbol).
- **Volumes are in cents of a unit here too** (confirmed by the arithmetic): closing volume `1`
  of a position opened at `85287.21` and closed at `85219.06` realised a gross profit of
  `-0.68`, which is `(85219.06 - 85287.21) * 0.01`.

### A market order with a stop-loss and a take-profit

The terminal sends a `MARKET` order with `timeInForce = IMMEDIATE_OR_CANCEL` and the levels as
`relativeStopLoss` / `relativeTakeProfit`. The events then arrive in this order (confirmed):

1. `ORDER_ACCEPTED` — the order, with its relative levels, and a new position in
   `POSITION_STATUS_CREATED` with zero volume;
2. `ORDER_FILLED` — the order filled, the deal, and the position now open. **The fill event does
   not yet carry the position's levels**;
3. `ORDER_ACCEPTED`, a server event — a **separate order of type `STOP_LOSS_TAKE_PROFIT`** with
   its own `orderId`, `closingOrder = true`, `GOOD_TILL_CANCEL`, the stop-loss in `stopPrice` and
   the take-profit in `limitPrice`; the position now carries `stopLoss` and `takeProfit` too.

**The relative levels are applied to the fill price** (`executionPrice`), not to the
position's `marginRate` (confirmed): a fill at `85287.21` with distances `90.01` and `100.01`
gave a stop-loss of `85197.20` and a take-profit of `85387.22`, and the second position
matched the same way. **Unconfirmed**: how far the quote at the moment of sending was from the
fill; the recording holds no quote.

So the two protective levels of a position are **one** venue order, not two, and that order has
a real `orderId` from the moment the position opens.

### Changing the levels

Moving a level, removing one and adding one back are all `ORDER_REPLACED` events on that same
protective order, with the position's own `stopLoss` / `takeProfit` updated alongside
(confirmed). A removed level is simply absent: after the take-profit was removed, the order
carried `stopPrice` and no `limitPrice`, and the position no `takeProfit`. Adding it back made
both fields reappear on the same `orderId`.

**Unconfirmed**: what happens when both levels are removed — whether the protective order is
cancelled or stays with neither field.

### A level triggering

A triggered level fills the protective order: an `ORDER_FILLED` server event carrying that
order, a closing deal, and the position in `POSITION_STATUS_CLOSED` (confirmed, for a stop-loss
and for a take-profit). The order still holds both levels as they stood; the deal's
`executionPrice` is the fill, which can differ from the level: a stop-loss at `85206.20`
filled at `85205.58`.

No field says which of the two levels triggered — neither in the published schema nor among
the fields the venue sends beyond it (see below). The protective order's own semantics tell them
apart: for a long position (the protective order sells) it is the take-profit if the fill is at
or above `limitPrice`, otherwise the stop-loss; for a short (it buys), the take-profit if the fill
is at or below `limitPrice`, otherwise the stop-loss; with one level present, that level. Both
recorded triggers agree, including a take-profit moved through the market, which the venue
accepted and filled at once: the two levels need not sit on opposite sides of the market.
**Unconfirmed**: that a take-profit never fills worse than its price.

### Closing part of a position

A partial close from the terminal is a `MARKET` order with `closingOrder = true`,
`IMMEDIATE_OR_CANCEL` and the `positionId` it closes: `ORDER_ACCEPTED`, then `ORDER_FILLED` with
a deal whose `closePositionDetail` carries the realised result (confirmed). A server
`ORDER_REPLACED` on the protective order follows, its volume reduced to the position's new
volume. The position's `price` stays the original entry price.

`closingOrder` is `true` on a partial close and on the protective order (confirmed).
**Unconfirmed**: a full manual close of a position from the terminal, and a stop-out; neither
happened in the session.

A later recording on EURUSD watched a person close part of a position with a stop-loss and a
take-profit by hand, while a read-only connection listened. The scrubbed recording is the fixture
`tests/fixtures/partial_close_live.json`. It confirmed, in more detail:

- **Exactly three execution events**, in this order: the closing order's `ORDER_ACCEPTED`, carrying
  the position at its volume before the close; its `ORDER_FILLED`, carrying the deal and the
  position already at the volume left; then the protective order's `ORDER_REPLACED`. The first two
  have `isServerEvent = false`, the last `true`.
- **The protective order keeps its `orderId`.** In the `ORDER_REPLACED`, `tradeData.volume` is the
  volume left on the position, the reduced total, not the volume closed, and `executedVolume` is
  0. The event carries the position at the volume left, and its `utcLastUpdateTimestamp` is 10 ms
  after the deal's execution.
- **All three carry a `clientMsgId`**, the one of the terminal's request, which matches no request
  of the listening connection (see §2).

**Unconfirmed**: a protective order that a level fills only in part, and whether its volume is then
the total or the rest.

### A pending order

A `LIMIT` order placed from the terminal arrives as `ORDER_ACCEPTED` with `GOOD_TILL_CANCEL`
and **already names a new `positionId`**, in `POSITION_STATUS_CREATED` with zero volume
(confirmed). Cancelling it is `ORDER_CANCELLED`, and that position is reported
`POSITION_STATUS_CLOSED` without ever having opened.

### Balance

- **Opening a position does not change the balance** (confirmed). The position shows its
  commission at once, but the account's `balance` and `balanceVersion` are unchanged until a
  deal closes volume.
- **A closing deal changes the balance by exactly its realised amounts** (confirmed, on every
  closing deal recorded): `balance after = balance before + grossProfit + swap + commission +
  pnlConversionFee`, with the commission covering both the opening and the closing side of the
  closed volume. `closePositionDetail.balance` is the balance after the deal, and
  `balanceVersion` increases by one for each change.
- **Swap is realised when the position closes** (confirmed, on a position held a few hours and
  closed some days before the session): its `closePositionDetail.swap` was part of the change
  above.
  **Unconfirmed**: when swap accrues on an open position, and how that is reported.
- **The cash-flow history reaches the account's first deposit** (confirmed), which has
  `balance == delta`. The deal list answered for a window starting at the account's
  registration; **unconfirmed** that it would have returned deals older than its window allows.
  Together the two lists rebuild the balance at any past moment the deal history covers.
- **Unconfirmed**: the sign of `delta` for a withdrawal; the session had none.

### The lists reconciliation reads

After a restart or a reconnect the adapter rebuilds what happened from five lists. All were asked
at the end of the recorded session, which held three positions, all closed by then (two traded
and closed, one pending order cancelled before it ever opened).

- **The snapshot** (`ProtoOAReconcileReq`) lists the open positions and the pending orders, and
  nothing about closed ones. An open position carries its `stopLoss` and `takeProfit`. Its
  protective order is listed among the orders **only when `returnProtectionOrders` is set**, and
  then under its own `orderId`; without the flag the order list held none (confirmed, on every
  snapshot taken both ways). A pending `LIMIT` order appeared in both.
- **A position's own order list** (`ProtoOAOrderListByPositionIdReq`) answered **without a time
  window** for all three positions (confirmed). For a closed position it holds the entry (a
  `MARKET` order, `IMMEDIATE_OR_CANCEL`), each closing order, and the protective order, which
  after a level triggered is `ORDER_STATUS_FILLED` with `executedVolume` equal to its volume and
  both levels (`stopPrice` and `limitPrice`) as they stood. A pending order cancelled before it
  opened is listed with `ORDER_STATUS_CANCELLED` and nothing executed. Every order carries its
  `positionId`. **Unconfirmed**: the list of a position that is still open.
- **A position's own deal list** (`ProtoOADealListByPositionIdReq`) likewise answered without a
  window. Each deal's `orderId` matches an order of the position's own order list, which is how a
  fill is tied to its order. Opening deals carry a commission too, not only closing ones. The list
  for the cancelled pending order was empty.
- **The account's deal list** and **order list** over a time window answered; each fitted in one
  page (`hasMore` false), so paging is **unconfirmed**, as are the order the lists come in, the
  order of items inside one millisecond, which of an order's times the order list filters by,
  and whether either end of a window is inclusive. The adapter asks in windows of a week and
  de-duplicates by id.
- **The cash-flow list** answered for a window of a week. **Unconfirmed**: whether it has pages
  at all, and whether a week is its maximum.

The orders a trader placed in the recorded session carry no `label`, because the terminal sets
none, so every recorded position is foreign. That the venue returns the adapter's own `label` and
`comment` fields unchanged is **unconfirmed**; so is whether a rejected order is listed.

### Commands on orders placed by hand

A second recorded run, on EURUSD on a hedging account, sent cancels and amends through the
adapter. A person had placed by hand a pending `LIMIT` order with a stop-loss and a take-profit
attached, and opened a position with a trailing stop-loss and a take-profit. The scrubbed
recording is the fixture `tests/fixtures/external_commands_live.json`. All of the following is
confirmed:

- **An amend of a pending order** (`ProtoOAAmendOrderReq`) is answered by an `ORDER_REPLACED`
  execution event carrying the amended order, under the same `orderId`. **A cancel**
  (`ProtoOACancelOrderReq`) is answered by `ORDER_CANCELLED`, with the order's position in
  `POSITION_STATUS_CLOSED`. No other execution event follows an accepted command.
- **A pending order reports its attached levels as distances**, in `relativeStopLoss` and
  `relativeTakeProfit`, with no `stopLoss` or `takeProfit`. An amend that sends the distances
  again keeps them.
- **A level amend** (`ProtoOAAmendPositionSLTPReq`) carrying `stopLossTriggerMethod`,
  `trailingStopLoss = true` and `guaranteedStopLoss = false` explicitly is accepted, and is
  answered by `ORDER_REPLACED` on the same protective order. The stop-loss stays trailing after
  its level is moved and after the take-profit is removed. **Leaving `takeProfit` out of the
  amend removes the take-profit**, so an amend that changes one level has to send the other
  again.
- **Refusals.** A cancel of an order id that does not exist is refused with a
  `ProtoOAOrderErrorEvent`, `ORDER_NOT_FOUND`. An order details request (`ProtoOAOrderDetailsReq`)
  for such an id is refused with a `ProtoOAErrorRes`, `ORDER_NOT_FOUND`.
- **The order details request answers an order that has ended**: `ProtoOAOrderDetailsRes` with a
  cancelled order in `ORDER_STATUS_CANCELLED` and no deals.
- **A trailing stop-loss's moves arrive only as `ProtoOATrailingSLChangedEvent`**, never as
  execution events; one run saw 15 of them in 60 seconds. A move changes neither the protective
  order's nor the position's `utcLastUpdateTimestamp`: a snapshot taken after several moves
  showed the trailed stop-loss under the time of the last amend.

In the recording, the trailing event's `utcLastUpdateTimestamp` was in milliseconds and agreed
within a few tens of milliseconds with the times of the execution events and spots around it.
That the spots share the execution events' clock is still **unconfirmed** (see
[Execution](execution.md), section 10).

**Unconfirmed**: whether an amend's `volume` is the order's whole volume or its unfilled rest once
it has partly filled; whether an amend keeps an expiration it sends again (the order had none);
what the venue does with an optional field an amend leaves out; and that leaving `stopLoss` out
of a level amend removes the stop-loss.

### Messages not seen

No margin-change, trader-update or margin-call event has arrived in any recorded session; their
shapes are **unconfirmed**. The order error event's shape is the one recorded above.

### Fields beyond the published schema

The venue sends two fields that the published schema does not define (confirmed; the bindings
were generated from the latest published schema, so regenerating them does not help):

- field 19 on every `ProtoOADeal`, length-delimited and empty in every deal recorded;
- field 23 on `ProtoOATrader`, a small integer with the same value in every response.

Protobuf keeps unknown fields when it parses a message, so nothing fails. Neither field carries
anything this adapter needs; their meaning is **unconfirmed**.

