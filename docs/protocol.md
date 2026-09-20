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
**Unconfirmed**: that the server sends heartbeats when it has nothing else to send; if it stays
silent on an otherwise idle connection instead, this reconnects an idle session every 90
seconds.

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
  into expiry.
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

**Unconfirmed**: whether subscribing to live trendbars requires an **active spot subscription**
for the same symbol. A live-trendbar subscribe sent with no spot subscription for that symbol
was refused — but with `INVALID_REQUEST`, not with the `NOT_SUBSCRIBED_TO_SPOTS` the schema
carries for exactly this case. The probe ran while that symbol's market was closed, so it is not
established whether `INVALID_REQUEST` answers the missing subscription or the shut market;
repeating it on an open market would settle it. Either way this adapter holds a spot
subscription for as long as it holds a live trendbar, so a consumer that asks only for bars
still gets one.

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
- **`count` is honoured well past a page** (confirmed): `count = 5000` was served in full, and
  `hasMore` was still true. The cap near 500 that this adapter's default page size was chosen
  around does not exist.
- **A trendbar day is not a calendar day** (confirmed): daily bars open at 21:00 UTC, not at
  00:00. The intraday periods behave as expected — observed M1, M15 and H1 open times are all
  multiples of their own length — but a daily bar covers the venue's trading day, so its open
  time is an offset into the calendar day rather than the start of one.

**Unconfirmed**: whether `fromTimestamp` and `toTimestamp` are inclusive. The paging absorbs
an inclusive `toTimestamp` harmlessly, since a repeated boundary is de-duplicated. An
*exclusive* `fromTimestamp` would not be absorbed — the bar at each window's start would be
lost on every page — which is why this is the first thing to check on a live connection.

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
