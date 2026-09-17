# The cTrader Open API wire protocol, as this adapter implements it

This document describes the transport-level behaviour of the cTrader Open API protobuf
protocol and how this adapter implements it. It is written for an outside reader — a
Spotware reviewer, or anyone integrating cTrader with something other than this adapter —
and it names its sources in prose rather than assuming familiarity with this codebase.

Message definitions come from Spotware's MIT-licensed
[openapi-proto-messages](https://github.com/spotware/openapi-proto-messages) schema. Where a
fact below is not yet confirmed against a live connection, it is marked **unconfirmed** and
carries a `TODO(verify):` comment at the corresponding place in the source.

## 1. Transport

The API is reached over TLS at `demo.ctraderapi.com:5035` (demo accounts) or
`live.ctraderapi.com:5035` (live accounts) — the same port for both environments.

Each frame on the wire is a 4-byte big-endian length prefix followed by a serialised
`ProtoMessage` envelope, capped at 15,000,000 bytes. This is exactly the framing Twisted's
`Int32StringReceiver` implements (`structFormat = "!I"`, a 4-byte prefix, `MAX_LENGTH =
15_000_000`), which is what Spotware's own reference SDK is built on. A length prefix
declaring more than the cap is treated as a protocol violation and the connection is dropped
rather than read further.

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
server heartbeats at the 30-second tolerance — as a lost connection and reconnects.
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
and a `retryAfter` value in seconds, scoped to the specific payload type that was throttled.

This adapter treats `retryAfter` as authoritative: on a breach, the affected bucket is paused
for exactly the duration the venue reports, rather than guessing a backoff. Requests are
throttled locally before they are ever sent, using separate budgets for ordinary requests and
for historical requests, so that a burst of historical requests cannot starve everything
else. **Unconfirmed**: the real budgets. Published figures for this API are inconsistent, so
the defaults this adapter ships with are deliberately conservative until a live connection's
own `BLOCKED_PAYLOAD_TYPE` responses settle the real numbers.

## 7. Scaling — read this before trusting any number

Three conversions in this protocol are the highest-consequence unknowns in the whole API,
because getting any of them wrong by a factor produces a working request for the wrong size:

- **Volumes are not lots.** The wire value needs a broker-supplied conversion to arrive at a
  human-meaningful size.
- **Monetary values carry a `moneyDigits` scale.** The same integer means different amounts of
  money depending on the instrument's configured digit count.
- **Trendbar prices are integers encoded as deltas from `low`**, not absolute prices.

None of this conversion exists in the adapter yet. When it is built, each converter must be
checked against a recorded real response before it ships, and scaling in particular will be
verified against a live account with a minimum-size order before it is trusted for anything
larger.

## 8. A known inconsistency: `maintenanceEndTimestamp`

The API has two error message types, and they document the same field name in two different
units: `maintenanceEndTimestamp` is in milliseconds on `ProtoErrorRes`, and in seconds on
`ProtoOAErrorRes`. Nothing on the wire says which unit a given value is in — it is implied
only by which of the two message types carries it.

This adapter reads each message type according to its own documented unit and normalises both
to seconds before the value reaches application code, so callers never have to know which
error type they got. **Unconfirmed**: this is inferred from the two messages' documentation,
not yet observed together on a live connection; the first real maintenance window will settle
it.
