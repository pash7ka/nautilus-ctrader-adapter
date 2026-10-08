"""Venue identity, endpoints and protocol constants.

Values sourced from Spotware's SDK carry a note saying so. Values that are still assumptions
carry `TODO(verify):` and say what would settle them.
"""

from __future__ import annotations

from nautilus_trader.model.identifiers import Venue

from nautilus_ctrader.messages import OpenApiMessages_pb2 as _oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as _om

CTRADER: str = "CTRADER"
CTRADER_VENUE: Venue = Venue(CTRADER)

DEMO_HOST: str = "demo.ctraderapi.com"
LIVE_HOST: str = "live.ctraderapi.com"
PROTOBUF_PORT: int = 5035

# Framing, from Spotware's TcpProtocol(Int32StringReceiver): Twisted's Int32StringReceiver
# uses structFormat "!I" and a 4-byte prefix; MAX_LENGTH is 15_000_000.
LENGTH_PREFIX_BYTES: int = 4
LENGTH_PREFIX_FORMAT: str = "!I"
MAX_FRAME_BYTES: int = 15_000_000

# Their SDK sends a heartbeat after 20 s idle; ProtoHeartbeatEvent's schema comment says the
# server tolerates 30 s. 10 s leaves a full missed beat of headroom.
HEARTBEAT_IDLE_SECS: float = 10.0

# No inbound frame for this long means the connection is lost, even if writes still succeed
# (a half-open TCP connection). Three missed server heartbeats at the 30 s tolerance.
# TODO(verify): that the server really sends heartbeats when it has nothing else to send; if
# it stays silent on an idle connection, this reconnects an idle session every interval.
INBOUND_SILENCE_SECS: float = 90.0

# Client.send in their SDK defaults to a 5 s response timeout.
DEFAULT_REQUEST_TIMEOUT_SECS: float = 5.0

# Bounds the TCP connect and TLS handshake, which would otherwise wait on the OS timeout.
CONNECT_TIMEOUT_SECS: float = 10.0

# The broker's order types that rest until they trigger; the others fill at once.
PENDING_ORDER_TYPES: tuple[int, ...] = (_om.LIMIT, _om.STOP, _om.STOP_LIMIT)

BUCKET_DEFAULT: str = "default"
BUCKET_HISTORICAL: str = "historical"

# Documented figures (50/s general, 5/s historical per connection); M1 used 5/1 as
# placeholders. TODO(verify): a live BLOCKED_PAYLOAD_TYPE with retryAfter under load settles
# the real figures.
DEFAULT_RATE_LIMIT_PER_SEC: float = 50.0
HISTORICAL_RATE_LIMIT_PER_SEC: float = 5.0

# Payload types that `CTraderSession.request()` defaults to the historical bucket for; an
# explicit `bucket` argument still overrides this.
HISTORICAL_PAYLOAD_TYPES: frozenset[type] = frozenset(
    {_oa.ProtoOAGetTrendbarsReq, _oa.ProtoOAGetTickDataReq},
)

# Confirmed live: 100 ids in one request are accepted and all 100 symbols come back.
# TODO(verify): where the venue's own limit actually sits, which is still unknown -
# only that it is at least this batch.
SYMBOL_BY_ID_BATCH: int = 100

# Refresh this far ahead of the access token's expiry. Several times the minimum refresh
# interval, so a failed attempt can be retried before the token lapses.
TOKEN_REFRESH_MARGIN_SECS: float = 900.0

# A refresh can only fix a token problem; any other account-auth rejection is final.
# TODO(verify): that an expired token surfaces as a rejected ProtoOAAccountAuthReq, and with
# which of these codes.
TOKEN_ERROR_CODES: frozenset[str] = frozenset({"OA_AUTH_TOKEN_EXPIRED", "CH_ACCESS_TOKEN_INVALID"})

# No automatic refresh closer to the previous one than this: a venue that keeps rejecting a
# revoked token as expired, or a very short granted lifetime, must not rotate tokens in a loop.
MIN_TOKEN_REFRESH_INTERVAL_SECS: float = 300.0

# How long a refresh that timed out still waits for its reply before its connection is closed.
# A refresh token is single-use, so a reply lost that way leaves no working token pair. Inside
# `connect()` the wait counts against `connect_timeout_secs`, and a value under about 15 s cuts
# it short; a bring-up the session runs after a connection loss has no such bound, so there it
# delays the reconnect backoff.
LATE_REFRESH_WAIT_SECS: float = 10.0

# Reconnect backoff.
BACKOFF_BASE_SECS: float = 1.0
BACKOFF_MAX_SECS: float = 60.0
BACKOFF_JITTER: float = 0.25
RECONNECT_FAILURE_THRESHOLD: int = 5

# A session must stay ready this long before a loss is treated as recovery rather than failure.
STABLE_SESSION_SECS: float = 30.0

# Cache keys the execution client writes for the application, as UTF-8 JSON.
UNLOADED_EXPOSURE_KEY: str = "ctrader.unloaded_exposure"
BALANCE_CHECKPOINT_KEY: str = "ctrader.balance_checkpoint"
