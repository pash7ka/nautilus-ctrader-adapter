"""Venue identity, endpoints and protocol constants.

Values sourced from Spotware's SDK carry a note saying so. Values that are still assumptions
carry `TODO(verify):` and say what would settle them.
"""

from __future__ import annotations

from nautilus_trader.model.identifiers import Venue

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

BUCKET_DEFAULT: str = "default"
BUCKET_HISTORICAL: str = "historical"

# TODO(verify): their SDK defaults to 5 messages/second overall, while prose documentation
# quotes ~50 requests/second general with a much lower historical budget. A live connection
# returning BLOCKED_PAYLOAD_TYPE with retryAfter under load settles the real figures. Until
# then these stay at the SDK's conservative default.
DEFAULT_RATE_LIMIT_PER_SEC: float = 5.0
HISTORICAL_RATE_LIMIT_PER_SEC: float = 1.0

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

# Reconnect backoff.
BACKOFF_BASE_SECS: float = 1.0
BACKOFF_MAX_SECS: float = 60.0
BACKOFF_JITTER: float = 0.25
RECONNECT_FAILURE_THRESHOLD: int = 5

# A session must stay ready this long before a loss is treated as recovery rather than failure.
STABLE_SESSION_SECS: float = 30.0
