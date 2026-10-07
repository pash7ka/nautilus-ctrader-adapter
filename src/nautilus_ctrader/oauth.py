"""The cTrader Open API OAuth flow: authorization URL, localhost callback, code exchange.

Synchronous, and independent of any Nautilus object. A first-time setup runs all three steps:

    state = secrets.token_urlsafe(32)
    webbrowser.open(build_authorization_url(client_id, redirect_uri, state))
    code = wait_for_authorization_code(redirect_uri, state)
    tokens = exchange_code(client_id, client_secret, code, redirect_uri)

A code obtained some other way, such as pasted by a user, goes straight to `exchange_code()`.
Refreshing a pair is not offered here: a connected account client refreshes and reports the
new pair through its token listeners, so there is one refresher only.

No token, code, client id or secret appears in any message, repr or exception argument.
"""

from __future__ import annotations

import html
import http.client
import http.server
import json
import math
import secrets
import socketserver
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass

from nautilus_ctrader.common.errors import (
    CTraderAuthorizationDenied,
    CTraderAuthorizationTimeout,
    CTraderTokenExchangeError,
)

# The documented authorization page (help.ctrader.com/open-api/account-authentication). The
# official SDK uses openapi.ctrader.com/apps/auth instead; the documented one is preferred.
AUTHORIZATION_URL = "https://id.ctrader.com/my/settings/openapi/grantingaccess/"
TOKEN_URL = "https://openapi.ctrader.com/apps/token"

# The callback server always binds IPv4 127.0.0.1 regardless of the redirect host, so only
# these are accepted as redirect hosts: a wider host could bind and expose the callback port,
# and "::1" would parse as loopback but never actually receive the redirect.
_LOOPBACK_HOSTNAMES = frozenset({"localhost", "127.0.0.1"})
_CALLBACK_BIND_HOST = "127.0.0.1"


@dataclass(frozen=True, repr=False)
class TokenPair:
    """An access/refresh token pair. Its repr masks both tokens."""

    access_token: str
    refresh_token: str
    # Unix seconds.
    expires_at: float

    def __repr__(self) -> str:
        return f"TokenPair(access_token=***, refresh_token=***, expires_at={self.expires_at})"


class RedirectUriError(ValueError):
    """A redirect URI the callback server cannot serve; `reason` says why."""

    def __init__(self, redirect_uri: str, reason: str) -> None:
        super().__init__(f"redirect URI {redirect_uri!r} refused: {reason}")
        self.reason = reason


def build_authorization_url(client_id: str, redirect_uri: str, state: str) -> str:
    """The authorization page URL to open in a browser.

    It carries the client id, so a caller should hand it to the browser rather than log it.
    """
    query = urllib.parse.urlencode(
        {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": "trading",
            "product": "web",
            "state": state,
        },
    )
    return f"{AUTHORIZATION_URL}?{query}"


def parse_redirect_uri(redirect_uri: str) -> tuple[int, str]:
    """The callback port and path of `redirect_uri`.

    Raises `RedirectUriError` unless it is plain http on `localhost` or `127.0.0.1`.
    """
    redirect = urllib.parse.urlsplit(redirect_uri)
    # '@' shifts host parsing to whatever follows it (userinfo@host), and a backslash is
    # treated as a literal netloc character by urlsplit but as a path/host separator by some
    # browsers; either lets a netloc like "evil.com\@localhost" parse as host "localhost" while
    # actually addressing something else. Reject both before trusting `.hostname` at all.
    if "@" in redirect.netloc or "\\" in redirect.netloc:
        raise RedirectUriError(redirect_uri, "host must not contain '@' or '\\'.")
    if redirect.scheme != "http" or redirect.hostname not in _LOOPBACK_HOSTNAMES:
        raise RedirectUriError(
            redirect_uri,
            "scheme must be http and host must be localhost or 127.0.0.1.",
        )
    try:
        port = redirect.port
        valid_port = True
    except ValueError:
        valid_port = False
    if not valid_port:
        raise RedirectUriError(redirect_uri, "invalid port.")
    if port == 0:
        raise RedirectUriError(redirect_uri, "port must not be 0.")
    return port or 80, redirect.path or "/"


def _sanitize_for_terminal(
    value: str,
    *,
    max_len: int = 200,
    redact: tuple[object, ...] = (),
) -> str:
    """Strip non-printable characters and cap the length, so a venue-supplied value can't
    smuggle control sequences or an unbounded blob into terminal output. Every non-empty
    string in `redact` is masked first, as is and URL-encoded, in case the venue echoes a
    request value back."""
    forms = {
        form
        for secret in redact
        if isinstance(secret, str) and secret
        for form in (
            secret,
            urllib.parse.quote_plus(secret),
            urllib.parse.quote(secret, safe=""),
        )
    }
    # Longest first, so a shorter form never leaves part of a longer one behind.
    for form in sorted(forms, key=len, reverse=True):
        value = value.replace(form, "***")
    return "".join(ch for ch in value if ch.isprintable())[:max_len]


class _CallbackServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """Serves each connection on its own daemon thread, so one idle or slow-drip connection can
    never hold up the real redirect behind it."""

    daemon_threads = True
    # HTTPServer defaults this on; on Windows SO_REUSEADDR lets another process share or steal
    # the port instead of just permitting reuse of one still in TIME_WAIT.
    allow_reuse_address = sys.platform != "win32"

    def handle_error(self, request: object, client_address: object) -> None:
        # The default prints a traceback to stderr; a failed request only ever ends that one
        # connection, and the wait goes on.
        pass


def wait_for_authorization_code(
    redirect_uri: str,
    state: str,
    timeout_secs: float = 300.0,
    *,
    on_listening: Callable[[], None] | None = None,
) -> str:
    """Serve the callback of `redirect_uri` on 127.0.0.1 until the redirect brings a code.

    - A request to any other path gets 404 and does not end the wait: browsers probe things
      like `/favicon.ico` on their own.
    - So does a request whose `state` is missing or differs from `state` (compared in constant
      time): it could be a stray page rather than the real redirect, and must not be able to
      inject a code or abort the wait.
    - `timeout_secs` is an exact deadline from this call. Every connection is served on its own
      thread, so an idle browser preconnect or a slow client never delays the real redirect.
    - `on_listening`, if given, runs once the socket listens: the cue to open the browser.

    Prints nothing. Raises:

    - `RedirectUriError` for a redirect URI that is not plain http on a loopback host;
    - `OSError` if the callback port cannot be bound, most likely because it is in use;
    - `CTraderAuthorizationDenied` if the redirect carried an error instead of a code;
    - `CTraderAuthorizationTimeout` if no redirect arrived in time.

    TODO(verify): that the authorization endpoint echoes `state` back on the redirect; if it
    doesn't, every real callback will be rejected as a state mismatch.
    """
    port, path = parse_redirect_uri(redirect_uri)
    outcome: dict[str, str] = {}
    outcome_lock = threading.Lock()
    outcome_ready = threading.Event()

    class _Handler(http.server.BaseHTTPRequestHandler):
        # A connection that opens and sends nothing, or trickles bytes in slowly - browsers do
        # the former for speculative preconnects - would otherwise block its handler thread
        # forever, since the base class leaves this as None. Each connection has its own
        # thread, so this only ends that one thread; it never affects the deadline below.
        timeout = 5

        def do_GET(self) -> None:
            parsed = urllib.parse.urlsplit(self.path)
            if parsed.path != path:
                self.send_response(404)
                self.end_headers()
                return
            params = urllib.parse.parse_qs(parsed.query)
            request_state = params.get("state", [""])[0]
            # `compare_digest` requires both arguments to be either `str` restricted to ASCII
            # or `bytes`; encoding both sides first accepts any `request_state` (a stray
            # non-ASCII value must compare as a mismatch, not raise) while keeping the
            # comparison constant-time.
            if not secrets.compare_digest(request_state.encode(), state.encode()):
                self.send_response(400)
                self.end_headers()
                return
            if "code" in params:
                # Record before answering the browser: if that write fails (peer gone, RST, a
                # full send window against the socket timeout), the code must already be
                # captured so the caller gets it instead of timing out.
                self._record(code=params["code"][0])
                self._respond("Authorization received. You can close this tab.")
            elif "error" in params:
                error = _sanitize_for_terminal(params["error"][0])
                self._record(error=error)
                self._respond(f"Authorization failed: {html.escape(error)}")
            else:
                self.send_response(400)
                self.end_headers()

        def _record(self, **result: str) -> None:
            # The first valid result wins; a later or concurrent connection must not overwrite
            # it, so the caller's return value can't be raced.
            with outcome_lock:
                if not outcome:
                    outcome.update(result)
                    outcome_ready.set()

        def _respond(self, message: str) -> None:
            body = f"<html><body>{message}</body></html>".encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            pass  # the default access log would echo the redirect's query string

    server = _CallbackServer((_CALLBACK_BIND_HOST, port), _Handler)
    # A short poll_interval keeps shutdown() (and so the deadline below) precise; the default
    # 0.5s would otherwise let a pending accept-loop iteration add up to half a second of its
    # own on top of timeout_secs.
    server_thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.1},
        daemon=True,
    )
    try:
        server_thread.start()
        if on_listening is not None:
            on_listening()
        deadline = time.monotonic() + timeout_secs
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not outcome_ready.wait(remaining):
            raise CTraderAuthorizationTimeout(timeout_secs)
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)

    if "error" in outcome:
        raise CTraderAuthorizationDenied(outcome["error"])
    return outcome["code"]


def is_clean_token(value: object) -> bool:
    """True for a non-empty `str` with no CR or LF - what an access/refresh token must be to
    be written into an env file line and never split it or smuggle a second assignment."""
    return isinstance(value, str) and bool(value) and "\r" not in value and "\n" not in value


def exchange_code(
    client_id: str,
    client_secret: str,
    code: str,
    redirect_uri: str,
    *,
    timeout_secs: float = 30.0,
    token_url: str = TOKEN_URL,
) -> TokenPair:
    """Exchange an authorization code for an access/refresh token pair.

    `redirect_uri` must be the one the code was issued for. `token_url` exists for tests.
    Raises `CTraderTokenExchangeError` if the endpoint refuses the code (its `errorCode` and
    `description` are fields), cannot be reached, or answers with anything but a usable pair.

    GET with query parameters, as documented (help.ctrader.com/open-api/account-authentication)
    and as Spotware's own SDK does it; the documented response fields are `accessToken`,
    `tokenType`, `expiresIn`, `refreshToken`, `errorCode` and `description`.
    TODO(verify): not yet exercised against the live endpoint.
    """
    request_values = (code, client_secret, client_id)
    query = urllib.parse.urlencode(
        {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )
    # Every failure below is raised outside its `except`, so the error keeps no cause and no
    # context: an `HTTPError` holds the request URL with the secret, and a read or decode error
    # can hold the body with the tokens.
    failure: CTraderTokenExchangeError | None = None
    try:
        with urllib.request.urlopen(f"{token_url}?{query}", timeout=timeout_secs) as response:
            body = response.read()
    except urllib.error.HTTPError as e:
        failure = CTraderTokenExchangeError(
            f"token endpoint returned HTTP {e.code}",
            http_status=e.code,
        )
    except urllib.error.URLError as e:
        reason = _sanitize_for_terminal(str(e.reason), redact=request_values)
        failure = CTraderTokenExchangeError(f"token endpoint request failed: {reason}")
    except (OSError, http.client.HTTPException) as e:
        # Not wrapped by urllib: a read timeout, a dropped connection, a truncated body.
        failure = CTraderTokenExchangeError(f"token endpoint request failed: {type(e).__name__}")
    if failure is not None:
        raise failure

    try:
        data = json.loads(body)
        parsed = True
    except (ValueError, RecursionError):
        parsed = False
    if not parsed:
        raise CTraderTokenExchangeError("token endpoint returned an unparsable response")
    if not isinstance(data, dict):
        raise CTraderTokenExchangeError("token endpoint response is not a JSON object")

    error_code = data.get("errorCode")
    if error_code:
        redact = (*request_values, data.get("accessToken"), data.get("refreshToken"))
        safe_code = _sanitize_for_terminal(str(error_code), redact=redact)
        description = data.get("description")
        raise CTraderTokenExchangeError(
            f"token endpoint rejected the code: {safe_code}",
            error_code=safe_code,
            description=(
                None
                if description is None
                else _sanitize_for_terminal(str(description), redact=redact)
            ),
        )

    access_token = data.get("accessToken")
    refresh_token = data.get("refreshToken")
    if not is_clean_token(access_token):
        raise CTraderTokenExchangeError("token endpoint response has an invalid accessToken")
    if not is_clean_token(refresh_token):
        raise CTraderTokenExchangeError("token endpoint response has an invalid refreshToken")

    expires_in = data.get("expiresIn")
    if (
        not isinstance(expires_in, int | float)
        or isinstance(expires_in, bool)
        or not math.isfinite(expires_in)
        or expires_in < 1
    ):
        raise CTraderTokenExchangeError("token endpoint response has an invalid expiresIn")

    return TokenPair(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_at=time.time() + int(expires_in),
    )
